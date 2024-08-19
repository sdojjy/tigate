// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"context"
	"math"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	appcontext "github.com/flowbehappy/tigate/pkg/common/context"
	"github.com/flowbehappy/tigate/pkg/common/server"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/metrics"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/flowbehappy/tigate/server/watcher"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// coordinator implements the Coordinator interface
type coordinator struct {
	nodeInfo    *common.NodeInfo
	initialized bool
	version     int64

	msgCh chan *messaging.TargetMessage

	// for log print
	lastCheckTime time.Time

	// scheduling fields
	supervisor *scheduler.Supervisor

	allChangefeeds         map[string]*ChangefeedMeta
	scheduledStateMachines map[string]*scheduler.StateMachine
	absentStateMachines    map[string]*scheduler.StateMachine
	removingStateMachines  map[string]*scheduler.StateMachine
	runningTasks           map[string]*scheduler.ScheduleTask
	maxTaskConcurrency     int

	lastSaveTime time.Time
	lastTickTime time.Time

	gcManager  gc.Manager
	pdClient   pd.Client
	pdClock    pdutil.Clock
	etcdClient etcd.CDCEtcdClient

	nodeManager     *watcher.NodeManager
	nodeInitializer *NodeInitializer
	nodeChanged     chan struct{}
}

func NewCoordinator(capture *common.NodeInfo,
	pdClient pd.Client,
	pdClock pdutil.Clock,
	etcdClient etcd.CDCEtcdClient, version int64) server.Coordinator {
	c := &coordinator{
		version:                version,
		nodeInfo:               capture,
		lastTickTime:           time.Now(),
		gcManager:              gc.NewManager(etcdClient.GetGCServiceID(), pdClient, pdClock),
		pdClient:               pdClient,
		etcdClient:             etcdClient,
		pdClock:                pdClock,
		msgCh:                  make(chan *messaging.TargetMessage, 1024),
		nodeChanged:            make(chan struct{}, 1),
		scheduledStateMachines: make(map[string]*scheduler.StateMachine),
		absentStateMachines:    make(map[string]*scheduler.StateMachine),
		maxTaskConcurrency:     10000,
	}
	id := scheduler.ChangefeedID(model.DefaultChangeFeedID("coordinator"))
	c.supervisor = scheduler.NewSupervisor(
		id,
		c.newChangefeed, c.newBootstrapMessage,
		scheduler.NewBasicScheduler(id),
		scheduler.NewBalanceScheduler(id, time.Minute, 1000),
	)

	// receive messages
	appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).
		RegisterHandler(messaging.CoordinatorTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
			c.msgCh <- msg
			return nil
		})
	c.nodeManager = appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName)
	c.nodeManager.RegisterNodeChangeHandler("coordinator",
		func(newNodes []*common.NodeInfo, removedNodes []*common.NodeInfo) {
			c.nodeChanged <- struct{}{}
		})
	return c
}

func (c *coordinator) Initialize(ctx context.Context) error {
	metas, err := loadAllChangefeeds(ctx, c.etcdClient)
	c.allChangefeeds = metas
	for id, meta := range metas {
		if shouldRunChangefeed(meta.info.State) {
			cf := newChangefeed(c, model.DefaultChangeFeedID(id), meta.info, meta.status.CheckpointTs)
			stm, err := scheduler.NewStateMachine(scheduler.ChangefeedID(model.DefaultChangeFeedID(id)), nil, cf)
			if err != nil {
				return err
			}
			c.absentStateMachines[id] = stm
		}
	}

	c.nodeInitializer = &NodeInitializer{
		initialized: false,
		nodes:       make(map[common.NodeID]*NodeStatus),
		coordinator: c,
	}
	for _, node := range c.nodeManager.GetAliveNodes() {
		c.nodeInitializer.nodes[node.ID] = NewNodeStatus(node)
	}
	msgs, err := c.nodeInitializer.HandleAliveNodeUpdate()
	if err != nil {
		return errors.Trace(err)
	}
	c.sendMessages(msgs)
	return err
}

// Run is the entrance of the coordinator, it will be called by the etcd watcher every 50ms.
//  1. Handle message reported by other modules.
//  2. Check if the node is changed:
//     - if a new node is added, send bootstrap message to that node ,
//     - if a node is removed, clean related state machine that binded to that node.
//  3. Schedule changefeeds if all node is bootstrapped.
func (c *coordinator) Run(
	ctx context.Context,
) error {
	gcTick := time.NewTicker(time.Minute)
	scheduleTick := time.NewTicker(time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTick.C:
			if err := c.updateGCSafepoint(ctx); err != nil {
				log.Warn("update gc safepoint failed",
					zap.Error(err))
			}
		case <-scheduleTick.C:
			if err := c.scheduleMaintainer(); err != nil {
				return errors.Trace(err)
			}
		case msg := <-c.msgCh:
			// handle grpc messages
			if err := c.handleMessage(msg); err != nil {
				log.Warn("handle message failed", zap.Any("msg", msg), zap.Error(err))
			}
		case <-c.nodeChanged:
			c.nodeInitializer.HandleAliveNodeUpdate()
		}
		now := time.Now()
		metrics.CoordinatorCounter.Add(float64(now.Sub(c.lastTickTime)) / float64(time.Second))
		c.lastTickTime = now
	}

	// 2. check if nodes is changed
	msgs, err := c.supervisor.HandleAliveCaptureUpdate(common.CaptureInfosToNodeInfos(state.Captures))
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.sendMessages(msgs)

	// 3. schedule changefeed maintainer
	msgs, err = c.scheduleMaintainer(state)
	if err != nil {
		return state, err
	}
	c.sendMessages(msgs)

	//4. update checkpoint ts and changefeed states
	c.saveChangefeedStatus()

	c.printStatus()
}

func (c *coordinator) handleMessage(msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeCoordinatorBootstrapResponse:
		req := msg.Message.(*heartbeatpb.CoordinatorBootstrapResponse)
		var statues = make([]scheduler.InferiorStatus, 0, len(req.Statuses))
		for _, status := range req.Statuses {
			statues = append(statues, &MaintainerStatus{status})
		}
		c.supervisor.UpdateCaptureStatus(msg.From.String(), statues)
	case messaging.TypeMaintainerHeartbeatRequest:
		if c.supervisor.CheckAllCaptureInitialized() {
			req := msg.Message.(*heartbeatpb.MaintainerHeartbeat)
			var statues = make([]scheduler.InferiorStatus, 0, len(req.Statuses))
			for _, status := range req.Statuses {
				statues = append(statues, &MaintainerStatus{status})
			}
			msgs, err := c.supervisor.HandleStatus(msg.From.String(), statues)
			if err != nil {
				log.Error("handle status failed", zap.Error(err))
				return errors.Trace(err)
			}
			c.sendMessages(msgs)
		}
	default:
		log.Panic("unexpected message", zap.Any("message", msg))
	}
	return nil
}

func shouldRunChangefeed(state model.FeedState) bool {
	switch state {
	case model.StateStopped, model.StateFailed, model.StateFinished:
		return false
	}
	return true
}

func (c *coordinator) AsyncStop() {
}

func (c *coordinator) sendMessages(msgs []*messaging.TargetMessage) {
	for _, msg := range msgs {
		err := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter).SendCommand(msg)
		if err != nil {
			log.Error("failed to send coordinator request", zap.Any("msg", msg), zap.Error(err))
			continue
		}
	}
}

func (c *coordinator) scheduleMaintainer() error {
	if !c.nodeInitializer.CheckAllNodeInitialized() {
		log.Info("skip scheduling since not all captures are initialized")
		return nil
	}
	batchSize := c.maxTaskConcurrency - len(c.runningTasks)
	if batchSize <= 0 {
		log.Warn("Skip scheduling since there are too many running task",
			zap.String("id", s.ID.String()),
			zap.Int("totalInferiors", allInferiors.Len()),
			zap.Int("totalStateMachines", s.StateMachines.Len()),
			zap.Int("maxTaskConcurrency", s.maxTaskConcurrency),
			zap.Int("runningTasks", s.RunningTasks.Len()),
		)
		return msgs, nil
	}
	// check all changefeeds.
	for id, cfState := range state.Changefeeds {
		if cfState.Info == nil {
			continue
		}
		if !preflightCheck(cfState) {
			log.Error("precheck failed ignored",
				zap.String("id", id.String()))
			continue
		}
		if shouldRunChangefeed(cfState.Info.State) {
			// todo use real changefeed instance here
			ok := c.scheduledChangefeeds.Has(scheduler.ChangefeedID(id))
			if !ok {
				c.scheduledChangefeeds.ReplaceOrInsert(scheduler.ChangefeedID(id), &changefeed{})
			}
		} else {
			// changefeed is stopped
			c.scheduledChangefeeds.Delete(scheduler.ChangefeedID(id))
		}
	}
	c.supervisor.MarkNeedAddInferior()
	c.supervisor.MarkNeedRemoveInferior()
	return c.supervisor.Schedule(c.scheduledChangefeeds)
}

func (c *coordinator) newBootstrapMessage(captureID model.CaptureID) *messaging.TargetMessage {
	log.Info("send coordinator bootstrap request", zap.String("to", captureID))
	return messaging.NewTargetMessage(
		messaging.ServerId(captureID),
		messaging.MaintainerManagerTopic,
		&heartbeatpb.CoordinatorBootstrapRequest{Version: c.version})
}

func (c *coordinator) newChangefeed(id scheduler.InferiorID) scheduler.Inferior {
	cfID := model.ChangeFeedID(id.(scheduler.ChangefeedID))
	cfInfo := c.allChangefeeds[cfID.ID]
	cf := newChangefeed(c, cfID, cfInfo.info, cfInfo.status.CheckpointTs)
	c.scheduledChangefeeds.ReplaceOrInsert(scheduler.ChangefeedID(cfInfo.ID), cf)
	return cf
}

func (c *coordinator) saveChangefeedStatus() {
	if time.Since(c.lastSaveTime) > time.Millisecond*500 {
		c.scheduledChangefeeds.Ascend(func(key scheduler.InferiorID, value scheduler.Inferior) bool {
			id := model.ChangeFeedID(key.(scheduler.ChangefeedID))
			cfState, ok := c.lastState.Changefeeds[id]
			if !ok {
				return true
			}
			cf := value.(*changefeed)
			if cf.State == nil {
				return true
			}
			if !shouldRunChangefeed(model.FeedState(cf.State.FeedState)) {
				cfState.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
					info.State = model.FeedState(cf.State.FeedState)
					return info, true, nil
				})
			}
			updateStatus(cfState, cf.checkpointTs)
			saveErrorFn := func(err *heartbeatpb.RunningError) {
				node, ok := c.lastState.Captures[err.Node]
				addr := err.Node
				if ok {
					addr = node.AdvertiseAddr
				}
				cfState.PatchTaskPosition(err.Node,
					func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
						if position == nil {
							position = &model.TaskPosition{}
						}
						position.Error = &model.RunningError{
							//Time:    err.Time, //todo: save time
							Addr:    addr,
							Code:    err.Code,
							Message: err.Message,
						}
						return position, true, nil
					})
			}
			if len(cf.State.Err) > 0 {
				for _, err := range cf.State.Err {
					saveErrorFn(err)
				}
			}
			if len(cf.State.Warning) > 0 {
				for _, err := range cf.State.Warning {
					saveErrorFn(err)
				}
			}
			return true
		})
		c.lastSaveTime = time.Now()
	}
}

// preflightCheck makes sure that the metadata in Etcd is complete enough to run the tick.
// If the metadata is not complete, such as when the ChangeFeedStatus is nil,
// this function will reconstruct the lost metadata and skip this tick.
func preflightCheck(changefeed *orchestrator.ChangefeedReactorState) (ok bool) {
	ok = true
	if changefeed.Status == nil {
		// complete the changefeed status when it is just created.
		changefeed.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status == nil {
					status = &model.ChangeFeedStatus{
						// changefeed status is nil when the changefeed has just created.
						CheckpointTs:      changefeed.Info.StartTs,
						MinTableBarrierTs: changefeed.Info.StartTs,
						AdminJobType:      model.AdminNone,
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	} else if changefeed.Status.MinTableBarrierTs == 0 {
		// complete the changefeed status when the TiCDC cluster is
		// upgraded from an old version(less than v6.7.0).
		changefeed.PatchStatus(
			func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
				if status != nil {
					if status.MinTableBarrierTs == 0 {
						status.MinTableBarrierTs = status.CheckpointTs
					}
					return status, true, nil
				}
				return status, false, nil
			})
		ok = false
	}

	if !ok {
		log.Info("changefeed preflight check failed, will skip this tick",
			zap.String("namespace", changefeed.ID.Namespace),
			zap.String("changefeed", changefeed.ID.ID),
			zap.Any("status", changefeed.Status), zap.Bool("ok", ok),
		)
	}

	return
}

func updateStatus(
	changefeed *orchestrator.ChangefeedReactorState,
	checkpointTs uint64,
) {
	if checkpointTs == 0 || changefeed == nil {
		return
	}
	changefeed.PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			changed := false
			if status == nil {
				return nil, false, nil
			}
			if status.CheckpointTs != checkpointTs {
				status.CheckpointTs = checkpointTs
				changed = true
			}
			return status, changed, nil
		})
}

func (c *coordinator) updateGCSafepoint(ctx context.Context) error {
	minCheckpointTs := c.calculateGCSafepoint()
	// When the changefeed starts up, CDC will do a snapshot read at
	// (checkpointTs - 1) from TiKV, so (checkpointTs - 1) should be an upper
	// bound for the GC safepoint.
	gcSafepointUpperBound := minCheckpointTs - 1
	err := c.gcManager.TryUpdateGCSafePoint(ctx, gcSafepointUpperBound, true)
	return errors.Trace(err)
}

// calculateGCSafepoint calculates GCSafepoint for different upstream.
// Note: we need to maintain a TiCDC service GC safepoint for each upstream TiDB cluster
// to prevent upstream TiDB GC from removing data that is still needed by TiCDC.
// GcSafepoint is the minimum checkpointTs of all changefeeds that replicating a same upstream TiDB cluster.
func (c *coordinator) calculateGCSafepoint() uint64 {
	var minCpts uint64 = math.MaxUint64

	for _, meta := range c.allChangefeeds {
		if meta.info == nil || !meta.info.NeedBlockGC() {
			continue
		}
		checkpointTs := meta.info.GetCheckpointTs(meta.status)
		if minCpts > checkpointTs {
			minCpts = checkpointTs
		}
	}
	// check if the upstream has a changefeed, if not we should update the gc safepoint
	if minCpts == math.MaxUint64 {
		ts := c.pdClock.CurrentTime()
		minCpts = oracle.GoTimeToTS(ts)
	}
	return minCpts
}

// handleRemovedNodes handles server changes.
func (c *coordinator) handleRemovedNodes(
	removed []common.NodeID,
) ([]*messaging.TargetMessage, error) {
	sentMsgs := make([]*messaging.TargetMessage, 0)
	if len(removed) > 0 {
		s.StateMachines.Ascend(func(id InferiorID, stateMachine *StateMachine) bool {
			for _, captureID := range removed {
				msg, affected := stateMachine.HandleCaptureShutdown(captureID)
				if msg != nil {
					sentMsgs = append(sentMsgs, msg)
				}
				if affected {
					// Cleanup its running task.
					s.RunningTasks.Delete(id)
					log.Info("remove running task",
						zap.String("stid", s.ID.String()),
						zap.String("id", id.String()))
				}
			}
			return true
		})
	}
	return sentMsgs, nil
}

func (c *coordinator) printStatus() {
	if time.Since(c.lastCheckTime) > time.Second*10 {
		workingTask := 0
		absentTask := 0
		commitTask := 0
		removingTask := 0
		c.supervisor.StateMachines.Ascend(func(key scheduler.InferiorID, value *scheduler.StateMachine) bool {
			switch value.State {
			case scheduler.SchedulerStatusAbsent:
				absentTask++
			case scheduler.SchedulerStatusCommiting:
				commitTask++
			case scheduler.SchedulerStatusWorking:
				workingTask++
			case scheduler.SchedulerStatusRemoving:
				removingTask++
			}
			return true
		})
		log.Info("changefeed status",
			zap.Int("absent", absentTask),
			zap.Int("commit", commitTask),
			zap.Int("working", workingTask),
			zap.Int("removing", removingTask),
			zap.Any("runningTask", c.supervisor.RunningTasks.Len()),
		)
		c.lastCheckTime = time.Now()
	}
}
