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
	"time"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/scheduler"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type NodeState int

const (
	// StateUninitialized means the server status is unknown,
	// no heartbeat response received yet.
	StateUninitialized NodeState = 1
	// StateInitialized means scheduler has received heartbeat response.
	StateInitialized NodeState = 2
)

type NodeInitializer struct {
	id          string
	initialized bool

	nodes map[common.NodeID]*NodeStatus

	// track all status reported by remote inferiors when bootstrap
	initStatus map[common.NodeID][]scheduler.InferiorStatus

	coordinator *coordinator
}

type NodeStatus struct {
	state             NodeState
	node              *common.NodeInfo
	lastBootstrapTime time.Time
}

func NewNodeStatus(node *common.NodeInfo) *NodeStatus {
	return &NodeStatus{
		state: StateUninitialized,
		node:  node,
	}
}

// HandleAliveNodeUpdate update node liveness.
func (n *NodeInitializer) HandleAliveNodeUpdate(
	aliveCaptures map[common.NodeID]*common.NodeInfo,
) ([]*messaging.TargetMessage, error) {
	var removed []common.NodeID
	msgs := make([]*messaging.TargetMessage, 0)
	for id, info := range aliveCaptures {
		if _, ok := n.nodes[id]; !ok {
			// A new server.
			n.nodes[id] = NewNodeStatus(info)
			log.Info("find a new server",
				zap.String("ID", n.id),
				zap.String("addr", info.AdvertiseAddr),
				zap.String("server", id))
		}
	}

	// Find removed captures.
	for id, capture := range n.nodes {
		if _, ok := aliveCaptures[id]; !ok {
			log.Info("removed a server",
				zap.String("ID", n.id),
				zap.String("addr", capture.node.AdvertiseAddr),
				zap.String("server", id))
			delete(n.nodes, id)

			// Only update changes after initialization.
			if !n.initialized {
				continue
			}
			removed = append(removed, id)
		}
		// not removed, if not initialized, try to send bootstrap message again
		if capture.state == StateUninitialized &&
			time.Since(capture.lastBootstrapTime) > time.Millisecond*500 {
			msgs = append(msgs, n.coordinator.newBootstrapMessage(id))
			capture.lastBootstrapTime = time.Now()
		}
	}

	removedMsgs, err := n.coordinator.handleRemovedNodes(removed)
	if err != nil {
		log.Error("handle changes failed", zap.Error(err))
		return nil, errors.Trace(err)
	}
	msgs = append(msgs, removedMsgs...)
	return msgs, nil
}

// UpdateCaptureStatus update the server status after receive a bootstrap message from remote
// supervisor will cache the status if the supervisor is not initialized
func (n *NodeInitializer) UpdateCaptureStatus(from common.NodeID, statuses []scheduler.InferiorStatus) {
	c, ok := n.nodes[from]
	if !ok {
		log.Warn("server is not found",
			zap.String("ID", n.id),
			zap.String("server", from))
	}
	if c.state == StateUninitialized {
		c.state = StateInitialized
		log.Info("server initialized",
			zap.String("ID", n.id),
			zap.String("server", c.node.ID),
			zap.String("captureAddr", c.node.AdvertiseAddr))
	}
	// scheduler is not initialized, is still collecting the remote server status
	// cache the last one
	if !n.initialized {
		n.initStatus[from] = statuses

		// Check if this is the first time all captures are initialized.
		if n.checkAllNodeInitialized() {
			log.Info("all server initialized",
				zap.String("ID", n.id),
				zap.Int("captureCount", len(n.nodes)))
			statusMap := make(map[scheduler.InferiorID]map[common.NodeID]scheduler.InferiorStatus)
			for captureID, statuses := range n.initStatus {
				for _, status := range statuses {
					if _, ok := statusMap[status.GetInferiorID()]; !ok {
						statusMap[status.GetInferiorID()] = map[common.NodeID]scheduler.InferiorStatus{}
					}
					statusMap[status.GetInferiorID()][captureID] = status
				}
			}
			for id, status := range statusMap {
				statemachine, err := scheduler.NewStateMachine(id, status, n.coordinator.newChangefeed(id))
				if err != nil {
					log.Warn("scheduler new state machine failed")
					return
				}
				if statemachine.State != scheduler.SchedulerStatusAbsent {
					s.StateMachines.ReplaceOrInsert(id, statemachine)
				}
			}
			n.initialized = true
			n.initStatus = nil
		}
	}
}

// CheckAllNodeInitialized check if all server is initialized.
// returns true when all server reports the bootstrap response
func (n *NodeInitializer) CheckAllNodeInitialized() bool {
	return n.initialized && n.checkAllNodeInitialized()
}

func (n *NodeInitializer) checkAllNodeInitialized() bool {
	for _, nodeStatus := range n.nodes {
		if nodeStatus.state == StateUninitialized {
			return false
		}
	}
	return len(n.nodes) != 0
}
