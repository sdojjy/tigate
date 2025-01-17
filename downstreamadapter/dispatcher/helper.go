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

package dispatcher

import (
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/pkg/common"
	commonEvent "github.com/flowbehappy/tigate/pkg/common/event"
	"github.com/flowbehappy/tigate/utils/dynstream"
	"github.com/flowbehappy/tigate/utils/threadpool"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type SchemaIDToDispatchers struct {
	mutex sync.RWMutex
	m     map[int64]map[common.DispatcherID]interface{}
}

func NewSchemaIDToDispatchers() *SchemaIDToDispatchers {
	return &SchemaIDToDispatchers{
		m: make(map[int64]map[common.DispatcherID]interface{}),
	}
}

func (s *SchemaIDToDispatchers) Set(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; !ok {
		s.m[schemaID] = make(map[common.DispatcherID]interface{})
	}
	s.m[schemaID][dispatcherID] = struct{}{}
}

func (s *SchemaIDToDispatchers) Delete(schemaID int64, dispatcherID common.DispatcherID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[schemaID]; ok {
		delete(s.m[schemaID], dispatcherID)
	}
}

func (s *SchemaIDToDispatchers) Update(oldSchemaID int64, newSchemaID int64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.m[oldSchemaID]; ok {
		s.m[newSchemaID] = s.m[oldSchemaID]
		delete(s.m, oldSchemaID)
	} else {
		log.Error("schemaID not found", zap.Any("schemaID", oldSchemaID))
	}
}

func (s *SchemaIDToDispatchers) GetDispatcherIDs(schemaID int64) []common.DispatcherID {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if ids, ok := s.m[schemaID]; ok {
		dispatcherIDs := make([]common.DispatcherID, 0, len(ids))
		for id := range ids {
			dispatcherIDs = append(dispatcherIDs, id)
		}
		return dispatcherIDs
	}
	return nil
}

type ComponentStateWithMutex struct {
	mutex           sync.Mutex
	componentStatus heartbeatpb.ComponentState
}

func newComponentStateWithMutex(status heartbeatpb.ComponentState) *ComponentStateWithMutex {
	return &ComponentStateWithMutex{
		componentStatus: status,
	}
}

func (s *ComponentStateWithMutex) Set(status heartbeatpb.ComponentState) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.componentStatus = status
}

func (s *ComponentStateWithMutex) Get() heartbeatpb.ComponentState {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.componentStatus
}

type TsWithMutex struct {
	mutex sync.Mutex
	ts    uint64
}

func newTsWithMutex(ts uint64) *TsWithMutex {
	return &TsWithMutex{
		ts: ts,
	}
}

func (r *TsWithMutex) Set(ts uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.ts = ts
}

func (r *TsWithMutex) Get() uint64 {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.ts
}

/*
HeartBeatInfo is used to collect the message for HeartBeatRequest for each dispatcher.
Mainly about the progress of each dispatcher:
1. The checkpointTs of the dispatcher, shows that all the events whose ts <= checkpointTs are flushed to downstream successfully.
*/
type HeartBeatInfo struct {
	heartbeatpb.Watermark
	Id              common.DispatcherID
	TableSpan       *heartbeatpb.TableSpan
	ComponentStatus heartbeatpb.ComponentState
	IsRemoving      bool
}

// Resend Task is reponsible for resending the TableSpanBlockStatus message with ddl info to maintainer each 50ms.
// The task will be cancelled when the the dispatcher received the ack message from the maintainer
type ResendTask struct {
	message    *heartbeatpb.TableSpanBlockStatus
	dispatcher *Dispatcher
	taskHandle *threadpool.TaskHandle
}

func newResendTask(message *heartbeatpb.TableSpanBlockStatus, dispatcher *Dispatcher) *ResendTask {
	taskScheduler := GetDispatcherTaskScheduler()
	t := &ResendTask{
		message:    message,
		dispatcher: dispatcher,
	}
	t.taskHandle = taskScheduler.Submit(t, time.Now().Add(50*time.Millisecond))
	return t
}

func (t *ResendTask) Execute() time.Time {
	t.dispatcher.blockStatusesChan <- t.message
	return time.Now().Add(200 * time.Millisecond)
}

func (t *ResendTask) Cancel() {
	t.taskHandle.Cancel()
}

var DispatcherTaskScheduler threadpool.ThreadPool
var dispatcherTaskSchedulerOnce sync.Once

func GetDispatcherTaskScheduler() threadpool.ThreadPool {
	if DispatcherTaskScheduler == nil {
		dispatcherTaskSchedulerOnce.Do(func() {
			DispatcherTaskScheduler = threadpool.NewThreadPoolDefault()
		})
	}
	return DispatcherTaskScheduler
}

func SetDispatcherTaskScheduler(taskScheduler threadpool.ThreadPool) {
	DispatcherTaskScheduler = taskScheduler
}

// DispatcherEventsHandler is used to dispatcher the events received.
// If the event is a DML event, it will be added to the sink for writing to downstream.
// If the event is a resolved TS event, it will be update the resolvedTs of the dispatcher.
// If the event is a DDL event,
//  1. If it is a single table DDL, it will be added to the sink for writing to downstream(async).
//  2. If it is a multi-table DDL, We will generate a TableSpanBlockStatus message with ddl info to send to maintainer.
//     for the multi-table DDL, we will also generate a ResendTask to resend the TableSpanBlockStatus message with ddl info
//     to maintainer each 200ms to avoid message is missing.
//
// If the event is a Sync Point event, we deal it as a multi-table DDL event.
//
// We can handle multi events in batch if there only dml events and resovledTs events.
// For DDL event and Sync Point Event, we should handle them singlely.
// Thus, if a event is DDL event or Sync Point Event, we will only get one event at once.
// Otherwise, we can get a batch events.
// We always return block = true for Handle() except we only receive the resolvedTs events.
// So we only will reach next Handle() when previous events are all push downstream successfully.
type DispatcherEventsHandler struct {
}

func (h *DispatcherEventsHandler) Path(event DispatcherEvent) common.DispatcherID {
	return event.GetDispatcherID()
}

func (h *DispatcherEventsHandler) Handle(dispatcher *Dispatcher, events ...DispatcherEvent) bool {
	return dispatcher.HandleEvents(events)
}

type DispatcherEvent struct {
	commonEvent.Event
	isBatchable bool
}

func (d DispatcherEvent) IsBatchable() bool {
	return d.isBatchable
}

func NewDispatcherEvent(event commonEvent.Event) DispatcherEvent {
	dispatcherEvent := DispatcherEvent{
		Event: event,
	}
	switch event.GetType() {
	case commonEvent.TypeResolvedEvent, commonEvent.TypeDMLEvent:
		dispatcherEvent.isBatchable = true
	case commonEvent.TypeDDLEvent, commonEvent.TypeSyncPointEvent:
		dispatcherEvent.isBatchable = false
	default:
		log.Error("unknown event type", zap.Int("type", int(event.GetType())))
	}
	return dispatcherEvent
}

var dispatcherEventsDynamicStream dynstream.DynamicStream[common.DispatcherID, DispatcherEvent, *Dispatcher]
var dispatcherEventsDynamicStreamOnce sync.Once

func GetDispatcherEventsDynamicStream() dynstream.DynamicStream[common.DispatcherID, DispatcherEvent, *Dispatcher] {
	if dispatcherEventsDynamicStream == nil {
		dispatcherEventsDynamicStreamOnce.Do(func() {
			dispatcherEventsDynamicStream = dynstream.NewDynamicStream(&DispatcherEventsHandler{}, dynstream.NewOptionWithBatchSize(128))
			dispatcherEventsDynamicStream.Start()
		})
	}
	return dispatcherEventsDynamicStream
}

func SetDispatcherEventsDynamicStream(dynamicStream dynstream.DynamicStream[common.DispatcherID, DispatcherEvent, *Dispatcher]) {
	dispatcherEventsDynamicStream = dynamicStream
}

type DispatcherStatusWithID struct {
	id     common.DispatcherID
	status *heartbeatpb.DispatcherStatus
}

func NewDispatcherStatusWithID(dispatcherStatus *heartbeatpb.DispatcherStatus, dispatcherID common.DispatcherID) DispatcherStatusWithID {
	return DispatcherStatusWithID{
		status: dispatcherStatus,
		id:     dispatcherID,
	}
}

func (d *DispatcherStatusWithID) GetDispatcherStatus() *heartbeatpb.DispatcherStatus {
	return d.status
}

func (d *DispatcherStatusWithID) GetDispatcherID() common.DispatcherID {
	return d.id
}

func (d DispatcherStatusWithID) IsBatchable() bool {
	return true
}

// DispatcherStatusHandler is used to handle the DispatcherStatus event.
// Each dispatcher status may contain a ACK info or a dispatcher action or both.
// If we get a ack info, we need to check whether the ack is for the current pending ddl event.
// If so, we can cancel the resend task.
// If we get a dispatcher action, we need to check whether the action is for the current pending ddl event.
// If so, we can deal the ddl event based on the action.
// 1. If the action is a write, we need to add the ddl event to the sink for writing to downstream(async).
// 2. If the action is a pass, we just need to pass the event in tableProgress(for correct calculation) and
// wake the dispatcherEventsHandler to handle the event.
type DispatcherStatusHandler struct {
}

func (h *DispatcherStatusHandler) Path(event DispatcherStatusWithID) common.DispatcherID {
	return event.GetDispatcherID()
}

func (h *DispatcherStatusHandler) Handle(dispatcher *Dispatcher, events ...DispatcherStatusWithID) (await bool) {
	for _, event := range events {
		dispatcher.HandleDispatcherStatus(event.GetDispatcherStatus())
	}
	return false
}

var dispatcherStatusDynamicStream dynstream.DynamicStream[common.DispatcherID, DispatcherStatusWithID, *Dispatcher]
var dispatcherStatusDynamicStreamOnce sync.Once

func GetDispatcherStatusDynamicStream() dynstream.DynamicStream[common.DispatcherID, DispatcherStatusWithID, *Dispatcher] {
	if dispatcherStatusDynamicStream == nil {
		dispatcherStatusDynamicStreamOnce.Do(func() {
			dispatcherStatusDynamicStream = dynstream.NewDynamicStream(&DispatcherStatusHandler{})
			dispatcherStatusDynamicStream.Start()
		})
	}
	return dispatcherStatusDynamicStream
}

func SetDispatcherStatusDynamicStream(dynamicStream dynstream.DynamicStream[common.DispatcherID, DispatcherStatusWithID, *Dispatcher]) {
	dispatcherStatusDynamicStream = dynamicStream
}

// TableNameStore is used to store all the table name(schema name with table name).
// TableNameStore only exists in the table trigger event dispatcher, which means each changefeed only has one TableNameStore.
// TableNameStore support to provide all the table name of the specified ts(only support incremental ts)
// When meeting Create Table / Create Tables / Drop Table / Rename Table / Rename Tables / Drop Database / Recover Table
// TableNameStore need to update the table name info.
type TableNameStore struct {
	// store all the existing table which existed at the latest query ts
	existingTables map[string]map[string]*commonEvent.SchemaTableName // databaseName -> {tableName -> SchemaTableName}
	// store the change of table name from the latest query ts to now(latest event)
	latestTableChanges *LatestTableChanges
}

func NewTableNameStore() *TableNameStore {
	return &TableNameStore{
		existingTables:     make(map[string]map[string]*commonEvent.SchemaTableName),
		latestTableChanges: &LatestTableChanges{m: make(map[uint64]*commonEvent.TableNameChange)},
	}
}

func (s *TableNameStore) AddEvent(event *commonEvent.DDLEvent) {
	if event.TableNameChange != nil {
		s.latestTableChanges.Add(event)
	}
}

// GetAllTableNames only will be called when maintainer send message to ask dispatcher to write checkpointTs to downstream.
// So the ts must be <= the latest received event ts of table trigger event dispatcher.
func (s *TableNameStore) GetAllTableNames(ts uint64) []*commonEvent.SchemaTableName {
	s.latestTableChanges.mutex.Lock()
	if len(s.latestTableChanges.m) > 0 {
		// update the existingTables with the latest table changes <= ts
		for commitTs, tableNameChange := range s.latestTableChanges.m {
			if commitTs <= ts {
				if tableNameChange.DropDatabaseName != "" {
					delete(s.existingTables, tableNameChange.DropDatabaseName)
				} else {
					for _, addName := range tableNameChange.AddName {
						if s.existingTables[addName.SchemaName] == nil {
							s.existingTables[addName.SchemaName] = make(map[string]*commonEvent.SchemaTableName, 0)
						}
						s.existingTables[addName.SchemaName][addName.TableName] = &addName
					}
					for _, dropName := range tableNameChange.DropName {
						delete(s.existingTables[dropName.SchemaName], dropName.TableName)
						if len(s.existingTables[dropName.SchemaName]) == 0 {
							delete(s.existingTables, dropName.SchemaName)
						}
					}
				}
			}
			delete(s.latestTableChanges.m, commitTs)
		}
	}

	s.latestTableChanges.mutex.Unlock()

	tableNames := make([]*commonEvent.SchemaTableName, 0)
	for _, tables := range s.existingTables {
		for _, tableName := range tables {
			tableNames = append(tableNames, tableName)
		}
	}

	return tableNames
}

type LatestTableChanges struct {
	mutex sync.Mutex
	m     map[uint64]*commonEvent.TableNameChange
}

func (l *LatestTableChanges) Add(ddlEvent *commonEvent.DDLEvent) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	l.m[ddlEvent.FinishedTs] = ddlEvent.TableNameChange
}
