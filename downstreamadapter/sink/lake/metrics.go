// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lake

import (
	sinkmetrics "github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/spool"
)

func newSpoolMetrics(id common.ChangeFeedID) *spool.Metrics {
	k, f := id.Keyspace(), id.Name()
	return &spool.Metrics{
		MemoryBytes:        sinkmetrics.LakeBufferBytes.WithLabelValues(k, f, "memory"),
		DiskBytes:          sinkmetrics.LakeBufferBytes.WithLabelValues(k, f, "disk"),
		PendingPostEnqueue: sinkmetrics.LakeBufferEntries.WithLabelValues(k, f, "enqueue"),
		DiskQuotaWaiters:   sinkmetrics.LakeBufferEntries.WithLabelValues(k, f, "waiters"),
		SegmentCount:       sinkmetrics.LakeBufferEntries.WithLabelValues(k, f, "segments"),
		DiskQuotaWait:      sinkmetrics.LakeSpoolDuration.WithLabelValues(k, f),
		LoadedBytes:        sinkmetrics.LakeSpoolLoad.WithLabelValues(k, f),
		RotatedCount:       sinkmetrics.LakeSpoolRotations.WithLabelValues(k, f),
		Close: func() {
			for _, kind := range []string{"memory", "disk"} {
				sinkmetrics.LakeBufferBytes.DeleteLabelValues(k, f, kind)
			}
			for _, kind := range []string{"enqueue", "waiters", "segments"} {
				sinkmetrics.LakeBufferEntries.DeleteLabelValues(k, f, kind)
			}
			sinkmetrics.LakeSpoolDuration.DeleteLabelValues(k, f)
			sinkmetrics.LakeSpoolLoad.DeleteLabelValues(k, f)
			sinkmetrics.LakeSpoolRotations.DeleteLabelValues(k, f)
		},
	}
}
