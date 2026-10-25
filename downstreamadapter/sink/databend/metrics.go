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

package databend

import (
	sinkmetrics "github.com/pingcap/ticdc/downstreamadapter/sink/metrics"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/spool"
)

func newSpoolMetrics(id common.ChangeFeedID) *spool.Metrics {
	k, f := id.Keyspace(), id.Name()
	return &spool.Metrics{
		MemoryBytes:        sinkmetrics.DatabendBufferBytes.WithLabelValues(k, f, "memory"),
		DiskBytes:          sinkmetrics.DatabendBufferBytes.WithLabelValues(k, f, "disk"),
		PendingPostEnqueue: sinkmetrics.DatabendBufferEntries.WithLabelValues(k, f, "enqueue"),
		DiskQuotaWaiters:   sinkmetrics.DatabendBufferEntries.WithLabelValues(k, f, "waiters"),
		SegmentCount:       sinkmetrics.DatabendBufferEntries.WithLabelValues(k, f, "segments"),
		DiskQuotaWait:      sinkmetrics.DatabendSpoolDuration.WithLabelValues(k, f),
		LoadedBytes:        sinkmetrics.DatabendSpoolLoad.WithLabelValues(k, f),
		RotatedCount:       sinkmetrics.DatabendSpoolRotations.WithLabelValues(k, f),
		Close: func() {
			for _, kind := range []string{"memory", "disk"} {
				sinkmetrics.DatabendBufferBytes.DeleteLabelValues(k, f, kind)
			}
			for _, kind := range []string{"enqueue", "waiters", "segments"} {
				sinkmetrics.DatabendBufferEntries.DeleteLabelValues(k, f, kind)
			}
			sinkmetrics.DatabendSpoolDuration.DeleteLabelValues(k, f)
			sinkmetrics.DatabendSpoolLoad.DeleteLabelValues(k, f)
			sinkmetrics.DatabendSpoolRotations.DeleteLabelValues(k, f)
		},
	}
}
