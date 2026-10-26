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

package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	DatabendBufferBytes    = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "ticdc", Subsystem: "databend", Name: "buffer_bytes", Help: "Local Databend memory and physical spool bytes."}, []string{"namespace", "changefeed", "kind"})
	DatabendBufferEntries  = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "ticdc", Subsystem: "databend", Name: "buffer_entries", Help: "Local Databend pending enqueue, segment and quota waiter counts."}, []string{"namespace", "changefeed", "kind"})
	DatabendSpoolDuration  = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "ticdc", Subsystem: "databend", Name: "spool_wait_seconds", Help: "Databend spool quota wait time.", Buckets: prometheus.ExponentialBuckets(0.001, 2, 20)}, []string{"namespace", "changefeed"})
	DatabendSpoolLoad      = prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "ticdc", Subsystem: "databend", Name: "spool_load_bytes", Help: "Databend encoded bytes loaded from local spool.", Buckets: prometheus.ExponentialBuckets(1024, 4, 9)}, []string{"namespace", "changefeed"})
	DatabendSpoolRotations = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "ticdc", Subsystem: "databend", Name: "spool_rotations_total", Help: "Databend local spool segment rotations."}, []string{"namespace", "changefeed"})
)

func InitDatabendMetrics(registry *prometheus.Registry) {
	registry.MustRegister(DatabendBufferBytes, DatabendBufferEntries, DatabendSpoolDuration, DatabendSpoolLoad, DatabendSpoolRotations)
}
