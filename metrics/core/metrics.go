// Copyright 2015 Google Inc. All Rights Reserved.
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

package core

// Provided by Kubelet/cadvisor.
var StandardMetrics = []Metric{
	MetricUptime,
	MetricCpuUsage,
	MetricMemoruUsage,
	MetricMemoryWorkingSet,
	MetricMemoryPageFaults,
	MetricMemoryMajorPageFaults,
	MetricNetworkRx,
	MetricNetworkRxErrors,
	MetricNetworkTx,
	MetricNetworkTxErrors}

// Metrics computed based on cluster state using Kubernetes API.
var AdditionalMetrics = []Metric{
	MetricCpuRequest,
	MetricCpuLimit,
	MetricMemoryRequest,
	MetricMemoryLimit}

// Computed based on corresponding StandardMetrics.
var RateMetrics = []Metric{
	MetricCpuUsageRate,
	MetricMemoryPageFaultsRate,
	MetricMemoryMajorPageFaultsRate,
	MetricNetworkRxRate,
	MetricNetworkRxErrorsRate,
	MetricNetworkTxRate,
	MetricNetworkTxErrorsRate}

var RateMetricsMapping = map[string]Metric{
	MetricCpuUsage.MetricDescriptor.Name:              MetricCpuUsageRate,
	MetricMemoryPageFaults.MetricDescriptor.Name:      MetricMemoryPageFaultsRate,
	MetricMemoryMajorPageFaults.MetricDescriptor.Name: MetricMemoryMajorPageFaultsRate,
	MetricNetworkRx.MetricDescriptor.Name:             MetricNetworkRxRate,
	MetricNetworkRxErrors.MetricDescriptor.Name:       MetricNetworkRxErrorsRate,
	MetricNetworkTx.MetricDescriptor.Name:             MetricNetworkTxRate,
	MetricNetworkTxErrors.MetricDescriptor.Name:       MetricNetworkTxErrorsRate}

var LabeledMetrics = []Metric{
	MetricFilesystemUsage,
	MetricFilesystemLimit,
}

var AllMetrics = append(append(append(StandardMetrics, AdditionalMetrics...), RateMetrics...), LabeledMetrics...)

// Definition of Standard Metrics.
var MetricUptime = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "uptime",
		Description: "Number of milliseconds since the container was started",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsMilliseconds,
	},
}

var MetricCpuUsage = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "cpu/usage",
		Description: "Cumulative CPU usage on all cores",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsNanoseconds,
	},
}

var MetricMemoruUsage = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/usage",
		Description: "Total memory usage",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
	},
}

var MetricMemoryWorkingSet = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/working_set",
		Description: "Total working set usage. Working set is the memory being used and not easily dropped by the kernel",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
	},
}

var MetricMemoryPageFaults = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/page_faults",
		Description: "Number of page faults",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsCount,
	},
}

var MetricMemoryMajorPageFaults = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/major_page_faults",
		Description: "Number of major page faults",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsCount,
	},
}

var MetricNetworkRx = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/rx",
		Description: "Cumulative number of bytes received over the network",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
	},
}

var MetricNetworkRxErrors = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/rx_errors",
		Description: "Cumulative number of errors while receiving over the network",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsCount,
	},
}

var MetricNetworkTx = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/tx",
		Description: "Cumulative number of bytes sent over the network",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
	},
}

var MetricNetworkTxErrors = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/tx_errors",
		Description: "Cumulative number of errors while sending over the network",
		Type:        MetricCumulative,
		ValueType:   ValueInt64,
		Units:       UnitsCount,
	},
}

// Definition of Additional Metrics.
var MetricCpuRequest = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "cpu/request",
		Description: "CPU request (the guaranteed amount of resources) in millicores. This metric is Kubernetes specific.",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsCount,
	},
}

var MetricCpuLimit = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "cpu/limit",
		Description: "CPU hard limit in millicores.",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsCount,
	},
}

var MetricMemoryRequest = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/request",
		Description: "Memory request (the guaranteed amount of resources) in bytes. This metric is Kubernetes specific.",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
	},
}

var MetricMemoryLimit = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/limit",
		Description: "Memory hard limit in bytes.",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
	},
}

// Definition of Rate Metrics.
var MetricCpuUsageRate = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "cpu/usage_rate",
		Description: "CPU usage on all cores in millicores",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsCount,
	},
}

var MetricMemoryPageFaultsRate = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/page_faults_rate",
		Description: "Rate of page faults in counts per second",
		Type:        MetricGauge,
		ValueType:   ValueFloat,
		Units:       UnitsCount,
	},
}

var MetricMemoryMajorPageFaultsRate = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "memory/major_page_faults_rate",
		Description: "Rate of major page faults in counts per second",
		Type:        MetricGauge,
		ValueType:   ValueFloat,
		Units:       UnitsCount,
	},
}

var MetricNetworkRxRate = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/rx_rate",
		Description: "Rate of bytes received over the network in bytes per second",
		Type:        MetricGauge,
		ValueType:   ValueFloat,
		Units:       UnitsCount,
	},
}

var MetricNetworkRxErrorsRate = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/rx_errors_rate",
		Description: "Rate of errors sending over the network in errors per second",
		Type:        MetricGauge,
		ValueType:   ValueFloat,
		Units:       UnitsCount,
	},
}

var MetricNetworkTxRate = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/tx_rate",
		Description: "Rate of bytes transmitted over the network in bytes per second",
		Type:        MetricGauge,
		ValueType:   ValueFloat,
		Units:       UnitsCount,
	},
}

var MetricNetworkTxErrorsRate = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "network/tx_errors_rate",
		Description: "Rate of errors transmitting over the network in errors per second",
		Type:        MetricGauge,
		ValueType:   ValueFloat,
		Units:       UnitsCount,
	},
}

// Labeled metrics

var MetricFilesystemUsage = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "filesystem/usage",
		Description: "Total number of bytes consumed on a filesystem",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
		Labels:      metricLabels,
	},
}

var MetricFilesystemLimit = Metric{
	MetricDescriptor: MetricDescriptor{
		Name:        "filesystem/limit",
		Description: "The total size of filesystem in bytes",
		Type:        MetricGauge,
		ValueType:   ValueInt64,
		Units:       UnitsBytes,
		Labels:      metricLabels,
	},
}

type MetricDescriptor struct {
	// The unique name of the metric.
	Name string `json:"name,omitempty"`

	// Description of the metric.
	Description string `json:"description,omitempty"`

	// Descriptor of the labels specific to this metric.
	Labels []LabelDescriptor `json:"labels,omitempty"`

	// Type and value of metric data.
	Type      MetricType `json:"type,omitempty"`
	ValueType ValueType  `json:"value_type,omitempty"`
	Units     UnitsType  `json:"units,omitempty"`
}

// Metric represents a resource usage stat metric.
type Metric struct {
	MetricDescriptor
}
