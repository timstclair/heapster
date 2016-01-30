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

package kubelet

import (
	"strings"
	"time"

	"github.com/golang/glog"
	cadvisor "github.com/google/cadvisor/info/v1"
	. "k8s.io/heapster/metrics/core"
)

type cadvisorDecoder struct {
	// Returns whether this metric is present.
	HasValue func(*cadvisor.ContainerSpec) bool
	// Returns a slice of internal point objects that contain metric values and associated labels.
	GetValue func(*cadvisor.ContainerSpec, *cadvisor.ContainerStats) MetricValue
}
type labeledCadvisorDecoder struct {
	// Returns whether this metric is present.
	HasLabeledMetric func(*cadvisor.ContainerSpec) bool
	// Returns a slice of internal point objects that contain metric values and associated labels.
	GetLabeledMetric func(*cadvisor.ContainerSpec, *cadvisor.ContainerStats) []LabeledMetric
}

// Definition of StandardMetrics decoders from the cAdvisor API. Key is the metric name.
var standardCadvisorDecoders = map[string]cadvisorDecoder{
	// TODO: get uptime from status
	MetricUptime.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return !spec.CreationTime.IsZero()
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   time.Since(spec.CreationTime).Nanoseconds() / time.Millisecond.Nanoseconds(),
				Start:      spec.CreationTime}
		},
	},

	MetricCpuUsage.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasCpu
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   int64(stat.Cpu.Usage.Total),
				Start:      spec.CreationTime}
		},
	},

	MetricMemoruUsage.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasMemory
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricGauge,
				IntValue:   int64(stat.Memory.Usage)}
		},
	},

	MetricMemoryWorkingSet.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasMemory
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricGauge,
				IntValue:   int64(stat.Memory.WorkingSet)}
		},
	},

	MetricMemoryPageFaults.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasMemory
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   int64(stat.Memory.ContainerData.Pgfault),
				Start:      spec.CreationTime}
		},
	},

	MetricMemoryMajorPageFaults.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasMemory
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   int64(stat.Memory.ContainerData.Pgmajfault),
				Start:      spec.CreationTime}
		},
	},

	MetricNetworkRx.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasNetwork
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   int64(stat.Network.RxBytes),
				Start:      spec.CreationTime}
		},
	},

	MetricNetworkRxErrors.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasNetwork
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   int64(stat.Network.RxErrors),
				Start:      spec.CreationTime}
		},
	},

	MetricNetworkTx.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasNetwork
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   int64(stat.Network.TxBytes),
				Start:      spec.CreationTime}
		},
	},

	MetricNetworkTxErrors.Name: {
		HasValue: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasNetwork
		},
		GetValue: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) MetricValue {
			return MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricCumulative,
				IntValue:   int64(stat.Network.TxErrors),
				Start:      spec.CreationTime}
		},
	},
}

// Definition of LabeledMetrics decoders from the cAdvisor API. Key is the metric name.
var labeledCadvisorDecoders = map[string]labeledCadvisorDecoder{
	MetricFilesystemUsage.Name: {
		HasLabeledMetric: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasFilesystem
		},
		GetLabeledMetric: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) []LabeledMetric {
			result := make([]LabeledMetric, 0, len(stat.Filesystem))
			for _, fs := range stat.Filesystem {
				result = append(result, LabeledMetric{
					Name: "filesystem/usage",
					Labels: map[string]string{
						LabelResourceID.Key: fs.Device,
					},
					ValueType:  ValueInt64,
					MetricType: MetricCumulative,
					IntValue:   int64(fs.Usage),
				})
			}
			return result
		},
	},

	MetricFilesystemLimit.Name: {
		HasLabeledMetric: func(spec *cadvisor.ContainerSpec) bool {
			return spec.HasFilesystem
		},
		GetLabeledMetric: func(spec *cadvisor.ContainerSpec, stat *cadvisor.ContainerStats) []LabeledMetric {
			result := make([]LabeledMetric, 0, len(stat.Filesystem))
			for _, fs := range stat.Filesystem {
				result = append(result, LabeledMetric{
					Name: "filesystem/limit",
					Labels: map[string]string{
						LabelResourceID.Key: fs.Device,
					},
					ValueType:  ValueInt64,
					MetricType: MetricCumulative,
					IntValue:   int64(fs.Limit),
				})
			}
			return result
		},
	},
}

func decodeCadvisorMetrics(source *kubeletMetricsSource, c *cadvisor.ContainerInfo) (string, *MetricSet) {
	var metricSetKey string
	cMetrics := &MetricSet{
		MetricValues: map[string]MetricValue{},
		Labels: map[string]string{
			LabelNodename.Key: source.nodename,
			LabelHostname.Key: source.hostname,
			LabelHostID.Key:   source.hostId,
		},
		LabeledMetrics: []LabeledMetric{},
	}

	if isNode(c) {
		metricSetKey = NodeKey(source.nodename)
		cMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypeNode
	} else if isSysContainer(c) {
		cName := getSysContainerName(c)
		metricSetKey = NodeContainerKey(source.nodename, cName)
		cMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypeSystemContainer
		cMetrics.Labels[LabelContainerName.Key] = cName
	} else {
		cName := c.Spec.Labels[kubernetesContainerLabel]
		ns := c.Spec.Labels[kubernetesPodNamespaceLabel]
		podName := c.Spec.Labels[kubernetesPodNameLabel]

		// Support for kubernetes 1.0.*
		if ns == "" && strings.Contains(podName, "/") {
			tokens := strings.SplitN(podName, "/", 2)
			if len(tokens) == 2 {
				ns = tokens[0]
				podName = tokens[1]
			}
		}
		if cName == "" {
			// Better this than nothing. This is a temporary hack for new heapster to work
			// with Kubernetes 1.0.*.
			// TODO: fix this with POD list.
			// Parsing name like:
			// k8s_kube-ui.7f9b83f6_kube-ui-v1-bxj1w_kube-system_9abfb0bd-811f-11e5-b548-42010af00002_e6841e8d
			pos := strings.Index(c.Name, ".")
			if pos >= 0 {
				// remove first 4 chars.
				cName = c.Name[len("k8s_"):pos]
			}
		}

		if cName == "" || ns == "" || podName == "" {
			glog.Errorf("Missing metadata for container %v. Got: %+v", c.Name, c.Spec.Labels)
			return "", nil
		}

		if cName == infraContainerName {
			metricSetKey = PodKey(ns, podName)
			cMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypePod
		} else {
			metricSetKey = PodContainerKey(ns, podName, cName)
			cMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypePodContainer
			cMetrics.Labels[LabelContainerName.Key] = cName
			cMetrics.Labels[LabelContainerBaseImage.Key] = c.Spec.Image
		}
		cMetrics.Labels[LabelPodId.Key] = c.Spec.Labels[kubernetesPodUID]
		cMetrics.Labels[LabelPodName.Key] = podName
		cMetrics.Labels[LabelNamespaceName.Key] = ns
		// Needed for backward compatibility
		cMetrics.Labels[LabelPodNamespace.Key] = ns
	}

	for _, metric := range StandardMetrics {
		if metric.HasValue != nil && metric.HasValue(&c.Spec) {
			cMetrics.MetricValues[metric.Name] = metric.GetValue(&c.Spec, c.Stats[0])
		}
	}

	for _, metric := range LabeledMetrics {
		if metric.HasLabeledMetric != nil && metric.HasLabeledMetric(&c.Spec) {
			labeledMetrics := metric.GetLabeledMetric(&c.Spec, c.Stats[0])
			cMetrics.LabeledMetrics = append(cMetrics.LabeledMetrics, labeledMetrics...)
		}
	}

	if c.Spec.HasCustomMetrics {
	metricloop:
		for _, spec := range c.Spec.CustomMetrics {
			if cmValue, ok := c.Stats[0].CustomMetrics[spec.Name]; ok && cmValue != nil && len(cmValue) >= 1 {
				newest := cmValue[0]
				for _, metricVal := range cmValue {
					if newest.Timestamp.Before(metricVal.Timestamp) {
						newest = metricVal
					}
				}
				mv := MetricValue{}
				switch spec.Type {
				case cadvisor.MetricGauge:
					mv.MetricType = MetricGauge
				case cadvisor.MetricCumulative:
					mv.MetricType = MetricCumulative
				default:
					glog.V(4).Infof("Skipping %s: unknown custom metric type: %v", spec.Name, spec.Type)
					continue metricloop
				}

				switch spec.Format {
				case cadvisor.IntType:
					mv.ValueType = ValueInt64
					mv.IntValue = newest.IntValue
				case cadvisor.FloatType:
					mv.ValueType = ValueFloat
					mv.FloatValue = float32(newest.FloatValue)
				default:
					glog.V(4).Infof("Skipping %s: unknown custom metric format", spec.Name, spec.Format)
					continue metricloop
				}

				cMetrics.MetricValues[CustomMetricPrefix+spec.Name] = mv
			}
		}
	}

	// This is temporary workaround to support cpu/usege_rate metric.
	if currentVal, ok := cMetrics.MetricValues["cpu/usage"]; ok {
		if lastVal, ok := source.cpuLastVal[metricSetKey]; ok {
			// cpu/usage values are in nanoseconds; we want to have it in millicores (that's why constant 1000 is here).
			rateVal := 1000 * (currentVal.IntValue - lastVal.Val) / (c.Stats[0].Timestamp.UnixNano() - lastVal.Timestamp.UnixNano())
			cMetrics.MetricValues["cpu/usage_rate"] = MetricValue{
				ValueType:  ValueInt64,
				MetricType: MetricGauge,
				IntValue:   rateVal,
			}
		}
		source.cpuLastVal[metricSetKey] = cpuVal{
			Val:       currentVal.IntValue,
			Timestamp: c.Stats[0].Timestamp,
		}
	}
	// TODO: add labels: LabelPodNamespaceUID, LabelLabels, LabelResourceID

	return metricSetKey, cMetrics
}
