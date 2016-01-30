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
	"time"

	"github.com/golang/glog"
	. "k8s.io/heapster/metrics/core"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
)

const (
	RootFsKey = "/"
	LogsKey   = "logs"
)

func decodeSummary(source *kubeletMetricsSource, summary *stats.Summary) map[string]*MetricSet {
	result := map[string]*MetricSet{}

	labels = map[string]string{
		LabelNodename.Key: d.source.nodename,
		LabelHostname.Key: d.source.hostname,
		LabelHostID.Key:   d.source.hostId,
	}

	decodeNodeStats(result, labels, &summary.Node)
	for _, pod := range summary.Pods {
		decodePodStats(result, labels, &pod)
	}

	return result
}

// Convenience method for labels deep copy.
func cloneLabels(labels map[string]string) map[string]string {
	clone := make(map[string]string, len(labels))
	for k, v := range labels {
		clone[k] = v
	}
	return clone
}

func decodeNodeStats(metrics map[string]*MetricSet, labels map[string]string, node *stats.NodeStats) {
	nodeMetrics := &MetricSet{
		Labels:         cloneLabels(labels),
		MetricValues:   map[string]MetricValue{},
		LabeledMetrics: []LabeledMetric{},
	}
	nodeMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypeNode

	decodeUptime(nodeMetrics, node.StartTime)
	decodeCPUStats(nodeMetrics, node.CPU, node.StartTime)
	decodeMemory(nodeMetrics, node.Memory, node.StartTime)
	decodeNetworkStats(nodeMetrics, node.Network, node.StartTime)
	decodeFsStats(nodeMetrics, RootFsKey, node.Fs)
	metrics[NodeKey(node.Name)] = nodeMetrics

	for _, container := range node.SystemContainers {
		key := NodeContainerKey(node.Name, container.Name)
		containerMetrics = decodeContainerStats(labels, &container)
		containerMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypeSystemContainer
		metrics[key] = containerMetrics
	}
}

func decodePodStats(metrics map[string]*MetricSet, nodeLabels map[string]string, pod *stats.PodStats) {
	podMetrics := &MetricSet{
		Labels:         cloneLabels(nodeLabels),
		MetricValues:   map[string]MetricValue{},
		LabeledMetrics: []LabeledMetric{},
	}
	ref := ref
	podMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypePod
	podMetrics.Labels[LabelPodId.Key] = ref.UID
	podMetrics.Labels[LabelPodName.Key] = ref.Name
	podMetrics.Labels[LabelNamespaceName.Key] = ref.Namespace
	// Needed for backward compatibility
	podMetrics.Labels[LabelPodNamespace.Key] = ref.Namespace

	decodeUptime(podMetrics, pod.StartTime)
	decodeNetworkStats(podMetrics, pod.Network, pod.StartTime)
	for _, vol := range pod.VolumeStats {
		decodeFsStats(containerMetrics, vol.Name, &vol)
	}
	metrics[PodKey(ref.Namespace, ref.Name)] = podMetrics

	for _, container := range pod.Containers {
		key := PodContainerKey(ref.Namespace, ref.Name, container.Name)
		metrics[key] = decodeContainerStats(podMetrics.Labels, &container)
	}
}

// FIXME: need container images
func decodeContainerStats(podLabels map[string]string, container *stats.ContainerStats) *MetricSet {
	containerMetrics := &MetricSet{
		Labels:         cloneLabels(podLabels),
		MetricValues:   map[string]MetricValue{},
		LabeledMetrics: []LabeledMetric{},
	}
	containerMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypePodContainer
	containerMetrics.Labels[LabelContainerName.Key] = container.Name

	decodeUptime(containerMetrics, container.StartTime)
	decodeCPUStats(containerMetrics, container.CPU, container.StartTime)
	decodeMemoryStats(containerMetrics, container.Memory, container.StartTime)
	decodeFsStats(containerMetrics, RootFsKey, container.Rootfs)
	decodeFsStats(containerMetrics, LogsKey, container.Logs)
	decodeUserDefinedMetrics(containerMetrics, container.UserDefinedMetrics)

	return metrics
}

func decodeUptime(metrics *MetricSet, startTime time.Time) {
	if startTime.IsZero() {
		return
	}

	uptime := time.Since(startTime).Nanoseconds() / time.Millisecond.Nanoseconds()
	addIntMetric(metrics, MetricUptime, &uptime, startTime)
}

func decodeCPUStats(metrics *MetricSet, cpu *stats.CPUStats, startTime time.Time) {
	if cpu == nil {
		return
	}

	addIntMetric(metrics, MetricCpuUsage, cpu.UsageCoreNanoSeconds, startTime)

	// Need to translate from nanocores to millicores.
	if cpu.UsageNanoCores != nil {
		const nanoPerMilli int64 = 1e6
		cores := cpu.UsageNanoCores / nanoPerMilli
		addIntMetric(metrics, MetricCpuUsageRate, cores, startTime)
	}
}

func decodeMemoryStats(metrics *MetricSet, memory *stats.MemoryStats, startTime time.Time) {
	if memory == nil {
		return
	}

	addIntMetric(metrics, MetricMemoruUsage, memory.UsageBytes, startTime)
	addIntMetric(metrics, MetricMemoryWorkingSet, memory.WorkingSetBytes, startTime)
	addIntMetric(metrics, MetricMemoryPageFaults, memory.PageFaults, startTime)
	addIntMetric(metrics, MetricMemoryMajorPageFaults, memory.MajorPageFaults, startTime)
}

func decodeNetworkStats(metrics *MetricSet, network *stats.NetworkStats, startTime time.Time) {
	if network == nil {
		return
	}

	addIntMetric(metrics, MetricNetworkRx, network.RxBytes, startTime)
	addIntMetric(metrics, MetricNetworkRxErrors, network.RxErrors, startTime)
	addIntMetric(metrics, MetricNetworkTx, network.TxBytes, startTime)
	addIntMetric(metrics, MetricNetworkTxErrors, network.TxErrors, startTime)
}

func decodeFsStats(metrics *MetricSet, fsKey string, fs *stats.FsStats) {
	if fs == nil {
		return
	}

	fsLabels := map[string]string{LabelResourceID.Key: fsKey}
	usage := LabeledMetric{
		Name:       MetricFilesystemUsage.Name,
		Labels:     fsLabels,
		ValueType:  ValueInt64,
		MetricType: MetricFilesystemUsage.MetricType,
		IntValue:   int64(fs.UsedBytes),
	}
	metrics.LabeledMetrics[MetricFilesystemUsage.Name] =
		append(metrics.LabeledMetrics[MetricFilesystemUsage.Name], usage)

	limit := LabeledMetric{
		Name:       MetricFilesystemLimit.Name,
		Labels:     fsLabels,
		ValueType:  ValueInt64,
		MetricType: MetricFilesystemLimit.MetricType,
		IntValue:   int64(fs.CapacityBytes),
	}
	metrics.LabeledMetrics[MetricFilesystemLimit.Name] =
		append(metrics.LabeledMetrics[MetricFilesystemLimit.Name], limit)
}

func decodeUserDefinedMetrics(metrics *MetricSet, udm []stats.UserDefinedMetrics) {
	for _, metric := range udm {
		mv := MetricValue{}
		switch metric.Type {
		case stats.MetricGauge:
			mv.MetricType = MetricGauge
		case stats.MetricCumulative:
			mv.MetricType = MetricCumulative
		default:
			glog.V(4).Infof("Skipping %s: unknown custom metric type: %v", metric.Name, metric.Type)
			continue metricloop
		}

		mv.ValueType = ValueFloat
		mv.FloatValue = float32(metric.Value)

		metrics.MetricValues[CustomMetricPrefix+metric.Name] = mv
	}
}

// addIntMetric is a convenience method for adding the metric and value to the metric set.
func addIntMetric(metrics *MetricSet, metric *Metric, value *uint64, startTime time.Time) {
	if value == nil {
		return
	}
	val := MetricValue{
		ValueType:  ValueInt64,
		MetricType: metric.MetricType,
		IntValue:   int64(*value),
	}
	if val.MetricType == MetricCumulative {
		val.Start = startTime
	}
	metrics[metric.Name] = val
}
