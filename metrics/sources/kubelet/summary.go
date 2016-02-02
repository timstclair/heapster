// Copyright 2016 Google Inc. All Rights Reserved.
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
	"fmt"
	"time"

	. "k8s.io/heapster/metrics/core"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
)

var (
	summaryRequestLatency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "heapster",
			Subsystem: "kubelet_summary",
			Name:      "request_duration_microseconds",
			Help:      "The Kubelet summary request latencies in microseconds.",
		},
		[]string{"node"},
	)
)

func init() {
	prometheus.MustRegister(summaryRequestLatency)
}

// Kubelet-provided metrics for pod and system container.
type summaryMetricsSource struct {
	*kubeletMetricsSource

	// Whether this node requires the fall-back source.
	useFallback bool
}

func NewSummaryMetricsSource(kubelet *kubeletMetricsSource) MetricsSource {
	return &summaryMetricsSource{kubelet, false}
}

func (this *summaryMetricsSource) Name() string {
	return this.String()
}

func (this *summaryMetricsSource) String() string {
	return fmt.Sprintf("kubelet_summary:%s:%d", this.host.IP, this.host.Port)
}

func (this *summaryMetricsSource) ScrapeMetrics(start, end time.Time) *DataBatch {
	if this.useFallback {
		return this.kubeletMetricsSource.ScrapeMetrics(start, end)
	}

	result := &DataBatch{
		Timestamp:  end,
		MetricSets: map[string]*MetricSet{},
	}

	summary, err := func() (*stats.Summary, error) {
		startTime := time.Now()
		defer kubeletRequestLatency.WithLabelValues(this.hostname).Observe(float64(time.Since(startTime)))
		return this.kubeletClient.GetSummary(this.host)
	}()

	if err != nil {
		if IsNotFoundError(err) {
			glog.Warningf("Summary not found, using fallback: %v", err)
			this.useFallback = true
			return this.kubeletMetricsSource.ScrapeMetrics(start, end)
		}
		glog.Errorf("error while getting metrics summary from Kubelet: %v", err)
		return result
	}

	result.MetricSets = this.decodeSummary(summary)

	return result
}

const (
	RootFsKey = "/"
	LogsKey   = "logs"
)

// decodeSummary translates the kubelet stats.Summary API into the flattened heapster MetricSet API.
func (this *summaryMetricsSource) decodeSummary(summary *stats.Summary) map[string]*MetricSet {
	result := map[string]*MetricSet{}

	labels := map[string]string{
		LabelNodename.Key: this.nodename,
		LabelHostname.Key: this.hostname,
		LabelHostID.Key:   this.hostId,
	}

	this.decodeNodeStats(result, labels, &summary.Node)
	for _, pod := range summary.Pods {
		this.decodePodStats(result, labels, &pod)
	}

	return result
}

// Convenience method for labels deep copy.
func (this *summaryMetricsSource) cloneLabels(labels map[string]string) map[string]string {
	clone := make(map[string]string, len(labels))
	for k, v := range labels {
		clone[k] = v
	}
	return clone
}

func (this *summaryMetricsSource) decodeNodeStats(metrics map[string]*MetricSet, labels map[string]string, node *stats.NodeStats) {
	nodeMetrics := &MetricSet{
		Labels:         this.cloneLabels(labels),
		MetricValues:   map[string]MetricValue{},
		LabeledMetrics: []LabeledMetric{},
	}
	nodeMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypeNode

	startTime := node.StartTime.Time
	this.decodeUptime(nodeMetrics, startTime)
	this.decodeCPUStats(nodeMetrics, node.CPU, startTime)
	this.decodeMemoryStats(nodeMetrics, node.Memory, startTime)
	this.decodeNetworkStats(nodeMetrics, node.Network, startTime)
	this.decodeFsStats(nodeMetrics, RootFsKey, node.Fs)
	metrics[NodeKey(node.NodeName)] = nodeMetrics

	for _, container := range node.SystemContainers {
		key := NodeContainerKey(node.NodeName, container.Name)
		containerMetrics := this.decodeContainerStats(labels, &container)
		containerMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypeSystemContainer
		metrics[key] = containerMetrics
	}
}

func (this *summaryMetricsSource) decodePodStats(metrics map[string]*MetricSet, nodeLabels map[string]string, pod *stats.PodStats) {
	podMetrics := &MetricSet{
		Labels:         this.cloneLabels(nodeLabels),
		MetricValues:   map[string]MetricValue{},
		LabeledMetrics: []LabeledMetric{},
	}
	ref := pod.PodRef
	podMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypePod
	podMetrics.Labels[LabelPodId.Key] = ref.UID
	podMetrics.Labels[LabelPodName.Key] = ref.Name
	podMetrics.Labels[LabelNamespaceName.Key] = ref.Namespace
	// Needed for backward compatibility
	podMetrics.Labels[LabelPodNamespace.Key] = ref.Namespace

	startTime := pod.StartTime.Time
	this.decodeUptime(podMetrics, startTime)
	this.decodeNetworkStats(podMetrics, pod.Network, startTime)
	for _, vol := range pod.VolumeStats {
		this.decodeFsStats(podMetrics, vol.Name, &vol.FsStats)
	}
	metrics[PodKey(ref.Namespace, ref.Name)] = podMetrics

	for _, container := range pod.Containers {
		key := PodContainerKey(ref.Namespace, ref.Name, container.Name)
		metrics[key] = this.decodeContainerStats(podMetrics.Labels, &container)
	}
}

func (this *summaryMetricsSource) decodeContainerStats(podLabels map[string]string, container *stats.ContainerStats) *MetricSet {
	containerMetrics := &MetricSet{
		Labels:         this.cloneLabels(podLabels),
		MetricValues:   map[string]MetricValue{},
		LabeledMetrics: []LabeledMetric{},
	}
	containerMetrics.Labels[LabelMetricSetType.Key] = MetricSetTypePodContainer
	containerMetrics.Labels[LabelContainerName.Key] = container.Name

	startTime := container.StartTime.Time
	this.decodeUptime(containerMetrics, startTime)
	this.decodeCPUStats(containerMetrics, container.CPU, startTime)
	this.decodeMemoryStats(containerMetrics, container.Memory, startTime)
	this.decodeFsStats(containerMetrics, RootFsKey, container.Rootfs)
	this.decodeFsStats(containerMetrics, LogsKey, container.Logs)
	this.decodeUserDefinedMetrics(containerMetrics, container.UserDefinedMetrics)

	return containerMetrics
}

func (this *summaryMetricsSource) decodeUptime(metrics *MetricSet, startTime time.Time) {
	if startTime.IsZero() {
		return
	}

	uptime := uint64(time.Since(startTime).Nanoseconds() / time.Millisecond.Nanoseconds())
	this.addIntMetric(metrics, &MetricUptime, &uptime, startTime)
}

func (this *summaryMetricsSource) decodeCPUStats(metrics *MetricSet, cpu *stats.CPUStats, startTime time.Time) {
	if cpu == nil {
		return
	}

	this.addIntMetric(metrics, &MetricCpuUsage, cpu.UsageCoreNanoSeconds, startTime)

	// Need to translate from nanocores to millicores.
	if cpu.UsageNanoCores != nil {
		const nanoPerMilli uint64 = 1e6
		cores := *cpu.UsageNanoCores / nanoPerMilli
		this.addIntMetric(metrics, &MetricCpuUsageRate, &cores, startTime)
	}
}

func (this *summaryMetricsSource) decodeMemoryStats(metrics *MetricSet, memory *stats.MemoryStats, startTime time.Time) {
	if memory == nil {
		return
	}

	this.addIntMetric(metrics, &MetricMemoruUsage, memory.UsageBytes, startTime)
	this.addIntMetric(metrics, &MetricMemoryWorkingSet, memory.WorkingSetBytes, startTime)
	this.addIntMetric(metrics, &MetricMemoryPageFaults, memory.PageFaults, startTime)
	this.addIntMetric(metrics, &MetricMemoryMajorPageFaults, memory.MajorPageFaults, startTime)
}

func (this *summaryMetricsSource) decodeNetworkStats(metrics *MetricSet, network *stats.NetworkStats, startTime time.Time) {
	if network == nil {
		return
	}

	this.addIntMetric(metrics, &MetricNetworkRx, network.RxBytes, startTime)
	this.addIntMetric(metrics, &MetricNetworkRxErrors, network.RxErrors, startTime)
	this.addIntMetric(metrics, &MetricNetworkTx, network.TxBytes, startTime)
	this.addIntMetric(metrics, &MetricNetworkTxErrors, network.TxErrors, startTime)
}

func (this *summaryMetricsSource) decodeFsStats(metrics *MetricSet, fsKey string, fs *stats.FsStats) {
	if fs == nil {
		return
	}

	fsLabels := map[string]string{LabelResourceID.Key: fsKey}
	if fs.UsedBytes != nil {
		usage := LabeledMetric{
			Name:       MetricFilesystemUsage.Name,
			Labels:     fsLabels,
			ValueType:  ValueInt64,
			MetricType: MetricFilesystemUsage.Type,
			IntValue:   int64(*fs.UsedBytes),
		}
		metrics.LabeledMetrics = append(metrics.LabeledMetrics, usage)
	}

	if fs.CapacityBytes != nil {
		limit := LabeledMetric{
			Name:       MetricFilesystemLimit.Name,
			Labels:     fsLabels,
			ValueType:  ValueInt64,
			MetricType: MetricFilesystemLimit.Type,
			IntValue:   int64(*fs.CapacityBytes),
		}
		metrics.LabeledMetrics = append(metrics.LabeledMetrics, limit)
	}
}

func (this *summaryMetricsSource) decodeUserDefinedMetrics(metrics *MetricSet, udm []stats.UserDefinedMetric) {
	for _, metric := range udm {
		mv := MetricValue{}
		switch metric.Type {
		case stats.MetricGauge:
			mv.MetricType = MetricGauge
		case stats.MetricCumulative:
			mv.MetricType = MetricCumulative
		default:
			glog.V(4).Infof("Skipping %s: unknown custom metric type: %v", metric.Name, metric.Type)
			continue
		}

		mv.ValueType = ValueFloat
		mv.FloatValue = float32(metric.Value)

		metrics.MetricValues[CustomMetricPrefix+metric.Name] = mv
	}
}

// addIntMetric is a convenience method for adding the metric and value to the metric set.
func (this *summaryMetricsSource) addIntMetric(metrics *MetricSet, metric *Metric, value *uint64, startTime time.Time) {
	if value == nil {
		return
	}
	val := MetricValue{
		ValueType:  ValueInt64,
		MetricType: metric.Type,
		IntValue:   int64(*value),
	}
	if val.MetricType == MetricCumulative {
		val.Start = startTime
	}
	metrics.MetricValues[metric.Name] = val
}

type summaryProvider struct {
	kubeletProvider *kubeletProvider
}

func (this *summaryProvider) GetMetricsSources() []MetricsSource {
	kubeletSources := this.kubeletProvider.GetMetricsSources()
	sources := make([]MetricsSource, 0, len(kubeletSources))
	for _, source := range kubeletSources {
		kubelet, ok := source.(*kubeletMetricsSource)
		if !ok {
			glog.Errorf("Unexpected kubelet source: %q", source.Name())
			continue
		}

		sources = append(sources, NewSummaryMetricsSource(kubelet))
	}
	return sources
}

func NewSummaryProvider(kubeletProvider *kubeletProvider) MetricsSourceProvider {
	return &summaryProvider{kubeletProvider}
}
