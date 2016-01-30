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
	"fmt"
	"net/url"
	"time"

	. "k8s.io/heapster/metrics/core"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	kube_api "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/client/cache"
	kube_client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/fields"
)

const (
	infraContainerName = "POD"
	// TODO: following constants are copied from k8s, change to use them directly
	kubernetesPodNameLabel      = "io.kubernetes.pod.name"
	kubernetesPodNamespaceLabel = "io.kubernetes.pod.namespace"
	kubernetesPodUID            = "io.kubernetes.pod.uid"
	kubernetesContainerLabel    = "io.kubernetes.container.name"

	CustomMetricPrefix = "CM:"
)

var (
	// The Kubelet request latencies in microseconds.
	kubeletRequestLatency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "heapster",
			Subsystem: "kubelet",
			Name:      "request_duration_microseconds",
			Help:      "The Kubelet request latencies in microseconds.",
		},
		[]string{"node"},
	)
)

func init() {
	prometheus.MustRegister(kubeletRequestLatency)
}

type cpuVal struct {
	Val       int64
	Timestamp time.Time
}

type sourceType int

const (
	// Legacy source type for older kubelets, served at /stats/container.
	sourceTypeCadvisor sourceType = "cadvisor"
	// Preferred source type for newer kubelets, served at /stats/summary.
	sourceTypeSummary sourceType = "summary"
)

// Kubelet-provided metrics for pod and system container.
type kubeletMetricsSource struct {
	host          Host
	kubeletClient *KubeletClient
	nodename      string
	hostname      string
	hostId        string
	cpuLastVal    map[string]cpuVal
	sourceType    sourceType
}

func (this *kubeletMetricsSource) Name() string {
	return this.String()
}

func (this *kubeletMetricsSource) String() string {
	return fmt.Sprintf("kubelet:%s:%d", this.host.IP, this.host.Port)
}

func (this *kubeletMetricsSource) ScrapeMetrics(start, end time.Time) *DataBatch {
	switch this.sourceType {
	case sourceTypeSummary:
		return this.scrapeSummary(end)

	case sourceTypeCadvisor:
		return this.scrapeCadvisor(start, end)

	default:
		// source type unset
		hasSummary, err := this.kubeletClient.HasSummaryEndpoint(this.host)
		if err != nil {
			glog.Errorf("error determining kubelet source type: %v", err)
			return &DataBatch{
				Timestamp:  end,
				MetricSets: map[string]*MetricSet{},
			}
		}

		if hasSummary {
			this.sourceType = sourceTypeSummary
		} else {
			this.sourceType = sourceTypeCadvisor
		}

		return this.ScrapeMetrics(start, end)
	}
}

func (this *kubeletMetricsSource) scrapeSummary(end time.Time) *DataBatch {
	result := &DataBatch{
		Timestamp:  end,
		MetricSets: map[string]*MetricSet{},
	}

	summary, err := func() {
		startTime := time.Now()
		defer kubeletRequestLatency.WithLabelValues(this.hostname).Observe(float64(time.Since(startTime)))
		return this.kubeletClient.GetSummary(this.host)
	}()

	if err != nil {
		glog.Errorf("error while getting metrics summary from Kubelet: %v", err)
		return result
	}

	result.MetricSets = decodeSummary(this, summary)

	return result
}

func (this *kubeletMetricsSource) scrapeCadvisor(start, end time.Time) *DataBatch {
	result := &DataBatch{
		Timestamp:  end,
		MetricSets: map[string]*MetricSet{},
	}
	containers, err := func() {
		startTime := time.Now()
		defer kubeletRequestLatency.WithLabelValues(this.hostname).Observe(float64(time.Since(startTime)))
		return this.kubeletClient.GetAllRawContainers(this.host, start, end)
	}()
	if err != nil {
		glog.Errorf("error while getting containers from Kubelet: %v", err)
	}
	glog.Infof("successfully obtained stats for %v containers", len(containers))

	keys := make(map[string]bool)
	for _, c := range containers {
		name, metrics := decodeCadvisorMetrics(this, &c)
		if name == "" {
			continue
		}
		result.MetricSets[name] = metrics
		keys[name] = true
	}
	// No remember data for pods that have been removed.
	for key := range this.cpuLastVal {
		if _, ok := keys[key]; !ok {
			delete(this.cpuLastVal, key)
		}
	}
	return result
}

type kubeletProvider struct {
	nodeLister    *cache.StoreToNodeLister
	reflector     *cache.Reflector
	kubeletClient *KubeletClient
	cpuLastVals   map[string]map[string]cpuVal
}

func (this *kubeletProvider) GetMetricsSources() []MetricsSource {
	sources := []MetricsSource{}
	nodes, err := this.nodeLister.List()
	if err != nil {
		glog.Errorf("error while listing nodes: %v", err)
		return sources
	}

	nodeNames := make(map[string]bool)
	for _, node := range nodes.Items {
		nodeNames[node.Name] = true
		hostname, ip, err := getNodeHostnameAndIP(&node)
		if err != nil {
			glog.Errorf("%v", err)
			continue
		}
		if _, ok := this.cpuLastVals[node.Name]; !ok {
			this.cpuLastVals[node.Name] = make(map[string]cpuVal)
		}
		sources = append(sources, &kubeletMetricsSource{
			host:          Host{IP: ip, Port: this.kubeletClient.GetPort()},
			kubeletClient: this.kubeletClient,
			nodename:      node.Name,
			hostname:      hostname,
			hostId:        node.Spec.ExternalID,
			cpuLastVal:    this.cpuLastVals[node.Name],
		})
	}

	for key := range this.cpuLastVals {
		if _, ok := nodeNames[key]; !ok {
			delete(this.cpuLastVals, key)
		}
	}

	return sources
}

func getNodeHostnameAndIP(node *kube_api.Node) (string, string, error) {
	for _, c := range node.Status.Conditions {
		if c.Type == kube_api.NodeReady && c.Status != kube_api.ConditionTrue {
			return "", "", fmt.Errorf("Node %v is not ready", node.Name)
		}
	}
	hostname, ip := node.Name, ""
	for _, addr := range node.Status.Addresses {
		if addr.Type == kube_api.NodeHostName && addr.Address != "" {
			hostname = addr.Address
		}
		if addr.Type == kube_api.NodeInternalIP && addr.Address != "" {
			ip = addr.Address
		}
	}
	if ip != "" {
		return hostname, ip, nil
	}
	return "", "", fmt.Errorf("Node %v has no valid hostname and/or IP address: %v %v", node.Name, hostname, ip)
}

func NewKubeletProvider(uri *url.URL) (MetricsSourceProvider, error) {
	// create clients
	kubeConfig, kubeletConfig, err := getKubeConfigs(uri)
	if err != nil {
		return nil, err
	}
	kubeClient := kube_client.NewOrDie(kubeConfig)
	kubeletClient, err := NewKubeletClient(kubeletConfig)
	if err != nil {
		return nil, err
	}
	// watch nodes
	lw := cache.NewListWatchFromClient(kubeClient, "nodes", kube_api.NamespaceAll, fields.Everything())
	nodeLister := &cache.StoreToNodeLister{Store: cache.NewStore(cache.MetaNamespaceKeyFunc)}
	reflector := cache.NewReflector(lw, &kube_api.Node{}, nodeLister.Store, time.Hour)
	reflector.Run()

	return &kubeletProvider{
		nodeLister:    nodeLister,
		reflector:     reflector,
		kubeletClient: kubeletClient,
		cpuLastVals:   make(map[string]map[string]cpuVal),
	}, nil
}
