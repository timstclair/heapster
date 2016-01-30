// Copyright 2014 Google Inc. All Rights Reserved.
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

// This file implements a cadvisor datasource, that collects metrics from an instance
// of cadvisor runing on a specific host.

package kubelet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	cadvisor "github.com/google/cadvisor/info/v1"
	kube_client "k8s.io/kubernetes/pkg/client/unversioned"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
)

type Host struct {
	IP       string
	Port     int
	Resource string
}

type KubeletClient struct {
	config *kube_client.KubeletConfig
	client *http.Client
}

const (
	pathSummary    = "/stats/summary/"
	pathContainers = "/stats/container/"
)

// HasSummaryEndpoint returns whether the Host kubelet supports the /stats/summary endpoint.
func (self *KubeletClient) HasSummaryEndpoint(host Host) (bool, error) {
	url := self.getRequestUrl(host, pathSummary)
	resp, err := self.client.Head(url.String())
	if err != nil {
		return false, err
	}
	resp.Body.Close()
	return resp.StatusCode != http.StatusNotFound, nil
}

func sampleContainerStats(stats []*cadvisor.ContainerStats) []*cadvisor.ContainerStats {
	if len(stats) == 0 {
		return []*cadvisor.ContainerStats{}
	}
	return []*cadvisor.ContainerStats{stats[len(stats)-1]}
}

func (self *KubeletClient) postRequestAndGetValue(client *http.Client, req *http.Request, value interface{}) error {
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body - %v", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed - %q, response: %q", response.Status, string(body))
	}
	err = json.Unmarshal(body, value)
	if err != nil {
		return fmt.Errorf("failed to parse output. Response: %q. Error: %v", string(body), err)
	}
	return nil
}

func (self *KubeletClient) parseStat(containerInfo *cadvisor.ContainerInfo) *cadvisor.ContainerInfo {
	containerInfo.Stats = sampleContainerStats(containerInfo.Stats)
	if len(containerInfo.Aliases) > 0 {
		containerInfo.Name = containerInfo.Aliases[0]
	}
	return containerInfo
}

// TODO(vmarmol): Use Kubernetes' if we export it as an API.
type statsRequest struct {
	// The name of the container for which to request stats.
	// Default: /
	ContainerName string `json:"containerName,omitempty"`

	// Max number of stats to return.
	// If start and end time are specified this limit is ignored.
	// Default: 60
	NumStats int `json:"num_stats,omitempty"`

	// Start time for which to query information.
	// If ommitted, the beginning of time is assumed.
	Start time.Time `json:"start,omitempty"`

	// End time for which to query information.
	// If ommitted, current time is assumed.
	End time.Time `json:"end,omitempty"`

	// Whether to also include information from subcontainers.
	// Default: false.
	Subcontainers bool `json:"subcontainers,omitempty"`
}

func (self *KubeletClient) getRequestUrl(host Host, path string) url.URL {
	scheme := "http"
	if self.config != nil && self.config.EnableHttps {
		scheme = "https"
	}

	return url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("%s:%d", host.IP, host.Port),
		Path:   path,
	}
}

// Get complete node stats summary.
func (self *KubeletClient) GetSummary(host Host) (*stats.Summary, error) {
	url := self.getRequestUrl(host, pathSummary)
	req, err := http.NewRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	summary := stats.Summary{}
	err = self.postRequestAndGetValue(self.client, req, summary)
	return &summary, err
}

// Get stats for all non-Kubernetes containers.
func (self *KubeletClient) GetAllRawContainers(host Host, start, end time.Time) ([]cadvisor.ContainerInfo, error) {
	url := self.getRequestUrl(host, pathContainers)
	return self.getAllContainers(url, start, end)
}

func (self *KubeletClient) GetPort() int {
	return int(self.config.Port)
}

func (self *KubeletClient) getAllContainers(url url.URL, start, end time.Time) ([]cadvisor.ContainerInfo, error) {
	// Request data from all subcontainers.
	request := statsRequest{
		ContainerName: "/",
		NumStats:      1,
		Start:         start,
		End:           end,
		Subcontainers: true,
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url.String(), bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	var containers map[string]cadvisor.ContainerInfo
	client := self.client
	err = self.postRequestAndGetValue(client, req, &containers)
	if err != nil {
		return nil, fmt.Errorf("failed to get all container stats from Kubelet URL %q: %v", url.String(), err)
	}

	result := make([]cadvisor.ContainerInfo, 0, len(containers))
	for _, containerInfo := range containers {
		cont := self.parseStat(&containerInfo)
		if cont != nil {
			result = append(result, *cont)
		}
	}
	return result, nil
}

func NewKubeletClient(kubeletConfig *kube_client.KubeletConfig) (*KubeletClient, error) {
	transport, err := kube_client.MakeTransport(kubeletConfig)
	if err != nil {
		return nil, err
	}
	c := &http.Client{
		Transport: transport,
		Timeout:   kubeletConfig.HTTPTimeout,
	}
	return &KubeletClient{
		config: kubeletConfig,
		client: c,
	}, nil
}
