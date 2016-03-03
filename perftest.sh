#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail
set -x

IMAGE="heapster:perf_test"

# build custom image
cd deploy
docker/build.sh $IMAGE

# push custom image
TMP_FILE=$(mktemp)
REMOTE_FILE="/tmp/$(basename $TMP_FILE)"
docker save -o $TMP_FILE $IMAGE
for NODE in $(kubectl get nodes -o=jsonpath={.items[*].metadata.name}); do
  gcloud compute copy-files $TMP_FILE $NODE:$REMOTE_FILE
  gcloud compute ssh $NODE --command "sudo docker load -i $REMOTE_FILE"
  gcloud compute ssh $NODE --command "sudo rm -f $REMOTE_FILE"
done
rm -f $TMP_FILE

# start testing controllers
kubectl create -f kube-config/standalone-perf/heapster-controller-kubelet.yaml
kubectl create -f kube-config/standalone-perf/heapster-controller-summary.yaml
