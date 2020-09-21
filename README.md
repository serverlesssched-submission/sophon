# Sophon Serverless Scheduler

## Simulator
`cd simulator-workload-gen`

Download Azure Function Traces: https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md

Set env AZURE\_TRACE\_DIR to the location of unzipped traces.

Run: `./run.py < run.json`

Results will be in generated pdf.

## Setup Kubernetes
Deploy a Kubernetes cluster with Calico networks (ideally >= 10 nodes). Modify file `./invokers` to include IP of each worker node, one per line. Modify file `./controller` to include the IP of the master node.

Modify `openwhisk-deploy-kube/template.yaml`: Replace values of invoker.replicaCount and invoker.containerFactory.kubernetes.replicaCount with the number of nodes in your Kubernetes.

Complete "Initial setup" in https://github.com/apache/openwhisk-deploy-kube#initial-setup.

Set up a Docker registry on controler node: https://docs.docker.com/registry/deploying/ 

## Test OpenWhisk
`cd openwhisk`

`./gradlew clean && ./gradlew distDocker -x test`

cd to repo root dir.

`./distribute.sh && ./auto_test.sh vanilla`

Run logs will be in auto\_test/vanilla

## Test Sophon
`cd sophon`

`./gradlew clean && ./gradlew distDocker -x test`

cd to repo root dir.

`./distribute.sh && ./auto_test.sh sophon`

Run logs will be in auto\_test/sophon

