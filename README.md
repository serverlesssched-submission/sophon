# Sophon Serverless Scheduler

## Build
Sources of Vanilla OpenWhisk, Vanilla w/ container stub, Sophon, and Sophon w/ container stub are in `vanilla`, `stub-vanilla`, `sophon`, and `stub-sophon`, respectively. 

In the source directory, run `./gradlew distDocker -x test` to build.

## Setup Kubernetes
Deploy a Kubernetes cluster with Calico networks configured (ideally >= 10 nodes). 

Complete "Initial setup" in https://github.com/apache/openwhisk-deploy-kube#initial-setup.

Distribute whisk/\* Docker images to all worker nodes.

Config files are under `openwhisk-deploy-kube`. Config files of Vanilla OpenWhisk, Vanilla w/ container stub, Sophon, and Sophon w/ container stub are in `vanilla`, `stub-vanilla`, `sophon`, and `stub-sophon`, respectively. 

Customize config files and deploy: https://github.com/apache/openwhisk-deploy-kube#customize-the-deployment.

## Run Workload
Download Azure Function Traces: https://github.com/Azure/AzurePublicDataset/blob/master/AzureFunctionsDataset2019.md

Set env AZURE\_TRACE\_DIR to the location of unzipped traces.

Set env APIHOST to OpenWhisk api host.

`cd` to workload-gen.

Run: `./run_real.py run_real.json`. 

You can monitor the system in APIHOST/monitoring/d/ Grafana page.
