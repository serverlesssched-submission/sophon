#!/usr/bin/env bash

set -xe

lambda=0.3
distWeight=0.05
evictWeight=`echo 1-$distWeight|bc|awk '{printf "%f", $0}'`
threshold=1.0

bplambda=0.3
bpdistWeight=0.05
bpevictWeight=`echo 1-$bpdistWeight|bc|awk '{printf "%f", $0}'`
bpthreshold=1.0

run=${1}
mkdir -p auto_test
mkdir auto_test/$run

controller = `cat ./controller`
sed "s/SEDcontroller/${controller}/g; s/SEDlambda/${lambda}/g; s/SEDdistanceweight/${distWeight}/g; s/SEDevictionweight/${evictWeight}/g; s/SEDthreshold/${threshold}/g; s/SEDbplambda/${bplambda}/g; s/SEDbpdistanceweight/${bpdistWeight}/g; s/SEDbpevictionweight/${bpevictWeight}/g; s/SEDbpthreshold/${bpthreshold}/g" openwhisk-deploy-kube/template.yaml > openwhisk-deploy-kube/test.yaml

cd openwhisk-deploy-kube
if [ `kubectl -n openwhisk get pods -o wide|wc -l` != 0 ]; then make cleanup; fi
sleep 2
make
while true; do sleep 2; kubectl -n openwhisk get pods -o wide|grep "install-packages.*Completed" && break; done
sleep 10
cd -

cd simulator-workload-gen
./run_real.py < run_real.json > ~/auto_test/${run}/metadata 2>&1
sleep 10
cd -

kubectl -n openwhisk logs owdev-controller-0 | grep -v ealth > ~/auto_test/${run}/merged
for i in {0..9}; do kubectl -n openwhisk logs owdev-invoker-${i} | grep -v ealth >> ~/auto_test/${run}/merged; done

cd openwhisk-deploy-kube
make cleanup
cd -
