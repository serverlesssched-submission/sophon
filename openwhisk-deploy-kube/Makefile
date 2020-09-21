all : install

install : 
	helm install --create-namespace owdev ./helm/openwhisk -n openwhisk -f test.yaml

uninstall :
	helm -n openwhisk uninstall owdev || true

wait5 :
	sleep 5 

wait60 :
	sleep 60 

delete : 
	kubectl delete --all pods --namespace=openwhisk --grace-period=0 --force || true

cleanup : uninstall wait60 delete

restart : cleanup wait5 install

