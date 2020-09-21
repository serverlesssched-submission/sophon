#!/usr/bin/env bash

set -x

for image in whisk/invoker whisk/controller whisk/user-events whisk/cache-invalidator-cosmosdb whisk/standalone; 
do
	docker tag ${image}:latest `cat controller`:5000/${image}
	docker push `cat controller`:5000/${image}
done

for image in whisk/invoker whisk/controller whisk/user-events whisk/cache-invalidator-cosmosdb whisk/standalone;
do	
	for ip in `cat invokers`
	do
		ssh -i ~/pem -o StrictHostKeyChecking=no ubuntu@$ip "sudo docker pull `cat controller`:5000/${image}"
	done
done

