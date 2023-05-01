helm install postgresql-dev -f values.yaml bitnami/postgresql -n postgresql --set volumePermissions.enabled=true
kubectl edit service/postgresql-dev -n postgresql  AND CHANGE type to LoadBalancre
