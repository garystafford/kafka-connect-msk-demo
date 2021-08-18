# Helm Chart: kafka-client-msk-v2

## Deploy Helm Chart

```shell
export NAMESPACE=kafka

# perform dry run
helm install kafka-connect-msk-v2 ./kafka-connect-msk-v2 \
  --namespace $NAMESPACE --debug --dry-run

# apply chart resources
helm install kafka-connect-msk-v2 ./kafka-connect-msk-v2 \
  --namespace $NAMESPACE --create-namespace

# optional: upgrade
helm upgrade kafka-connect-msk-v2 ./kafka-connect-msk-v2 --namespace $NAMESPACE

kubectl get pods -n kafka
kubectl describe pod -n kafka -l app=kafka-connect-msk
```
