# Helm Chart: kafka-client-msk

## Deploy Helm Chart

```shell
export NAMESPACE=kafka

# perform dry run
helm install kafka-connect-msk ./kafka-connect-msk \
  --namespace $NAMESPACE --debug --dry-run

# apply chart resources
helm install kafka-connect-msk ./kafka-connect-msk \
  --namespace $NAMESPACE --create-namespace

# optional: upgrade
helm upgrade kafka-connect-msk ./kafka-connect-msk --namespace $NAMESPACE

kubectl get pods -n kafka
kubectl describe pod -n kafka -l app=kafka-connect-msk
```
