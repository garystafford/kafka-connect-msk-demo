```shell
aws cloudformation deploy \
    --stack-name emr-demo-cluster-glue \
    --template-file ./cloudformation/stack-glue.yml \
    --parameter-overrides file://cloudformation/dev-cluster.json \
    --capabilities CAPABILITY_NAMED_IAM

aws cloudformation deploy \
    --stack-name emr-demo-cluster-hive \
    --template-file ./cloudformation/stack-hive.yml \
    --parameter-overrides file://cloudformation/dev-cluster.json \
    --capabilities CAPABILITY_NAMED_IAM

aws cloudformation describe-stack-events \
  --stack-name emr-demo-cluster-glue