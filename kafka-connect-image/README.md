# Instructions

```shell
# build
export TAG="1.2.0"

# ***** CHANGE ME! *****
#export BBROKERS=$(aws ssm get-parameter --name /msk/iam/brokers --query 'Parameter.Value' --output text)
export BBROKERS="b-1.your-bootstrap-broker.kafka.us-east-1.amazonaws.com:9098,b-2.your-bootstrap-broker.kafka.us-east-1.amazonaws.com:9098,b-3.your-bootstrap-broker.kafka.us-east-1.amazonaws.com:9098"

time docker build \
    --build-arg BBROKERS=$BBROKERS \
    --tag garystafford/kafka-connect-msk:$TAG \
    --no-cache .

# push
docker push garystafford/kafka-connect-msk:$TAG

# test
docker run -it --rm garystafford/kafka-connect-msk:$TAG env
```
