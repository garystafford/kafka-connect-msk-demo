# purpose: Kafka Connect for Amazon MSK
# author: Gary A. Stafford
# date: 2021-10-28

FROM openjdk:17-alpine3.14

### VARIABLES ###
ARG BBROKERS
ENV BBROKERS=$BBROKERS

ENV KAFKA_PLUGINS="/usr/local/share/kafka/plugins"

ENV KAFKA_VERSION="2.8.1"
ENV KAFKA_PACKAGE="kafka_2.13-2.8.1"
ENV AWS_MSK_IAM_AUTH="1.1.1"
ENV CONNECT_JDBC="10.2.5"
ENV CONNECT_S3="10.0.3"
ENV APICURIO="2.1.1.Final"
ENV CONFLUENT_KAFKA="6.2.1"
ENV CONNECT_DEBEZIUM="1.7.0.Final"
#################

RUN apk add --no-cache curl wget tar bash jq vim

# setup java truststore
RUN cp $JAVA_HOME/lib/security/cacerts /tmp/kafka.client.truststore.jks

# install kafka
RUN wget https://downloads.apache.org/kafka/$KAFKA_VERSION/$KAFKA_PACKAGE.tgz \
    && tar -xzf $KAFKA_PACKAGE.tgz \
    && rm -rf $KAFKA_PACKAGE.tgz

# setup kafka plugins directory
RUN mkdir -p $KAFKA_PLUGINS

## fix 'Failed to load class "org.slf4j.impl.StaticLoggerBinder"' error
#RUN rm libs/slf4j-log4j12-1.7.30.jar \
#    && wget https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/1.7.32/slf4j-simple-1.7.32.jar \
#    && mv slf4j-simple-1.7.32.jar /$KAFKA_PACKAGE/libs/

# install aws-msk-iam-auth jar for kafka and kafka connect CLIs
# https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.1/aws-msk-iam-auth-1.1.1-all.jar
RUN wget https://github.com/aws/aws-msk-iam-auth/releases/download/v$AWS_MSK_IAM_AUTH/aws-msk-iam-auth-$AWS_MSK_IAM_AUTH-all.jar \
    && cp aws-msk-iam-auth-$AWS_MSK_IAM_AUTH-all.jar $KAFKA_PLUGINS/ \
    && mv aws-msk-iam-auth-$AWS_MSK_IAM_AUTH-all.jar /$KAFKA_PACKAGE/libs/

# https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc
RUN wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-jdbc/versions/$CONNECT_JDBC/confluentinc-kafka-connect-jdbc-$CONNECT_JDBC.zip \
    && unzip confluentinc-kafka-connect-jdbc-$CONNECT_JDBC.zip \
    && mv confluentinc-kafka-connect-jdbc-$CONNECT_JDBC $KAFKA_PLUGINS/

# https://www.confluent.io/hub/confluentinc/kafka-connect-s3
RUN wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/$CONNECT_S3/confluentinc-kafka-connect-s3-$CONNECT_S3.zip \
    && unzip confluentinc-kafka-connect-s3-$CONNECT_S3.zip \
    && mv confluentinc-kafka-connect-s3-$CONNECT_S3 $KAFKA_PLUGINS/

RUN rm -rf *.zip

# https://debezium.io/documentation/reference/configuration/avro.html#overview-of-deploying-a-debezium-connector-that-uses-avro-serialization
RUN wget https://repo1.maven.org/maven2/io/apicurio/apicurio-registry-distro-connect-converter/$APICURIO/apicurio-registry-distro-connect-converter-$APICURIO.tar.gz \
    && mkdir apicurio-registry-distro-connect-converter-$APICURIO \
    && tar -xzf apicurio-registry-distro-connect-converter-$APICURIO.tar.gz -C apicurio-registry-distro-connect-converter-$APICURIO \
    && mv apicurio-registry-distro-connect-converter-$APICURIO $KAFKA_PLUGINS/ \
    && rm -rf apicurio-registry-distro-connect-converter-$APICURIO.tar.gz

# https://debezium.io/documentation/reference/1.6/connectors/postgresql.html
RUN wget https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/$CONNECT_DEBEZIUM/debezium-connector-postgres-$CONNECT_DEBEZIUM-plugin.tar.gz \
    && tar -xzf debezium-connector-postgres-$CONNECT_DEBEZIUM-plugin.tar.gz \
    && rm -rf debezium-connector-postgres-$CONNECT_DEBEZIUM-plugin.tar.gz \
    && mv debezium-connector-postgres $KAFKA_PLUGINS/

# install additional connector dependencies for avro format with schema registry
RUN wget https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/$CONFLUENT_KAFKA/kafka-connect-avro-converter-$CONFLUENT_KAFKA.jar \
    && wget https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/$CONFLUENT_KAFKA/kafka-schema-registry-client-$CONFLUENT_KAFKA.jar \
    && wget https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/$CONFLUENT_KAFKA/kafka-avro-serializer-$CONFLUENT_KAFKA.jar \
    && mv *.jar $KAFKA_PLUGINS/

# fixes dependency issues with s3 connector
RUN wget https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar \
    && wget https://repo1.maven.org/maven2/com/google/guava/guava-base/r03/guava-base-r03.jar \
    && wget https://repo1.maven.org/maven2/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar \
    && mv failureaccess-1.0.1.jar $KAFKA_PLUGINS/confluentinc-kafka-connect-s3-$CONNECT_S3/lib/ \
    && mv guava-base-r03.jar $KAFKA_PLUGINS/confluentinc-kafka-connect-s3-$CONNECT_S3/lib/ \
    && mv guava-30.1.1-jre.jar $KAFKA_PLUGINS/confluentinc-kafka-connect-s3-$CONNECT_S3/lib/

WORKDIR /$KAFKA_PACKAGE

## copy client props and connect config file(s)
COPY config/*.properties config/
COPY config/*.json config/

ENTRYPOINT ["/bin/bash", "-l", "-c"]

CMD ["tail -f /dev/null"]
