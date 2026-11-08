FROM alpine:3.21 AS jars

RUN apk add --no-cache wget

WORKDIR /jars

RUN wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
 && wget -q https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar \
 && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.2/iceberg-spark-runtime-3.5_2.12-1.7.2.jar \
 && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

FROM apache/spark:3.5.7

USER root

COPY --from=jars /jars/*.jar /opt/spark/jars/
COPY python_spark/ /opt/spark/app/
COPY config/log4j2.properties /opt/spark/conf/log4j2.properties

USER 185