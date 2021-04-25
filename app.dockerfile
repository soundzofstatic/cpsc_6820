FROM python:3.6-alpine3.10

ARG SPARK_VERSION=3.0.2
#ARG HADOOP_VERSION_SHORT=3.2
ARG HADOOP_VERSION_SHORT=2.7
#ARG HADOOP_VERSION=3.2.0
ARG HADOOP_VERSION=2.7.0
ARG AWS_SDK_VERSION=1.11.375

RUN apk add --no-cache bash openjdk8-jre && \
  apk add --no-cache libc6-compat && \
  ln -s /lib/libc.musl-x86_64.so.1 /lib/ld-linux-x86-64.so.2 && \
  pip install findspark

# Download and extract Spark
RUN wget -qO- https://www-eu.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT}.tgz | tar zx -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION_SHORT} /opt/spark

# To read IAM role for Fargate
# RUN echo spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper > /opt/spark/conf/spark-defaults.conf

# Add hadoop-aws and aws-sdk
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P /opt/spark/jars/

ENV PATH="/opt/spark/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"
# Define default command

RUN mkdir $SPARK_HOME/conf
RUN echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh

#Copy python script for batch
#copy app.py /app/app.py

ADD requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Define default command
CMD ["/bin/bash"]
CMD [ "python", "./server.py" ]