# VERSION 1.10.4
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim-stretch
LABEL maintainer="scoyne2@kent.edu"

##*******************
# Install OpenJDK-8
RUN mkdir /usr/share/man/man1
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;

# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME
##*******************

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.4
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        wget \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[log,crypto,celery,postgres,hive,jdbc,mysql,redis,s3,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install 'redis==3.3.8' \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base
##*******************
# SPARK
RUN cd /usr/ \
    && wget "http://mirrors.koehn.com/apache/spark/spark-2.4.3/spark-2.4.3-bin-hadoop2.7.tgz" \
    && tar xzf spark-2.4.3-bin-hadoop2.7.tgz \
    && rm spark-2.4.3-bin-hadoop2.7.tgz \
    && mv spark-2.4.3-bin-hadoop2.7 spark

ENV SPARK_HOME /usr/spark
RUN export SPARK_HOME
ENV PATH="/usr/spark/bin:${PATH}"
ENV SPARK_MAJOR_VERSION 2
ENV PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.4-src.zip:$SPARK_HOME/python/:$PYTHONPATH

RUN mkdir -p /usr/spark/work/ \
    && chmod -R 777 /usr/spark/work/

ENV SPARK_MASTER_PORT 707

RUN pip install pyspark
##*******************

##*******************
#setup spark yarn settings
COPY conf ${AIRFLOW_USER_HOME}/conf
ENV HADOOP_CONF_DIR ${AIRFLOW_USER_HOME}/conf
ENV YARN_CONF_DIR ${AIRFLOW_USER_HOME}/conf
ENV SPARK_SETTINGS ""
#copy spark jar folder from local
COPY jars ${SPARK_HOME}/jars

RUN mkdir -p /mnt/s3,/mnt1/s3/ \
    && chmod -R 777 /mnt/s3,/mnt1/s3/
ENV HADOOP_USER_NAME hadoop
##*******************

ENV AWS_ACCESS_KEY_ID AKIA4MYUEM6F2LY75OXO
ENV AWS_SECRET_ACCESS_KEY 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
ENV AWS_DEFAULT_REGION us-west-1

##*******************
COPY dags/ ${AIRFLOW_USER_HOME}/dags
COPY python/ ${AIRFLOW_USER_HOME}/python
##*******************
COPY script/entrypoint.sh /entrypoint.sh
COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793

USER airflow
WORKDIR ${AIRFLOW_USER_HOME}
ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"] # set default arg for entrypoint
