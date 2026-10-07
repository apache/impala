#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Runs Apache Spark against the minicluster HMS and HDFS. The official Spark image
# is used unmodified: the Iceberg runtime jar and the configuration are bind
# mounted into it, and everything the image would otherwise bake in is passed as
# environment variables. See testdata/bin/SPARK-README.md.

set -euo pipefail

: "${IMPALA_HOME:?IMPALA_HOME is not set; source bin/impala-config.sh first}"
: "${HADOOP_CONF_DIR:?HADOOP_CONF_DIR is not set; source bin/impala-config.sh first}"

# Spark 4.2 is the newest release, but as of Iceberg 1.12.0 no Spark 4.2 runtime is
# published (3.5, 4.0 and 4.1 are), so default to the newest Spark an Iceberg release
# actually supports. Override with IMPALA_SPARK_VERSION (and
# IMPALA_SPARK_WITH_ICEBERG=false if that Spark has no Iceberg runtime).
SPARK_VERSION=${IMPALA_SPARK_VERSION:-4.1.3}
SCALA_VERSION=${IMPALA_SPARK_SCALA_VERSION:-2.13}
JAVA_VERSION=${IMPALA_SPARK_JAVA_VERSION:-17}
SPARK_IMAGE=${IMPALA_SPARK_IMAGE:-\
spark:${SPARK_VERSION}-scala${SCALA_VERSION}-java${JAVA_VERSION}-ubuntu}
# Deliberately independent of the Iceberg version Impala itself builds against: it
# must be an Apache release published on Maven Central (IMPALA_ICEBERG_VERSION may be
# a vendor build), and GEOMETRY/GEOGRAPHY in the Spark integration needs 1.12.0 or
# newer, which is ahead of what Impala currently builds against.
ICEBERG_VERSION=${IMPALA_SPARK_ICEBERG_VERSION:-1.12.0}
WITH_ICEBERG=${IMPALA_SPARK_WITH_ICEBERG:-true}

CONTAINER=impala-minicluster-spark
# The Spark Thrift Server gives the container a long-lived service to connect to,
# reusing one SparkSession instead of paying JVM startup per statement.
THRIFT_PORT=9092
SPARK_DIR=${IMPALA_HOME}/testdata/bin/minicluster_spark
# Staged (gitignored) mount sources, assembled below.
CONF_DIR=${SPARK_DIR}/conf
JARS_DIR=${SPARK_DIR}/jars

# Assemble the directory that is mounted as the Spark conf directory. The image has
# no conf directory of its own, so this provides all of it.
mkdir -p "${CONF_DIR}"
pushd "${HADOOP_CONF_DIR}" > /dev/null
cp hive-site.xml core-site.xml hdfs-site.xml "${CONF_DIR}"
popd > /dev/null
cp "${SPARK_DIR}/spark-defaults.conf" "${CONF_DIR}/spark-defaults.conf"

# Add the Iceberg Spark runtime to the mounts. The jar is not part of the Impala
# toolchain, so fetch it from Maven Central; it is kept across runs so only the
# first run downloads it. The Iceberg catalogs are only configured when the jar is
# really there: spark.sql.extensions naming a missing class makes every session
# fail to start.
JAR_NAME=
if [[ "${WITH_ICEBERG}" == "true" ]]; then
  # Iceberg's Spark runtime is published per Spark minor version, e.g. '4.1'.
  SPARK_MINOR=$(echo "${SPARK_VERSION}" | cut -d. -f1,2)
  ARTIFACT=iceberg-spark-runtime-${SPARK_MINOR}_${SCALA_VERSION}
  JAR_NAME=${ARTIFACT}-${ICEBERG_VERSION}.jar
  JAR_URL=https://repo1.maven.org/maven2/org/apache/iceberg/${ARTIFACT}/\
${ICEBERG_VERSION}/${JAR_NAME}
  mkdir -p "${JARS_DIR}"
  if [[ ! -f "${JARS_DIR}/${JAR_NAME}" ]]; then
    echo "Downloading ${JAR_URL}"
    if ! curl -fSL --retry 3 -o "${JARS_DIR}/${JAR_NAME}.tmp" "${JAR_URL}"; then
      rm -f "${JARS_DIR}/${JAR_NAME}.tmp"
      echo "ERROR: could not download ${JAR_URL}" >&2
      echo "Iceberg ${ICEBERG_VERSION} may not publish a runtime for Spark" \
           "${SPARK_MINOR}. Pick a Spark version it supports with" \
           "IMPALA_SPARK_VERSION, or run Spark without Iceberg support (Hive" \
           "tables only) with IMPALA_SPARK_WITH_ICEBERG=false." >&2
      exit 1
    fi
    mv "${JARS_DIR}/${JAR_NAME}.tmp" "${JARS_DIR}/${JAR_NAME}"
  fi
  cat "${SPARK_DIR}/spark-defaults-iceberg.conf" >> "${CONF_DIR}/spark-defaults.conf"
fi

# Mount the jar as a single file: mounting a directory over /opt/spark/jars would
# hide the jars that ship in the image.
JAR_MOUNT=()
if [[ -n "${JAR_NAME}" ]]; then
  JAR_MOUNT=(-v "${JARS_DIR}/${JAR_NAME}:/opt/spark/jars/${JAR_NAME}:ro")
fi

# HADOOP_USER_NAME lets Spark write to the minicluster HDFS as the developer.
# The image has no conf directory and /opt/spark is not writable by its 'spark'
# user, so point the conf lookup and the daemon scripts at usable locations.
# SPARK_IDENT_STRING only affects log file names; the image leaves $USER unset,
# which would otherwise produce 'spark--org.apache...' names. SPARK_NO_DAEMONIZE
# keeps the Thrift Server in the foreground so it is the container's main process.
docker run --detach --network=host --name "${CONTAINER}" \
    -v "${CONF_DIR}:/opt/spark/conf:ro" \
    "${JAR_MOUNT[@]}" \
    -e HADOOP_USER_NAME="$USER" \
    -e HOME=/tmp \
    -e SPARK_CONF_DIR=/opt/spark/conf \
    -e HADOOP_CONF_DIR=/opt/spark/conf \
    -e SPARK_LOG_DIR=/tmp/spark-logs \
    -e SPARK_PID_DIR=/tmp \
    -e SPARK_IDENT_STRING="${CONTAINER}" \
    -e SPARK_NO_DAEMONIZE=true \
    "${SPARK_IMAGE}" \
    /opt/spark/sbin/start-thriftserver.sh \
        --hiveconf hive.server2.thrift.bind.host=0.0.0.0 \
        --hiveconf "hive.server2.thrift.port=${THRIFT_PORT}" \
        --hiveconf hive.server2.transport.mode=binary
