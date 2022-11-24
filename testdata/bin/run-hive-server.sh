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

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

HIVE_SERVER_PORT=10000
export HIVE_SERVER2_THRIFT_PORT=11050
HIVE_METASTORE_PORT=9083
LOGDIR=${IMPALA_CLUSTER_LOGS_DIR}/hive
HIVES2_TRANSPORT="plain_sasl"
METASTORE_TRANSPORT="buffered"
START_METASTORE=1
START_HIVESERVER=1
ENABLE_RANGER_AUTH=0

CLUSTER_BIN=${IMPALA_HOME}/testdata/bin

if ${CLUSTER_DIR}/admin is_kerberized; then
    # Making a kerberized cluster... set some more environment variables.
    . ${MINIKDC_ENV}

    HIVES2_TRANSPORT="kerberos"
    # The metastore isn't kerberized yet:
    # METASTORE_TRANSPORT="kerberos"
fi

mkdir -p ${LOGDIR}

while [ -n "$*" ]
do
  case $1 in
    -only_metastore)
      START_HIVESERVER=0
      ;;
    -with_ranger)
      ENABLE_RANGER_AUTH=1
      echo "Starting Hive with Ranger authorization."
      ;;
    -only_hiveserver)
      START_METASTORE=0
      ;;
    -help|-h|*)
      echo "run-hive-server.sh : Starts the hive server and the metastore."
      echo "[-only_metastore] : Only starts the hive metastore."
      echo "[-only_hiveserver] : Only starts the hive server."
      echo "[-with_ranger] : Starts with Ranger authorization (only for Hive 3)."
      exit 1;
      ;;
    esac
  shift;
done

if [[ $START_METASTORE -eq 0 && $START_HIVESERVER -eq 0 ]]; then
  echo "Skipping metastore and hiveserver. Nothing to do"
  exit 1;
fi

# TODO: We should have a retry loop for every service we start.
# Kill for a clean start.
if [[ $START_HIVESERVER -eq 1 ]]; then
  ${CLUSTER_BIN}/kill-hive-server.sh -only_hiveserver &> /dev/null
fi

if [[ $START_METASTORE -eq 1 ]]; then
  ${CLUSTER_BIN}/kill-hive-server.sh -only_metastore &> /dev/null
fi

export HIVE_METASTORE_HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,\
suspend=n,address=30010"

# Add Ranger dependencies if we are starting with Ranger authorization enabled.
if [[ $ENABLE_RANGER_AUTH -eq 1 ]]; then
  export HIVE_CONF_DIR="$HADOOP_CONF_DIR/hive-site-ranger-auth/"
  for f in "$RANGER_HOME"/ews/webapp/WEB-INF/classes/ranger-plugins/hive/ranger-*.jar \
      "$RANGER_HOME"/ews/webapp/WEB-INF/lib/ranger-*.jar \
      "$RANGER_HOME"/ews/lib/ranger-*.jar; do
    FILE_NAME=$(basename $f)
    export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${f}
  done
  # The following jar is needed by RangerRESTUtils.java.
  export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:\
      ${RANGER_HOME}/ews/webapp/WEB-INF/lib/gethostname4j-*.jar"
fi

# For Hive 3, we use Tez for execution. We have to add it to the classpath.
# NOTE: it would seem like this would only be necessary on the HS2 classpath,
# but compactions are initiated from the HMS in Hive 3. This may change at
# some point in the future, in which case we can add this to only the
# HS2 classpath.
export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${TEZ_HOME}/*
# This is a little hacky, but Tez bundles a bunch of junk into lib/, such
# as extra copies of the hadoop libraries, etc, and we want to avoid conflicts.
# So, we'll be a bit choosy about what we add to the classpath here.
for jar in $TEZ_HOME/lib/* ; do
  case $(basename $jar) in
    commons-*|RoaringBitmap*)
      export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$jar
      ;;
  esac
done

# Add kudu-hive.jar to the Hive Metastore classpath, so that Kudu's HMS
# plugin can be loaded.
for file in ${IMPALA_KUDU_JAVA_HOME}/*kudu-hive*jar; do
  export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${file}
done
# Default to skip validation on Kudu tables if KUDU_SKIP_HMS_PLUGIN_VALIDATION
# is unset.
export KUDU_SKIP_HMS_PLUGIN_VALIDATION=${KUDU_SKIP_HMS_PLUGIN_VALIDATION:-1}

# Starts a Hive Metastore Server on the specified port.
# To debug log4j2 loading issues, add to HADOOP_CLIENT_OPTS:
#   -Dorg.apache.logging.log4j.simplelog.StatusLogger.level=TRACE
if [ ${START_METASTORE} -eq 1 ]; then
  HADOOP_CLIENT_OPTS="-Xmx2024m -Dhive.log.file=hive-metastore.log" hive \
      --service metastore -p $HIVE_METASTORE_PORT >> ${LOGDIR}/hive-metastore.out 2>&1 &

  # Wait for the Metastore to come up because HiveServer2 relies on it being live.
  ${CLUSTER_BIN}/wait-for-metastore.py --transport=${METASTORE_TRANSPORT}
fi

# Include the latest libfesupport.so in the JAVA_LIBRARY_PATH
export JAVA_LIBRARY_PATH="${JAVA_LIBRARY_PATH-}:${IMPALA_HOME}/be/build/latest/service/"

# Add the toolchain's libstdc++ to the LD_LIBRARY_PATH, because libfesupport.so may
# need the newer version.
GCC_HOME="${IMPALA_TOOLCHAIN_PACKAGES_HOME}/gcc-${IMPALA_GCC_VERSION}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH-}:${GCC_HOME}/lib64"

export HIVESERVER2_HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,\
suspend=n,address=30020"
if [ ${START_HIVESERVER} -eq 1 ]; then
  # Starts a HiveServer2 instance on the port specified by the HIVE_SERVER2_THRIFT_PORT
  # environment variable. HADOOP_HEAPSIZE should be set to at least 2048 to avoid OOM
  # when loading ORC tables like widerow.
  HADOOP_CLIENT_OPTS="-Xmx2048m -Dhive.log.file=hive-server2.log" hive \
      --service hiveserver2 >> ${LOGDIR}/hive-server2.out 2>&1 &

  # Wait for the HiveServer2 service to come up because callers of this script
  # may rely on it being available.
  ${CLUSTER_BIN}/wait-for-hiveserver2.py --transport=${HIVES2_TRANSPORT}
fi
