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

. ${IMPALA_HOME}/bin/set-classpath.sh

SENTRY_SERVICE_CONFIG=${SENTRY_SERVICE_CONFIG:-}
SENTRY_LOG_DIR=${SENTRY_LOG_DIR:-}

if [ -z ${SENTRY_SERVICE_CONFIG} ]
then
  SENTRY_SERVICE_CONFIG=${SENTRY_CONF_DIR}/sentry-site.xml
fi

if [ -z ${SENTRY_LOG_DIR} ]
then
  LOGDIR="${IMPALA_CLUSTER_LOGS_DIR}"/sentry
else
  LOGDIR=${SENTRY_LOG_DIR}
fi

mkdir -p "${LOGDIR}" || true

# First kill any running instances of the service.
$IMPALA_HOME/testdata/bin/kill-sentry-service.sh

export HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=30020"

# Sentry picks up JARs from the HADOOP_CLASSPATH and not the CLASSPATH.
export HADOOP_CLASSPATH=${POSTGRES_JDBC_DRIVER}:$IMPALA_HOME/fe/target/test-classes
# Start the service.
${SENTRY_HOME}/bin/sentry --command service -c ${SENTRY_SERVICE_CONFIG} > "${LOGDIR}"/sentry.out 2>&1 &

# Wait for the service to come online
"$JAVA" -cp $CLASSPATH org.apache.impala.testutil.SentryServicePinger \
    --config_file "${SENTRY_SERVICE_CONFIG}" -n 30 -s 2
