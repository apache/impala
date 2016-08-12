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
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/set-classpath.sh

SENTRY_SERVICE_CONFIG=${SENTRY_CONF_DIR}/sentry-site.xml

# First kill any running instances of the service.
$IMPALA_HOME/testdata/bin/kill-sentry-service.sh

# Sentry picks up JARs from the HADOOP_CLASSPATH and not the CLASSPATH.
export HADOOP_CLASSPATH=${POSTGRES_JDBC_DRIVER}
# Start the service.
${SENTRY_HOME}/bin/sentry --command service -c ${SENTRY_SERVICE_CONFIG} &

# Wait for the service to come online
"$JAVA" -cp $CLASSPATH org.apache.impala.testutil.SentryServicePinger \
    --config_file "${SENTRY_SERVICE_CONFIG}" -n 30 -s 2
