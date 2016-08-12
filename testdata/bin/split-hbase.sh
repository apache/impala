#!/usr/bin/env bash
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

. ${IMPALA_HOME}/bin/impala-config.sh

if ${CLUSTER_DIR}/admin is_kerberized; then
  KERB_ARGS="--use_kerberos"
else
  KERB_ARGS=
fi

# Split hbasealltypesagg and hbasealltypessmall and assign their splits
cd $IMPALA_HOME/testdata
${IMPALA_HOME}/bin/mvn-quiet.sh clean
${IMPALA_HOME}/bin/mvn-quiet.sh package
mvn -q dependency:copy-dependencies

. ${IMPALA_HOME}/bin/set-classpath.sh
export CLASSPATH=$IMPALA_HOME/testdata/target/impala-testdata-0.1-SNAPSHOT.jar:$CLASSPATH

: ${JAVA_KERBEROS_MAGIC=}
for ATTEMPT in {1..10}; do
  if "$JAVA" ${JAVA_KERBEROS_MAGIC} \
      org.apache.impala.datagenerator.HBaseTestDataRegionAssigment \
      functional_hbase.alltypesagg functional_hbase.alltypessmall; then
    break
  fi
  # Hopefully reloading the data will somehow help the splitting succeed.
  $IMPALA_HOME/bin/start-impala-cluster.py
  $IMPALA_HOME/bin/load-data.py -w functional-query \
      --table_names=alltypesagg,alltypessmall --table_formats=hbase/none --force \
      ${KERB_ARGS} --principal=${MINIKDC_PRINC_HIVE}
  $IMPALA_HOME/tests/util/compute_table_stats.py --db_names=functional_hbase \
      --table_names=alltypesagg,alltypessmall ${KERB_ARGS}
done
