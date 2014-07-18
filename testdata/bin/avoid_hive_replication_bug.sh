#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# The purpose of this script is to avoid CDH-17414 which causes data files loaded
# with Hive to incorrectly have a replication factor of 1. When using beeline
# this problem only appears to occur immediately after creating the first HBase table
# since starting HiveServer2, i.e., subsequent loads seem to function correctly.
# This script creates an external HBase table in Hive to 'warm up' HiveServer2.
# Subsequent loads should assign a correct replication factor.

set -e

cat > /tmp/create_hive_table.q << EOF
DROP TABLE if exists hive_replication_bug_warmup_table;
create table hive_replication_bug_warmup_table(x int, y string)
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf1:c1")
  TBLPROPERTIES ("hbase.table.name" = "hive_replication_bug_warmup_table");
EOF

cat > /tmp/drop_hive_tables.q << EOF
DROP TABLE if exists hive_replication_bug_warmup_table;
EOF

JDBC_URL="jdbc:hive2://localhost:11050/default;"
if ${CLUSTER_DIR}/admin is_kerberized; then
    # Making a kerberized cluster... set some more environment variables.
    . ${MINIKDC_ENV}
    JDBC_URL="${JDBC_URL}principal=${MINIKDC_PRINC_HIVE}"
fi

beeline -n $USER -u "${JDBC_URL}" -f /tmp/create_hive_table.q
beeline -n $USER -u "${JDBC_URL}" -f /tmp/drop_hive_tables.q

# Clean up temp files.
rm /tmp/create_hive_table.q
rm /tmp/drop_hive_tables.q
