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
# This script deletes the mysql jdbc, sql files and stops
# the mysqld container.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

# Cleanup the jar
rm -rf /tmp/mysql-connector-j-8*

# Clean up mysql source jar
rm -rf /tmp/mysql-8.2.0.*

# Clean tmp files
rm -f /tmp/mysql_jdbc_alltypes.*
rm -f /tmp/mysql_jdbc.*sql
rm -f /tmp/mysql_jdbc_decimal_tbl.*

EXT_DATA_SOURCES_HDFS_PATH=${FILESYSTEM_PREFIX}/test-warehouse/data-sources
JDBC_DRIVERS_HDFS_PATH=${EXT_DATA_SOURCES_HDFS_PATH}/jdbc-drivers

# Remove jar file of mysql jdbc driver from Hadoop FS.
hadoop fs -rm -f ${JDBC_DRIVERS_HDFS_PATH}/mysql-jdbc.jar
echo "Removed mysql-jdbc.jar from HDFS" ${JDBC_DRIVERS_HDFS_PATH}

# Stop the mysqld docker container
docker stop mysql
