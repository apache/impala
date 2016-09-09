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

CONF_DIR=$1
SCRIPT=$2
WH=$3
MS_URL=$4
MS_DRIVER=$5
MS_USERNAME=$6
MS_PASSWORD=$7

HIVE="hive "
HIVE="$HIVE -hiveconf \"test.hive.warehouse.dir=$WH\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.url=$MS_URL\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.driver=$MS_DRIVER\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.username=$MS_USERNAME\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.password=$MS_PASSWORD\""

HIVE_CONF_DIR=$CONF_DIR
export HIVE_CONF_DIR
$HIVE -f $SCRIPT -v
