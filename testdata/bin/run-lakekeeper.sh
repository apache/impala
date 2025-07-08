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

# Check Iceberg version. We need at least Iceberg 1.5
IFS='.-' read -r major minor _ <<< "$IMPALA_ICEBERG_VERSION"
if (( major < 1 )) || { (( major == 1 )) && (( minor < 5 )); }; then
    echo "Iceberg version does NOT meet requirement (need at least 1.5):" \
         "$IMPALA_ICEBERG_VERSION"
    exit
fi

# Copy cluster configs to trino docker directory.
pushd ${HADOOP_CONF_DIR}
cp core-site.xml hdfs-site.xml ${IMPALA_HOME}/testdata/bin/minicluster_lakekeeper
popd

cd ${IMPALA_HOME}/testdata/bin/minicluster_lakekeeper

docker compose up -d
