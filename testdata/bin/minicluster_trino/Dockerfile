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

# Use an official Trino image as the base. Pin to a specific version rather than
# 'latest' so the interop tests are reproducible and not broken by Trino releases.
FROM trinodb/trino:482

# Use the developer username, so Trino will have write access to HDFS
ARG USERNAME

RUN \
    sed -i '/^http-server\.http\.port=/d' /etc/trino/config.properties && \
    echo "http-server.http.port=9091" >> /etc/trino/config.properties && \
    sed -i 's/localhost:8080/localhost:9091/g' /etc/trino/config.properties && \
    echo "node.id=impala-minicluster-trino" >> /etc/trino/node.properties && \
    echo "-DHADOOP_USER_NAME=$USERNAME" >> /etc/trino/jvm.config

COPY hive-site.xml core-site.xml hdfs-site.xml /etc/
COPY iceberg_lakekeeper.properties iceberg_rest.properties iceberg.properties hive.properties /etc/trino/catalog/

# Apply default session properties server-side via the file session property
# manager. Used to disable the DELTA_LENGTH_BYTE_ARRAY Parquet encoding for the
# Iceberg catalog, which Impala cannot read for string/binary columns yet (Trino
# 482 defaults it on). Trino auto-loads etc/session-property-config.properties.
COPY session-property-config.properties session-property-config.json /etc/trino/

# Expose the Trino port
EXPOSE 9091
