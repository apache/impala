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

# Use an official Trino image as the base
FROM trinodb/trino:latest

# Use the developer username, so Trino will have write access to HDFS
ARG USERNAME

RUN \
    sed -i 's/http-server.http.port=8080/http-server.http.port=9091/' /etc/trino/config.properties && \
    sed -i 's/localhost:8080/localhost:9091/' /etc/trino/config.properties && \
    echo "-DHADOOP_USER_NAME=$USERNAME" >> /etc/trino/jvm.config

COPY hive-site.xml core-site.xml hdfs-site.xml /etc/
COPY iceberg.properties hive.properties /etc/trino/catalog/

# Expose the Trino port
EXPOSE 9091
