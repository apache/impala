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

FROM ubuntu:16.04

# Install minimal dependencies required for Impala services to run.
# liblzo2-2 may be needed by the Impala-lzo plugin, which is used in tests.
# We install it in the base image for convenience.
RUN apt-get update && \
  apt-get install -y openjdk-8-jre-headless \
  libsasl2-2 libsasl2-modules libsasl2-modules-gssapi-mit \
  tzdata liblzo2-2

# Copy build artifacts required for the daemon processes.
# Need to have multiple copy commands to preserve directory structure.
COPY bin /opt/impala/bin
COPY lib /opt/impala/lib
COPY www /opt/impala/www
COPY kudu /opt/kudu
# Create conf directory for later config injection.
RUN mkdir /opt/impala/conf
# Create logs directory to collect container logs.
RUN mkdir /opt/impala/logs

WORKDIR /opt/impala/
