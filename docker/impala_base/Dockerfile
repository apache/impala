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
ARG BASE_IMAGE=ubuntu:16.04
FROM ${BASE_IMAGE}

# Install minimal dependencies required for Impala services to run.
RUN apt-get update && \
  apt-get install -y openjdk-8-jre-headless \
  libsasl2-2 libsasl2-modules libsasl2-modules-gssapi-mit \
  sudo netcat-openbsd less curl iproute2 vim iputils-ping \
  tzdata krb5-user && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Use a non-privileged impala user to run the daemons in the container.
# That user should own everything in the /opt/impala subdirectory.
RUN groupadd -r impala -g 1000 && useradd --no-log-init -r -u 1000 -g 1000 impala && \
    mkdir -p /opt/impala && chown impala /opt/impala && \
    chmod ugo+w /etc/passwd
USER impala

# Copy build artifacts required for the daemon processes. The daemon images are
# responsible for copying in the required jar files, as each daemon has a different
# set of jar dependencies.
# Need to have multiple copy commands to preserve directory structure.
COPY --chown=impala www /opt/impala/www
COPY --chown=impala bin /opt/impala/bin
# Symlink here instead of in setup_build_context to avoid duplicate binaries.
RUN cd /opt/impala/bin && ln -s impalad statestored && ln -s impalad catalogd && \
    ln -s impalad admissiond && \
# Create conf directory for later config injection.
    mkdir /opt/impala/conf && \
# Create logs directory to collect container logs.
    mkdir /opt/impala/logs && \
# Create ranger cache directory that is used when ranger is enabled.
    mkdir /opt/impala/rangercache

WORKDIR /opt/impala/
