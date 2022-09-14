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

# Build an image that runs a script to load data into the quickstart warehouse.
# The data load script is os-independent, so only build for a fixed OS.
ARG BASE_IMAGE=ubuntu:18.04
FROM ${BASE_IMAGE}

# Common label arguments.
ARG MAINTAINER
ARG URL
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION
ARG PIP=python-pip

# Install useful utilities. Set to non-interactive to avoid issues when installing tzdata.
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
  apt-get install -y \
  sudo netcat-openbsd less curl iproute2 vim iputils-ping \
  libsasl2-dev libsasl2-2 libsasl2-modules libsasl2-modules-gssapi-mit \
  tzdata krb5-user ${PIP} && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Install impala-shell from pip.
# TODO: consider if it would be better to use the latest impala-shell from the build
# environment.
RUN pip install impala-shell

# Use a non-privileged impala user to run the daemons in the container.
# That user should own everything in the /opt/impala and /var/lib/impala subdirectories
RUN groupadd -r impala -g 1000 && useradd --no-log-init -r -u 1000 -g 1000 impala && \
    mkdir -p /opt/impala && chown impala /opt/impala
USER impala

# Copy the client entrypoint and dataload files.
WORKDIR /opt/impala
COPY --chown=impala data-load-entrypoint.sh /data-load-entrypoint.sh
COPY --chown=impala *.sql /opt/impala/sql/

USER impala

# Add the entrypoint.
ENTRYPOINT ["/data-load-entrypoint.sh"]

LABEL name="Apache Impala Quickstart Client" \
      description="Client tools for Impala quickstart, including impala-shell and data loading utilities." \
      # Common labels.
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION
