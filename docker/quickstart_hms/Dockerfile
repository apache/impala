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

# Build a base HMS image for quickstart purposes.

ARG BASE_IMAGE=ubuntu:16.04
FROM ${BASE_IMAGE}

# Common label arguments.
ARG MAINTAINER
ARG URL
ARG VCS_REF
ARG VCS_TYPE
ARG VCS_URL
ARG VERSION

RUN apt-get update && \
  apt-get install -y openjdk-8-jre-headless \
  sudo netcat-openbsd less curl iproute2 vim iputils-ping \
  tzdata krb5-user && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# Use a non-privileged hive user to run the daemons in the container.
# That user should own everything in the /opt/hive and /var/lib/hive subdirectories
# We use uid/gid 1000 to match the impala user in other containers so that it has
# ownership over any files/directories in docker volumes.
RUN groupadd -r hive -g 1000 && useradd --no-log-init -r -u 1000 -g 1000 hive && \
    mkdir -p /opt/hive && chown hive /opt/hive && \
    mkdir -p /var/lib/hive && chown hive /var/lib/hive && \
    chmod ugo+w /etc/passwd
USER hive

# Copy the Hive install.
WORKDIR /opt/hive
COPY --chown=hive hive /opt/hive
COPY --chown=hive hadoop /opt/hadoop
COPY --chown=hive hms-entrypoint.sh /hms-entrypoint.sh

USER hive

# Add the entrypoint.
ENTRYPOINT ["/hms-entrypoint.sh"]

LABEL name="Apache Impala HMS Quickstart" \
      description="Basic HMS image for Impala quickstart." \
      # Common labels.
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION
