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

# This uses an invalid BASE_IMAGE, because this needs to be overridden.
ARG BASE_IMAGE=REPLACED_WITH_BASE_IMAGE
FROM ${BASE_IMAGE}

# The level of debugging tools to install is set by the argument to  "--install-debug-tools".
ARG INSTALL_OS_PACKAGES_ARGS="--install-debug-tools none"

# Install minimal dependencies required for Impala services to run.
ADD helper/install_os_packages.sh /root
RUN /root/install_os_packages.sh ${INSTALL_OS_PACKAGES_ARGS}

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

LABEL name="Apache Impala Daemon Base Image" \
      description="Common base image for Apache Impala daemons." \
      # Common labels.
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION
