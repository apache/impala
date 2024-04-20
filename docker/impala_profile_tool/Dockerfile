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

# If set to "--install-debug-tools full", then extra utilities will be installed.
ARG INSTALL_OS_PACKAGES_ARGS="--install-debug-tools full"

# Install dependencies required for Impala utility binaries to run, plus
# some useful utilities.
# TODO: ideally we wouldn't depend on the JVM libraries, but currently the JNI code
# in be/ is not cleanly separated from the code that doesn't use JNI.
ADD helper/install_os_packages.sh /root
RUN /root/install_os_packages.sh ${INSTALL_OS_PACKAGES_ARGS}

# Use a non-privileged impala user to run the processes in the container.
# That user should own everything in the /opt/impala subdirectory.
RUN groupadd -r impala -g 1000 && useradd --no-log-init -r -u 1000 -g 1000 impala && \
    mkdir -p /opt/impala && chown impala /opt/impala && \
    chmod ugo+w /etc/passwd
USER impala

# Copy build artifacts required for the utilities.
# Need to have multiple copy commands to preserve directory structure.
COPY --chown=impala bin /opt/impala/bin
COPY --chown=impala lib /opt/impala/lib

WORKDIR /opt/impala/

ENTRYPOINT ["/opt/impala/bin/utility_entrypoint.sh", "/opt/impala/bin/impala-profile-tool",\
     "-logtostderr"]

LABEL name="Apache Impala Profile Tool" \
      description="Tool for working with Impala profiles." \
      # Common labels.
      org.label-schema.maintainer=$MAINTAINER \
      org.label-schema.url=$URL \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-type=$VCS_TYPE \
      org.label-schema.vcs-url=$VCS_URL \
      org.label-schema.version=$VERSION
