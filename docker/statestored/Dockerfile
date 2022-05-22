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

ARG BASE_IMAGE=impala_base
FROM ${BASE_IMAGE}

# Only copy in dependencies required for the statestored.
COPY --chown=impala statestore-lib /opt/impala/lib

# Run the daemon_entrypoint.sh script as a sanity check. This verifies that
# daemon_entrypoint.sh is detecting Java appropriately and constructing a
# LD_LIBRARY_PATH that allows statestored to start.
RUN /opt/impala/bin/daemon_entrypoint.sh /opt/impala/bin/statestored --version

# Externally-facing ports
# Debug webserver
EXPOSE 25010

ENTRYPOINT ["/opt/impala/bin/daemon_entrypoint.sh", "/opt/impala/bin/statestored",\
     "-log_dir=/opt/impala/logs",\
     "-use_resolved_hostname=true"]
