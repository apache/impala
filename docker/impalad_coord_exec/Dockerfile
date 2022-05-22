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

COPY --chown=impala lib /opt/impala/lib

# Run the daemon_entrypoint.sh script as a sanity check. This verifies that
# daemon_entrypoint.sh is detecting Java appropriately and constructing a
# LD_LIBRARY_PATH that allows impalad to start. The --version flag exits
# before starting the JVM, so that is the limit of this check.
RUN /opt/impala/bin/daemon_entrypoint.sh /opt/impala/bin/impalad --version

# Externally-facing ports
# Beeswax
EXPOSE 21000
# HS2
EXPOSE 21050
# HS2 over HTTP
EXPOSE 28000
# Debug webserver
EXPOSE 25000

ENTRYPOINT ["/opt/impala/bin/daemon_entrypoint.sh", "/opt/impala/bin/impalad",\
     "-log_dir=/opt/impala/logs",\
     "-abort_on_config_error=false", "-state_store_host=statestored",\
     "-catalog_service_host=catalogd", "-mem_limit_includes_jvm=true",\
     "-use_local_catalog=true", "--rpc_use_loopback=true",\
     "-use_resolved_hostname=true"]
