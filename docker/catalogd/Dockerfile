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

# IMPALA-10068: Split out jars for catalog Docker images
# For now, we copy in all Impala dependencies. One way to decrease the catalogd image
# size would be to only copy in jars required by catalogd.
COPY --chown=impala lib /opt/impala/lib

# Run the daemon_entrypoint.sh script as a sanity check. This verifies that
# daemon_entrypoint.sh is detecting Java appropriately and constructing a
# LD_LIBRARY_PATH that allows catalogd to start. The --version flag exits
# before starting the JVM, so that is the limit of this check.
RUN /opt/impala/bin/daemon_entrypoint.sh /opt/impala/bin/catalogd --version

# Externally-facing ports
# Debug webserver
EXPOSE 25020

ENTRYPOINT ["/opt/impala/bin/daemon_entrypoint.sh", "/opt/impala/bin/catalogd",\
     "-log_dir=/opt/impala/logs",\
     "-abort_on_config_error=false", "-state_store_host=statestored",\
     "-catalog_topic_mode=minimal", "-hms_event_polling_interval_s=1",\
     "-invalidate_tables_on_memory_pressure=true",\
     "-use_resolved_hostname=true"]
