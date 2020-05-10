#!/bin/bash
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

# Wrapper that invokes "docker build" with the provided arguments
# and additional, common, build arguments.

set -euo pipefail

ARGS=()

# Add common metadata to docker images.
VCS_REF=$(git rev-parse HEAD || echo "")
VERSION=$(grep 'VERSION' ${IMPALA_HOME}/bin/version.info  | cut -d ' ' -f 2 || echo "")
ARGS+=("--build-arg" 'MAINTAINER=Apache Impala <dev@impala.apache.org>')
ARGS+=("--build-arg" 'URL=https://impala.apache.org')
ARGS+=("--build-arg" 'VCS_TYPE=git')
ARGS+=("--build-arg" 'VCS_URL=https://gitbox.apache.org/repos/asf/impala.git')
ARGS+=("--build-arg" "VERSION=$VERSION")
ARGS+=("--build-arg" "VCS_REF=$VCS_REF")

# Add caller-provided arguments to end.
ARGS+=("$@")

exec docker build "${ARGS[@]}"
