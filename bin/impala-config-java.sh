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

IMPALA_JDK_VERSION=${IMPALA_JDK_VERSION:-system}

# Set OS Java package variables for bootstrap_system and Docker builds
if [[ "${IMPALA_JDK_VERSION}" == "system" || "${IMPALA_JDK_VERSION}" == "8" ]]; then
  UBUNTU_JAVA_VERSION=8
  REDHAT_JAVA_VERSION=1.8.0
else
  UBUNTU_JAVA_VERSION="${IMPALA_JDK_VERSION}"
  REDHAT_JAVA_VERSION="${IMPALA_JDK_VERSION}"
fi

if [[ "$(uname -p)" == 'aarch64' ]]; then
  UBUNTU_PACKAGE_ARCH='arm64'
else
  UBUNTU_PACKAGE_ARCH='amd64'
fi
