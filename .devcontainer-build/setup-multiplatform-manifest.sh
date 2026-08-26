#!/usr/bin/env bash
#
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

# Creates or updates a multi-platform manifest with the corresponding x86 and ARM images.
# For example, use this script to create or update the "latest" manifest with the
# "latest-x86" and "latest-arm" images.
#
# Inputs (via environment variables):
#   OCI_IMG:     Base name of the OCI image repo used for all images and manifests.
#   OCI_TAG:     Tag name for the new multi-platform manifest.
#   OCI_X86_TAG: Source tag for the x86 image.
#   OCI_ARM_TAG: Source tag for the ARM image.
#   NON_INTERACTIVE: If set to 1, the script will not prompt for user confirmation before
#                    building the image. Default: "0".

set -euo pipefail

OCI_IMG="${OCI_IMG:-apache/impala-dev}"
OCI_TAG="${OCI_TAG:-latest}"
OCI_X86_TAG="${OCI_X86_TAG:-${OCI_TAG}-x86}"
OCI_ARM_TAG="${OCI_ARM_TAG:-${OCI_TAG}-arm}"
NON_INTERACTIVE="${NON_INTERACTIVE:-0}"

echo "[INFO] Setup multiplatform manifest:"
echo "         Image:           ${OCI_IMG}"
echo "         Source x86 Tag:  ${OCI_X86_TAG}"
echo "         Source ARM Tag:  ${OCI_ARM_TAG}"
echo "         Destination Tag: ${OCI_TAG}"
echo
echo "============================================================================"
echo

if [[ "${NON_INTERACTIVE}" != "1" ]]; then
  read -r -p "Press Return to continue, or Ctrl+C to exit... " _
fi

docker manifest rm "${OCI_IMG}:${OCI_TAG}" 2>/dev/null || true

ARM_SHA=$(docker manifest inspect "${OCI_IMG}:${OCI_ARM_TAG}" \
    | jq -r '.manifests[] | select(.platform.architecture == "arm64") | .digest')
X86_SHA=$(docker manifest inspect "${OCI_IMG}:${OCI_X86_TAG}" \
    | jq -r '.manifests[] | select(.platform.architecture == "amd64") | .digest')

docker manifest create \
    "${OCI_IMG}:${OCI_TAG}" \
    "${OCI_IMG}@${X86_SHA}" \
    "${OCI_IMG}@${ARM_SHA}"

docker manifest push "${OCI_IMG}:${OCI_TAG}"
