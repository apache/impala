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

# Adds a tag to an existing multi-platform manifest. For example, when releasing a new
# image, use this script to add the "stable-prev" tag to the stable manifest and then use
# this script again to add the "stable" tag to the "latest" manifest.
#
# This script is agnostic about the underlying images and platforms in the source manifest
# and simply copies all existing manifest entries to the new manifest.
#
# Inputs (via environment variables):
#   OCI_IMG:      Base name of the OCI image repo used for all images and manifests.
#   OCI_SRC_TAG:  Source tag of the existing multi-platform manifest to copy.
#   OCI_DEST_TAG: New tag name for the multi-platform manifest.
#   NON_INTERACTIVE: If set to 1, the script will not prompt for user confirmation before
#                    building the image. Default: "0".

set -euo pipefail

OCI_IMG="${OCI_IMG:-apache/impala-dev}"
OCI_SRC_TAG="${OCI_SRC_TAG:-stable}"
OCI_DEST_TAG="${OCI_DEST_TAG:-stable-$(date +"%Y%m%d")}"
NON_INTERACTIVE="${NON_INTERACTIVE:-0}"

echo "[INFO] Tagging multiplatform manifest:"
echo "         Image:           ${OCI_IMG}"
echo "         Source Tag:      ${OCI_SRC_TAG}"

SRC_IMGS=()
for SHA in \
    $(docker manifest inspect "${OCI_IMG}:${OCI_SRC_TAG}" | \
    jq -r ".manifests[].digest"); do
  SRC_IMGS+=("${OCI_IMG}@${SHA}")
  echo "         Source SHA:      ${SHA}"
done

echo "         Destination Tag: ${OCI_DEST_TAG}"
echo
echo "============================================================================"
echo

if [[ "${NON_INTERACTIVE}" != "1" ]]; then
  read -r -p "Press Return to continue, or Ctrl+C to exit... " _
fi

docker manifest rm "${OCI_IMG}:${OCI_DEST_TAG}" 2>/dev/null || true
docker manifest create "${OCI_IMG}:${OCI_DEST_TAG}" "${SRC_IMGS[@]}"
docker manifest push "${OCI_IMG}:${OCI_DEST_TAG}"
