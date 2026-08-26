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

# Builds the Impala devcontainer OCI image using the devcontainer CLI and docker buildx.
#
# Inputs (via environment variables):
#   JAVA_VERSION:   The version of Java to install in the devcontainer. Default: "17".
#   OCI_IMG:        The name of the OCI image to build. Default: "apache/impala-dev"
#   OCI_TAG:        The tag of the OCI image to build. The image platform type (either
#                   "x86" or "arm" will be appended to this value). Default: "latest"
#   DOCKER_BUILDER: The name of the docker buildx builder to create/use.
#                   Default: "impala-builder"
#   OUTPUT_TYPE:    The output type for the devcontainer build command. Default: "image"
#   PUSH:           If set to 1, the built image will be pushed to a remote registry,
#                   otherwise the resulting output will not be pushed. Default: "0"
#   PLATFORM:       The target platform for the built OCI image. Allowed values are
#                   "x86_64", "arm64", and "aarch64". Default: host architecture.
#   NON_INTERACTIVE: If set to 1, the script will not prompt for user confirmation before
#                    building the image. Default: "0".

set -euo pipefail

# Script Inputs via Environment Variables.
OCI_IMG="${OCI_IMG:-apache/impala-dev}"
OCI_TAG="${OCI_TAG:-latest}"
DOCKER_BUILDER="${DOCKER_BUILDER:-impala-builder}"
OUTPUT_TYPE="${OUTPUT_TYPE:-image}"
PUSH="${PUSH:-0}"
JAVA_VERSION="${JAVA_VERSION:-17}"
NON_INTERACTIVE="${NON_INTERACTIVE:-0}"

IMPALA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export IMPALA_HOME

export IMPALA_BUILD_THREADS="${IMPALA_BUILD_THREADS:-12}"

if [[ -z "${PLATFORM:-}" ]]; then
  PLATFORM="$(uname -m)"
fi

if [[ "${PLATFORM}" == "x86_64" ]]; then
  PLATFORM="linux/amd64"
  OCI_TAG="${OCI_TAG}-x86"
elif [[ "${PLATFORM}" == "arm64" || "${PLATFORM}" == "aarch64" ]]; then
  PLATFORM="linux/arm64"
  OCI_TAG="${OCI_TAG}-arm"
else
  echo "Unsupported platform '${PLATFORM}'. Allowed values are 'x86_64', 'arm64', and 'aarch64'." >&2
  exit 1
fi

if [[ "${PUSH}" == "1" ]]; then
  PUSH=",push=true"
else
  PUSH=""
fi

# Cleanup function to remove docker buildx builder (if it exists).
rm_docker_builder() {
  docker buildx rm -f "${DOCKER_BUILDER}" || true
}

# Cleanup on exit. Keep the builder when the script fails to aid debugging.
cleanup() {
  local exit_code=$?
  popd || true
  if [[ "${exit_code}" -eq 0 ]]; then
    rm_docker_builder
  else
    echo "[WARN] Script failed (exit code: ${exit_code}); keeping docker builder '${DOCKER_BUILDER}'." >&2
  fi
  return "${exit_code}"
}

# The following commands assume they are run from the parent directory of the directory
# containing this script.
pushd "$(dirname "$(readlink -f "$0")")/.."
trap cleanup EXIT

# Determine git information for labeling the image.
GIT_HASH="$(git rev-parse HEAD)"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
GIT_REMOTE_NAME="$(git config --get "branch.${GIT_BRANCH}.remote")"
GIT_REPO="$(git remote get-url "${GIT_REMOTE_NAME}" | cut -d'/' -f3- | cut -d'@' -f2-)"

# Determine values for Dockerfile args.
IMPALA_CMAKE_VERSION="$(grep 'export IMPALA_CMAKE_VERSION=' "${IMPALA_HOME}/bin/impala-config.sh" | cut -d'=' -f2)"
IMPALA_GCC_VERSION="$(grep 'export IMPALA_GCC_VERSION=' "${IMPALA_HOME}/bin/impala-config.sh" | cut -d'=' -f2)"
IMPALA_TOOLCHAIN_PACKAGES_RELPATH="toolchain/toolchain-packages-gcc${IMPALA_GCC_VERSION}"

echo "[INFO] Building Impala devcontainer:"
echo "         Git Repo:       ${GIT_REPO}"
echo "         Git Branch:     ${GIT_BRANCH}"
echo "         Git Hash:       ${GIT_HASH}"
echo "         CMake Version:  ${IMPALA_CMAKE_VERSION}"
echo "         Toolchain Path: ${IMPALA_TOOLCHAIN_PACKAGES_RELPATH}"
echo "         GCC Version:    ${IMPALA_GCC_VERSION}"
echo "         OCI Image:      ${OCI_IMG}:${OCI_TAG}"
echo "         Platform:       ${PLATFORM}"
echo "         Output Type:    ${OUTPUT_TYPE}"
echo -n "         Push:           "
if [[ -n "${PUSH}" ]]; then
  echo "true"
else
  echo "false"
fi
echo "         Java Version:   ${JAVA_VERSION}"
echo
echo "============================================================================"
echo

if [[ "${NON_INTERACTIVE}" != "1" ]]; then
  read -r -p "Press Return to continue, or Ctrl+C to exit... " _
fi

# Setup docker buildx builder
rm_docker_builder

set -x
docker buildx create \
    --name "${DOCKER_BUILDER}" \
    --driver docker-container \
    --bootstrap \
    --use

GIT_REPO="${GIT_REPO}" \
GIT_BRANCH="${GIT_BRANCH}" \
GIT_HASH="${GIT_HASH}" \
JAVA_VERSION="${JAVA_VERSION}" \
IMPALA_CMAKE_VERSION="${IMPALA_CMAKE_VERSION}" \
IMPALA_GCC_VERSION="${IMPALA_GCC_VERSION}" \
IMPALA_TOOLCHAIN_PACKAGES_RELPATH="${IMPALA_TOOLCHAIN_PACKAGES_RELPATH}" \
devcontainer build \
    --workspace-folder="${IMPALA_HOME}" \
    --config=".devcontainer-build/devcontainer.json" \
    --image-name="${OCI_IMG}:${OCI_TAG}" \
    --platform="${PLATFORM}" \
    --output "type=${OUTPUT_TYPE}${PUSH}"
