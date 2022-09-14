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

set -euo pipefail

usage() {
  echo "publish_container_to_apache.sh -v <version string> [-r <repo>]"
  echo "  -r: docker repository to upload to (defaults to apache/impala)"
  echo "  -v: version string to tag upload with, e.g. git hash or release version"
}

SCRIPT_DIR=$(cd $(dirname "$0") && pwd)

# Arguments to use for grep when filtering docker-images.txt
IMAGE_GREP_FILTER_ARGS="-v _debug"

VERSION=""

TARGET_REPO="apache/impala"

while getopts "r:v:" flag
do
    case "${flag}" in
        r) TARGET_REPO="$OPTARG"
          ;;
        v) VERSION="$OPTARG"
          ;;
        *)
          usage
          exit 1
    esac
done

if [[ "$VERSION" = "" ]]; then
  echo "-v must be provided"
  usage
  exit 1
fi

# Include the published images, filtering out debug/release as needed.
IMAGES=$(cat "$SCRIPT_DIR/docker-images.txt" | tr ' ' '\n' |\
    grep $IMAGE_GREP_FILTER_ARGS | sort | uniq | tr '\n' ' ')
IMAGES+=" impala_quickstart_client impala_quickstart_hms"

echo "Docker images to publish: $IMAGES"
echo "Version string: '$VERSION'"
read -p "Continue with upload to $TARGET_REPO [y/N]? "
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
  exit 0
fi

for IMAGE in $IMAGES
do
  # Prefix the image with the version so that the set of images can be identified
  # with a prefix, e.g. IMPALA_QUICKSTART_IMAGE_PREFIX in the quickstart docker compose.
  DST="${TARGET_REPO}:${VERSION}-${IMAGE}"
  DIGEST=$(docker images --no-trunc --quiet "${IMAGE}")
  echo "Publishing ${IMAGE} (${DIGEST}) to ${DST}"
  docker tag $IMAGE "$DST"
  docker push "$DST"
done

