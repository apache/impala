#!/bin/bash
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

set -euo pipefail

usage() {
  echo "push_to_registry.sh [options]"
  echo
  echo "Pushes the latest docker images created by a local Impala build to a docker "
  echo "registry, by default Docker Hub. Images must be prefixed with a string, which "
  echo "could be the docker hub username, a branch name, or some other identifier."
  echo
  echo "Supported options are:"
  echo "  -p <image prefix>"
  echo "     Append this image prefix to all images pushed to the registry. Required."
  echo "  -r <registry>"
  echo "     Push to this registry. Optional, if not specified pushes to docker hub."
  echo
  echo "Examples:"
  echo "To push to some user's repository on Docker Hub, with names like "
  echo "experimental-statestored:"
  echo "  push_to_registry.sh -p someuser/experimental"
}

PREFIX=
REGISTRY=
while getopts "p:r:" OPTION
do
  case "$OPTION" in
    p)
      PREFIX="$OPTARG"
      ;;
    r)
      REGISTRY="$OPTARG"
      ;;
    ?)
      echo "Unknown option."
      usage
      exit 1;
      ;;
  esac
done

if [[ -z "$PREFIX" ]]; then
  echo "-p must be specified"
  usage
  exit 1
fi

# The image tags that are updated by the impala build process.
# TODO(IMPALA-8622): get this list from a generated file.
IMAGES=(statestored catalogd impalad_coordinator impalad_executor impalad_coord_exec)

for IMAGE in "${IMAGES[@]}"; do
  if [[ -z "$REGISTRY" ]]; then
    # Docker Hub does not require a prefix.
    DEST=
  else
    DEST=$REGISTRY/
  fi
  DEST+="$PREFIX-$IMAGE"
  docker image tag "$IMAGE" "$DEST"
  docker push "$DEST"
done
