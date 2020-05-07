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

# Build Impala with the a variety of common build configurations and check that the build
# succeeds. Intended for use as a precommit test to make sure nothing got broken.
#
# Assumes that ninja and ccache are installed.
#
# Usage: build-all-flag-combinations.sh [--dryrun]

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

export IMPALA_MAVEN_OPTIONS="-U"

. bin/impala-config.sh

# These are configurations for buildall.
CONFIGS=(
  # Test gcc builds with and without -so:
  "-skiptests -noclean"
  "-skiptests -noclean -release"
  "-skiptests -noclean -release -so -ninja"
  # clang sanitizer builds:
  "-skiptests -noclean -asan"
  "-skiptests -noclean -tsan"
  "-skiptests -noclean -ubsan -so -ninja"
)

FAILED=""

TMP_DIR=$(mktemp -d)
function onexit {
  echo "$0: Cleaning up temporary directory"
  rm -rf ${TMP_DIR}
}
trap onexit EXIT

mkdir -p ${TMP_DIR}

for CONFIG in "${CONFIGS[@]}"; do
  DESCRIPTION="Options $CONFIG"

  if [[ $# == 1 && $1 == "--dryrun" ]]; then
    echo $DESCRIPTION
    continue
  fi

  if ! ./bin/clean.sh; then
    echo "Clean failed"
    exit 1
  fi
  echo "Building with OPTIONS: $DESCRIPTION"
  if ! time -p ./buildall.sh $CONFIG; then
    echo "Build failed: $DESCRIPTION"
    FAILED="${FAILED}:${DESCRIPTION}"
  fi
  ccache -s
  bin/jenkins/get_maven_statistics.sh logs/mvn/mvn.log

  # Keep each maven log from each round of the build
  cp logs/mvn/mvn.log "${TMP_DIR}/mvn.$(date +%s.%N).log"
  # Append the maven log to the accumulated maven log
  cat logs/mvn/mvn.log >> "${TMP_DIR}/mvn_accumulated.log"
done

# Restore the maven logs (these don't interfere with existing mvn.log)
cp ${TMP_DIR}/mvn* logs/mvn

if [[ "$FAILED" != "" ]]
then
  echo "The following builds failed:"
  echo "$FAILED"
  exit 1
fi

# Make a tarball of the .m2 directory
bin/jenkins/archive_m2_directory.sh logs/mvn/mvn_accumulated.log logs/m2_archive.tar.gz

# Note: The exit callback handles cleanup of the temp directory.
