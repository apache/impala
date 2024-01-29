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

. bin/impala-config.sh > /dev/null 2>&1

: ${GENERATE_M2_ARCHIVE:=false}

# These are configurations for buildall.
CONFIGS=(
  # Test gcc builds with and without -so:
  "-skiptests -noclean"
  "-skiptests -noclean -release"
  "-notests -noclean -release -package"
  "-skiptests -noclean -release -so -ninja"
  # clang sanitizer builds:
  "-skiptests -noclean -asan"
  "-skiptests -noclean -tsan"
  "-skiptests -noclean -ubsan -so -ninja"
  # USE_APACHE_HIVE=true build:
  "-skiptests -noclean -use_apache_hive"
  "-notests -noclean -use_apache_hive -package"
)

FAILED=""

if [[ "$GENERATE_M2_ARCHIVE" == true ]]; then
  # The m2 archive relies on parsing the maven log to get a list of jars downloaded
  # from particular repositories. To accurately produce the archive every time, we
  # need to clear out the ~/.m2 directory before producing the archive.
  rm -rf ~/.m2
fi

TMP_DIR=$(mktemp -d)
function onexit {
  echo "$0: Cleaning up temporary directory"
  rm -rf ${TMP_DIR}
}
trap onexit EXIT

for CONFIG in "${CONFIGS[@]}"; do
  CONFIG2=${CONFIG/-use_apache_hive/}
  if [[ "$CONFIG" != "$CONFIG2" ]]; then
    CONFIG=$CONFIG2
    export USE_APACHE_HIVE=true
  else
    export USE_APACHE_HIVE=false
  fi
  DESCRIPTION="Options $CONFIG USE_APACHE_HIVE=$USE_APACHE_HIVE"

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
  # Dump the current disk usage and check the usage for
  # several heavy users of disk space.
  df
  du -s ~/.m2 ./be ./toolchain

  # Keep each maven log from each round of the build
  cp logs/mvn/mvn.log "${TMP_DIR}/mvn.$(date +%s.%N).log"
  # Append the maven log to the accumulated maven log
  cat logs/mvn/mvn.log >> "${TMP_DIR}/mvn_accumulated.log"
done

# It is useful to be able to use "mvn versions:set" to modify the jar
# versions produced by Impala. This tests that Impala continues to build
# after running that command.
NEW_MAVEN_VERSION=4.0.0-$(date +%Y%m%d)
DESCRIPTION="Options: mvn versions:set -DnewVersion=${NEW_MAVEN_VERSION}"

if [[ $# == 1 && $1 == "--dryrun" ]]; then
  echo $DESCRIPTION
else
  # Note: this command modifies files in the git checkout
  pushd bin
  sed "s#export IMPALA_VERSION=.*#export IMPALA_VERSION=${NEW_MAVEN_VERSION}#g"\
      -i impala-config.sh
  popd
  pushd java
  mvn org.codehaus.mojo:versions-maven-plugin:2.13.0:set -DnewVersion=${NEW_MAVEN_VERSION}
  popd

  if ! ./bin/clean.sh; then
    echo "Clean failed"
    exit 1
  fi

  echo "Building with OPTIONS: $DESCRIPTION"
  if ! time -p ./buildall.sh -skiptests -noclean ; then
    echo "Build failed: $DESCRIPTION"
    FAILED="${FAILED}:${DESCRIPTION}"
  fi

  if ! git reset --hard HEAD ; then
    echo "Failed to reset the files changed by mvn versions:set"
  fi

  ccache -s
  bin/jenkins/get_maven_statistics.sh logs/mvn/mvn.log
  # Dump the current disk usage and check the usage for
  # several heavy users of disk space.
  df
  du -s ~/.m2 ./be ./toolchain

  # Keep each maven log from each round of the build
  cp logs/mvn/mvn.log "${TMP_DIR}/mvn.$(date +%s.%N).log"
  # Append the maven log to the accumulated maven log
  cat logs/mvn/mvn.log >> "${TMP_DIR}/mvn_accumulated.log"
fi

# To avoid diskspace issues, clean up the Impala build directory
# before creating the archive.
rm -rf be/build

# Restore the maven logs (these don't interfere with existing mvn.log)
# These files may not exist if this is doing a dryrun.
if ls ${TMP_DIR}/mvn* > /dev/null 2>&1 ; then
  cp ${TMP_DIR}/mvn* logs/mvn
fi

if [[ "$FAILED" != "" ]]
then
  echo "The following builds failed:"
  echo "$FAILED"
  exit 1
fi

if [[ "$GENERATE_M2_ARCHIVE" == true ]]; then
  echo "Generating m2 archive."

  # Print current diskspace
  df

  # Make a tarball of the .m2 directory
  bin/jenkins/archive_m2_directory.sh logs/mvn/mvn_accumulated.log logs/m2_archive.tar.gz
fi

# Note: The exit callback handles cleanup of the temp directory.
