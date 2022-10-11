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
#
# This script verifies that the Java pom.xml files use the
# IMPALA_VERSION. This can fail if the IMPALA_VERSION changes
# without the version in the pom.xml changing. To update the
# pom.xml files, run:
# cd $IMPALA_HOME/java
# mvn versions:set -DnewVersion=NEW_MAVEN_VERSION

set -euo pipefail

MAVEN_VERSION_STRING="<version>${IMPALA_VERSION}</version>"

# Detect if IMPALA_HOME is a git repository
pushd ${IMPALA_HOME} > /dev/null 2>&1
IS_GIT_CHECKOUT=false
if git ls-files --error-unmatch > /dev/null 2>&1 ; then
  IS_GIT_CHECKOUT=true
fi;
popd > /dev/null 2>&1

RETVAL=0
NO_MATCH_FILES=()
for pom_file in $(find ${IMPALA_HOME} -path ${IMPALA_TOOLCHAIN} -prune \
    -o -name pom.xml -print); do
  # If this is a git checkout, then only do the check for pom.xml
  # files known to git. If this is not a git checkout, then it should
  # be building from a tarball, and there should not be extra
  # pom.xml files except in the toolchain folder.
  if ${IS_GIT_CHECKOUT} &&
     ! git ls-files --error-unmatch ${pom_file} > /dev/null 2>&1 ; then
    # This pom.xml file is not known to git.
    continue;
  fi
  if ! grep $MAVEN_VERSION_STRING "${pom_file}" > /dev/null; then
    NO_MATCH_FILES+=(${pom_file})
    RETVAL=1
  fi
done

if [[ $RETVAL != 0 ]]; then
  echo "Check for Java pom.xml versions FAILED"
  echo "Expected ${MAVEN_VERSION_STRING}"
  echo "Not found in:"
  for pom_file in ${NO_MATCH_FILES[@]}; do
    echo "  ${pom_file}"
  done
  echo "The pom.xml files can be updated automatically via:"
  echo 'cd ${IMPALA_HOME}/java; mvn versions:set -DnewVersion=YOUR_NEW_VERSION'
fi

exit $RETVAL
