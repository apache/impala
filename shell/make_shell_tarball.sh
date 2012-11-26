#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This script makes a tarball of the Python-based shell that can be unzipped and
# run out-of-the-box with no configuration. The final tarball is left in
# ${IMPALA_HOME}/shell/build.

if [ "x${IMPALA_HOME}" == "x" ]; then
  echo "\$IMPALA_HOME must be set"
  exit 1
fi

IMPALA_VERSION_INFO_FILE=${IMPALA_HOME}/bin/version.info
VERSION=$(grep "VERSION: " ${IMPALA_VERSION_INFO_FILE} | awk '{print $2}')
GIT_HASH=$(grep "GIT_HASH: " ${IMPALA_VERSION_INFO_FILE} | awk '{print $2}')
BUILD_DATE=$(grep "BUILD_TIME: " ${IMPALA_VERSION_INFO_FILE} | cut -f 2- -d ' ')
cat ${IMPALA_VERSION_INFO_FILE}

SHELL_HOME=${IMPALA_HOME}/shell
BUILD_DIR=${SHELL_HOME}/build
TARBALL_ROOT=${BUILD_DIR}/impala-shell-${VERSION}

echo "Deleting all files in ${TARBALL_ROOT}/{gen-py,lib}"
rm -rf ${TARBALL_ROOT}/lib/* 2>&1 > /dev/null
rm -rf ${TARBALL_ROOT}/gen-py/* 2>&1 > /dev/null
mkdir -p ${TARBALL_ROOT}/lib

if [ ! -f ${IMPALA_VERSION_INFO_FILE} ]; then
  echo "No version.info file found. Generating new version info"
  ${IMPALA_HOME}/bin/save-version.sh
else
  echo "Using existing version.info file."
fi

rm -f ${SHELL_HOME}/gen-py/impala_build_version.py
cat > ${SHELL_HOME}/gen-py/impala_build_version.py <<EOF
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

def get_version():
  return "${VERSION}"

def get_git_hash():
  return "${GIT_HASH}"

def get_build_date():
  return "${BUILD_DATE}"
EOF

# Copy all the shell files into the build dir
cp -r ${HIVE_HOME}/lib/py/* ${TARBALL_ROOT}/lib
cp -r ${THRIFT_HOME}/python/lib/python*/site-packages/thrift\
    ${TARBALL_ROOT}/lib
cp -r ${SHELL_HOME}/gen-py ${TARBALL_ROOT}
cp ${SHELL_HOME}/thrift_sasl.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/impala-shell ${TARBALL_ROOT}
cp ${SHELL_HOME}/impala_shell.py ${TARBALL_ROOT}

pushd ${BUILD_DIR} > /dev/null
echo "Making tarball in ${BUILD_DIR}"
tar czf ${BUILD_DIR}/impala-shell-${VERSION}.tar.gz ./impala-shell-${VERSION}/\
    --exclude="*.pyc" || popd 2>&1 > /dev/null
