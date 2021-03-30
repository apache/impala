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


# This script makes a tarball of the Python-based shell that can be unzipped and
# run out-of-the-box with no configuration. The final tarball is left in
# ${IMPALA_HOME}/shell/build.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

if [ "x${IMPALA_HOME}" == "x" ]; then
  echo "\$IMPALA_HOME must be set"
  exit 1
fi

IMPALA_VERSION_INFO_FILE=${IMPALA_HOME}/bin/version.info

if [ ! -f ${IMPALA_VERSION_INFO_FILE} ]; then
  echo "No version.info file found. Generating new version info"
  ${IMPALA_HOME}/bin/save-version.sh
else
  echo "Using existing version.info file."
fi

VERSION=$(grep "VERSION: " ${IMPALA_VERSION_INFO_FILE} | awk '{print $2}')
GIT_HASH=$(grep "GIT_HASH: " ${IMPALA_VERSION_INFO_FILE} | awk '{print $2}')
BUILD_DATE=$(grep "BUILD_TIME: " ${IMPALA_VERSION_INFO_FILE} | cut -f 2- -d ' ')
cat ${IMPALA_VERSION_INFO_FILE}

SHELL_HOME=${IMPALA_HOME}/shell
BUILD_DIR=${SHELL_HOME}/build
TARBALL_ROOT=${BUILD_DIR}/impala-shell-${VERSION}

IMPALA_THRIFT_PY_VERSION="${IMPALA_THRIFT11_VERSION}"
THRIFT_GEN_PY_DIR="${SHELL_HOME}/build/thrift-11-gen/gen-py"

echo "Deleting all files in ${TARBALL_ROOT}/{gen-py,lib,ext-py}"
rm -rf ${TARBALL_ROOT}/lib/* 2>&1 > /dev/null
rm -rf ${TARBALL_ROOT}/gen-py/* 2>&1 > /dev/null
rm -rf ${TARBALL_ROOT}/ext-py/* 2>&1 > /dev/null
mkdir -p ${TARBALL_ROOT}/lib
mkdir -p ${TARBALL_ROOT}/ext-py

rm -f ${THRIFT_GEN_PY_DIR}/impala_build_version.py
cat > ${THRIFT_GEN_PY_DIR}/impala_build_version.py <<EOF
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

def get_version():
  return "${VERSION}"

def get_git_hash():
  return "${GIT_HASH}"

def get_build_date():
  return "${BUILD_DATE}"
EOF

# Building all eggs.
echo "Building all external modules into eggs"
for MODULE in ${SHELL_HOME}/ext-py/*; do
  pushd ${MODULE} > /dev/null 2>&1
  echo "Cleaning up old build artifacts."
  rm -rf dist 2>&1 > /dev/null
  rm -rf build 2>&1 > /dev/null
  echo "Creating an egg for ${MODULE}"
  if [[ "$MODULE" == *"/bitarray"* ]]; then
    # Need to use setuptools to build egg for bitarray module
    python -c "import setuptools; exec(open('setup.py').read())" -q bdist_egg
  else
    python setup.py -q bdist_egg clean
  fi
  cp dist/*.egg ${TARBALL_ROOT}/ext-py
  popd 2>&1 > /dev/null
done

# Copy all the shell files into the build dir
# The location of python libs for thrift is different in rhel/centos/sles

THRIFT_PY_ROOT="${IMPALA_TOOLCHAIN}/thrift-${IMPALA_THRIFT_PY_VERSION}"

if [ -d ${THRIFT_PY_ROOT}/python/lib/python*/site-packages/thrift ]; then
  cp -r ${THRIFT_PY_ROOT}/python/lib/python*/site-packages/thrift\
        ${TARBALL_ROOT}/lib
else
  cp -r ${THRIFT_PY_ROOT}/python/lib64/python*/site-packages/thrift\
        ${TARBALL_ROOT}/lib
fi

cp -r ${THRIFT_GEN_PY_DIR} ${TARBALL_ROOT}
cp ${SHELL_HOME}/option_parser.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/impala_shell_config_defaults.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/impala_client.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/TSSLSocketWithWildcardSAN.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/ImpalaHttpClient.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/shell_exceptions.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/shell_output.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/pkg_resources.py ${TARBALL_ROOT}/lib
cp ${SHELL_HOME}/impala-shell ${TARBALL_ROOT}
cp ${SHELL_HOME}/impala_shell.py ${TARBALL_ROOT}
cp ${SHELL_HOME}/compatibility.py ${TARBALL_ROOT}

pushd ${BUILD_DIR} > /dev/null
echo "Making tarball in ${BUILD_DIR}"
tar czf ${BUILD_DIR}/impala-shell-${VERSION}.tar.gz ./impala-shell-${VERSION}/\
    --exclude="*.pyc" || popd 2>&1 > /dev/null
