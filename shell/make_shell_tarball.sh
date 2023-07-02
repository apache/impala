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

if [ $# -lt 2 ]; then
  echo "Must provide pypi package and at least one python interpreter"
  exit 1
fi

PYPI_PACKAGE=$1
shift
SHELL_HOME=${IMPALA_HOME}/shell
BUILD_DIR=${SHELL_HOME}/build
TARBALL_ROOT=${BUILD_DIR}/impala-shell-${IMPALA_VERSION}

for PYTHON_EXE in $*; do
  PYTHON_NAME=$(basename ${PYTHON_EXE})
  PYTHON_VERSION=$(${PYTHON_EXE} -c 'import sys; \
    print("{}.{}".format(sys.version_info.major, sys.version_info.minor))')
  PYTHON_MAJOR_VERSION=$(${PYTHON_EXE} -c 'import sys; print(sys.version_info.major)')
  # pip install the wheel into the external dependencies directory
  PIP_CACHE="~/.cache/impala_pip/${PYTHON_NAME}"
  BUILD_TMP_DIR="$(mktemp -d)"

  echo "Deleting all files in ${TARBALL_ROOT}/install_py${PYTHON_VERSION}"
  rm -rf ${TARBALL_ROOT}/install_py${PYTHON_VERSION} 2>&1 > /dev/null
  echo "Installing for python ${PYTHON_VERSION}"
  # Use pip that matches the major version
  if [[ $PYTHON_MAJOR_VERSION == 2 ]]; then
    source ${IMPALA_HOME}/shell/build/python2_venv/bin/activate
  else
    source ${IMPALA_HOME}/shell/build/python3_venv/bin/activate
  fi
  mkdir -p ${TARBALL_ROOT}/install_py${PYTHON_VERSION}
  pip install --cache ${PIP_CACHE} \
      --target ${TARBALL_ROOT}/install_py${PYTHON_VERSION} ${PYPI_PACKAGE}
  # We don't need the impala-shell binary for the installation. It contains
  # a weird shebang from the virtualenv, so it is worth removing it.
  rm ${TARBALL_ROOT}/install_py${PYTHON_VERSION}/bin/impala-shell
  # Cleanup temp build directory
  rm -rf ${BUILD_TMP_DIR}
done

# Copy the impala-shell driver script into the tarball root
cp ${SHELL_HOME}/impala-shell ${TARBALL_ROOT}

pushd ${BUILD_DIR} > /dev/null
echo "Making tarball in ${BUILD_DIR}"
tar czf ${BUILD_DIR}/impala-shell-${IMPALA_VERSION}.tar.gz --exclude="*.pyc" \
    ./impala-shell-${IMPALA_VERSION}/ || popd 2>&1 > /dev/null
