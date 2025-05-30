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

# ----------------------------------------------------------------------
# This script is invoked during the Impala build process, and creates
# a distributable python package of the Impala shell. The resulting
# archive will be saved to:
#
#   ${DIST_DIR}/impala_shell-<version>.tar.gz
#
# Until the thrift-generated python files in ${IMPALA_HOME}/shell/impala_thrift_gen
# have been created by the build process, this script will not work.
# It also relies upon the impala_build_version.py file created by the
# ${IMPALA_HOME}/shell/gen_impala_build_version.sh script.
#
# After those files exist, however, this script can be run again at will.

set -eu -o pipefail

WORKING_DIR="$(cd "$(dirname "$0")" ; pwd -P )"
SHELL_HOME="${IMPALA_HOME}"/shell
STAGING_DIR="${WORKING_DIR}"/staging
DIST_DIR="${DIST_DIR:-$SHELL_HOME/dist}"
PACKAGE_DIR="${STAGING_DIR}"/impala_shell_package
NO_CLEAN_DIST="${NO_CLEAN_DIST:-}"

assemble_package_files() {
  mkdir -p "${PACKAGE_DIR}"

  cp -r "${SHELL_HOME}/impala_thrift_gen" "${PACKAGE_DIR}"
  cp -r "${SHELL_HOME}/impala_shell" "${PACKAGE_DIR}"

  cp "${SHELL_HOME}/README.md" "${PACKAGE_DIR}"
  cp "${SHELL_HOME}/MANIFEST.in" "${PACKAGE_DIR}"
  cp "${SHELL_HOME}/requirements.txt" "${PACKAGE_DIR}"
  cp "${SHELL_HOME}/setup.py" "${PACKAGE_DIR}"

  cp "${IMPALA_HOME}/LICENSE.txt" "${PACKAGE_DIR}"
}

create_distributable_python_package() {
  # Generate a new python package tarball in ${DIST_DIR}
  if [[ "${NO_CLEAN_DIST}" != "true" ]]; then
    rm -rf "${DIST_DIR}"
  fi

  mkdir -p "${DIST_DIR}"

  pushd "${PACKAGE_DIR}"
  echo "Building package..."
  PACKAGE_TYPE="${PACKAGE_TYPE:-}" OFFICIAL="${OFFICIAL:-}" \
    impala-python3 setup.py sdist --dist-dir "${DIST_DIR}"
  popd

  if [[ "${NO_CLEAN_DIST}" != "true" ]]; then
    rm -rf "${STAGING_DIR}"
  fi
}

assemble_package_files
create_distributable_python_package
