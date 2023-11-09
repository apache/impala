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
# Until the thrift-generated python files in ${IMPALA_HOME}/shell/gen-py
# have been created by the build process, this script will not work.
# It also relies upon the impala_build_version.py file created by the
# parent packaging script, ${IMPALA_HOME}/shell/make_shell_tarball.sh,
# which needs to be run before this script will work.
#
# After those files exist, however, this script can be run again at will.

set -eu -o pipefail

WORKING_DIR="$(cd "$(dirname "$0")" ; pwd -P )"
SHELL_HOME="${IMPALA_HOME}"/shell
STAGING_DIR="${WORKING_DIR}"/staging
DIST_DIR="${DIST_DIR:-$SHELL_HOME/dist}"
PACKAGE_DIR="${STAGING_DIR}"/impala_shell_package
MODULE_LIB_DIR="${PACKAGE_DIR}"/impala_shell
NO_CLEAN_DIST="${NO_CLEAN_DIST:-}"

THRIFT_GEN_PY_DIR=${SHELL_HOME}/gen-py

assemble_package_files() {
  mkdir -p "${MODULE_LIB_DIR}"

  cp -r "${THRIFT_GEN_PY_DIR}"/* "${MODULE_LIB_DIR}"

  cp "${WORKING_DIR}/__init__.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/compatibility.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/impala_shell.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/impala_client.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/option_parser.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/shell_output.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/impala_shell_config_defaults.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/TSSLSocketWithWildcardSAN.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/ImpalaHttpClient.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/shell_exceptions.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/cookie_util.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/kerberos_util.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/value_converter.py" "${MODULE_LIB_DIR}"
  cp "${SHELL_HOME}/thrift_printer.py" "${MODULE_LIB_DIR}"

  cp "${SHELL_HOME}/packaging/README.md" "${PACKAGE_DIR}"
  cp "${SHELL_HOME}/packaging/MANIFEST.in" "${PACKAGE_DIR}"
  cp "${SHELL_HOME}/packaging/requirements.txt" "${PACKAGE_DIR}"
  cp "${SHELL_HOME}/packaging/setup.py" "${PACKAGE_DIR}"

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
    impala-python setup.py sdist --dist-dir "${DIST_DIR}"
  popd

  if [[ "${NO_CLEAN_DIST}" != "true" ]]; then
    rm -rf "${STAGING_DIR}"
  fi
}

assemble_package_files
create_distributable_python_package
