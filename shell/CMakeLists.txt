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

add_custom_target(shell_tarball DEPENDS gen-deps
  COMMAND "${CMAKE_SOURCE_DIR}/shell/make_shell_tarball.sh"
)

add_custom_target(shell_pypi_package DEPENDS shell_tarball
  COMMAND "${CMAKE_SOURCE_DIR}/shell/packaging/make_python_package.sh"
)

# A separate package target is needed because without OFFICIAL the file name is
# non-deterministic. Uses a custom target to synchronize for multiple dependents.
set(SHELL_TEST_PKG
  "${CMAKE_SOURCE_DIR}/shell/build/dist/impala_shell-install-test.tar.gz")
get_filename_component(SHELL_TEST_PKG_DIR "${SHELL_TEST_PKG}" DIRECTORY)
# Generates SHELL_TEST_PKG
add_custom_target(shell_pypi_test_package DEPENDS shell_tarball
  COMMAND env BUILD_VERSION=install-test OFFICIAL=true DIST_DIR="${SHELL_TEST_PKG_DIR}"
    "${CMAKE_SOURCE_DIR}/shell/packaging/make_python_package.sh"
)

set(PYTHON2_VENV "${CMAKE_SOURCE_DIR}/shell/build/py2_venv")
set(PYTHON3_VENV "${CMAKE_SOURCE_DIR}/shell/build/py3_venv")

add_custom_command(OUTPUT "${PYTHON2_VENV}" DEPENDS impala_python
  COMMAND impala-virtualenv --python python2 "${PYTHON2_VENV}"
)

add_custom_target(shell_python2_install DEPENDS "${PYTHON2_VENV}" shell_pypi_test_package
  COMMAND "${PYTHON2_VENV}/bin/pip" install "${SHELL_TEST_PKG}"
)

add_custom_command(OUTPUT "${PYTHON3_VENV}" DEPENDS impala_python
  COMMAND impala-virtualenv --python python3 "${PYTHON3_VENV}"
)

add_custom_target(shell_python3_install DEPENDS "${PYTHON3_VENV}" shell_pypi_test_package
  COMMAND "${PYTHON3_VENV}/bin/pip" install "${SHELL_TEST_PKG}"
)
