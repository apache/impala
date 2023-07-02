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

# The shell tarball build only needs the build virtualenvs for the system
# pythons that are installed.
set(IMPALA_PYTHON_BUILD_VENVS "")

set(VENV_LOC "${CMAKE_SOURCE_DIR}/shell/build")
set(PIP_LOC "~/.cache/impala_pip")

# Tests depend on installing system pythons to specific locations, so we error if they
# won't match what's expected in this config and make_shell_tarball.sh.
set(PYTHON_EXES $ENV{IMPALA_EXTRA_PACKAGE_PYTHONS})
if (NOT $ENV{IMPALA_SYSTEM_PYTHON2} STREQUAL "")
  get_filename_component(PYTHON_NAME $ENV{IMPALA_SYSTEM_PYTHON2} NAME)
  if (NOT ${PYTHON_NAME} STREQUAL "python2")
    message(FATAL_ERROR "IMPALA_SYSTEM_PYTHON2 must be a binary named python2")
  endif()
  list(APPEND PYTHON_EXES $ENV{IMPALA_SYSTEM_PYTHON2})
endif()
if (NOT $ENV{IMPALA_SYSTEM_PYTHON3} STREQUAL "")
  get_filename_component(PYTHON_NAME $ENV{IMPALA_SYSTEM_PYTHON3} NAME)
  if (NOT ${PYTHON_NAME} STREQUAL "python3")
    message(FATAL_ERROR "IMPALA_SYSTEM_PYTHON3 must be a binary named python3")
  endif()
  list(APPEND PYTHON_EXES $ENV{IMPALA_SYSTEM_PYTHON3})
endif()
message(STATUS "Packaging for ${PYTHON_EXES}")

foreach(PYTHON_EXE IN LISTS PYTHON_EXES)
  get_filename_component(PYTHON_NAME "${PYTHON_EXE}" NAME)
  # These virtualenvs serve two purposes:
  # 1. They have system python with wheel installed, and they can be used to produce
  #    wheels for external dependencies for the shell tarball build.
  # 2. We pip install impala-shell into them for use in tests.
  # The initial virtualenv creation includes the "pip install wheel" command to
  # satisfy #1. #2 is a separate step and has no interaction with #1.
  set(VENV "${VENV_LOC}/${PYTHON_NAME}_venv")
  # IMPALA-12117: Use separate pip cache directories to avoid concurrency
  # issues. The standard location is in ~/.cache/pip, so this uses directories
  # inside ~/.cache. These typical consume a couple MB each.
  set(PIP_CACHE "${PIP_LOC}/${PYTHON_NAME}")

  # Supports fallback to impala-virtualenv for older Python versions.
  # This upgrades pip in the virtualenv to make the behavior more consistent across
  # different distributions
  add_custom_target(${PYTHON_NAME}_venv
    BYPRODUCTS "${VENV}"
    DEPENDS impala_python
    COMMAND "${CMAKE_SOURCE_DIR}/bin/cmake_aux/create_virtualenv.sh"
            "${PYTHON_EXE}" "${VENV}"
    COMMAND "${VENV}/bin/pip" install --cache-dir "${PIP_CACHE}" --upgrade pip
    COMMAND "${VENV}/bin/pip" install --cache-dir "${PIP_CACHE}" wheel
  )

  list(APPEND IMPALA_PYTHON_BUILD_VENVS ${PYTHON_NAME}_venv)
endforeach()

add_custom_target(shell_impala_build_version
  DEPENDS gen-deps "${CMAKE_SOURCE_DIR}/bin/version.info"
  COMMAND "${CMAKE_SOURCE_DIR}/shell/gen_impala_build_version.sh"
)

add_custom_target(shell_pypi_package DEPENDS gen-deps shell_impala_build_version impala_python
  COMMAND "${CMAKE_SOURCE_DIR}/shell/packaging/make_python_package.sh"
)

# A separate package target is needed because without OFFICIAL the file name is
# non-deterministic. Uses a custom target to synchronize for multiple dependents.
# Derive version from IMPALA_VERSION (drops everything after '-' because PEP 440 requires
# '+' but setup.py doesn't treat it consistently when generating the file name).
string(REGEX REPLACE "-.*" "" PKG_VERSION $ENV{IMPALA_VERSION})
set(SHELL_TEST_PKG
  "${CMAKE_SOURCE_DIR}/shell/build/dist/impala_shell-${PKG_VERSION}.tar.gz")
get_filename_component(SHELL_TEST_PKG_DIR "${SHELL_TEST_PKG}" DIRECTORY)
# Generates SHELL_TEST_PKG
add_custom_target(shell_pypi_test_package DEPENDS gen-deps shell_impala_build_version impala_python
  COMMAND env BUILD_VERSION=${PKG_VERSION} OFFICIAL=true DIST_DIR="${SHELL_TEST_PKG_DIR}"
    "${CMAKE_SOURCE_DIR}/shell/packaging/make_python_package.sh"
)

# Tests expect to find venvs at 'python2_venv' and 'python3_venv' in tests/shell/util.py.
set(PYTHON2_VENV "${VENV_LOC}/python2_venv")
add_custom_target(shell_python2_install DEPENDS python2_venv shell_pypi_test_package
  COMMAND "${PYTHON2_VENV}/bin/pip" install --cache-dir "${PIP_LOC}/python2" "${SHELL_TEST_PKG}"
)

set(PYTHON3_VENV "${VENV_LOC}/python3_venv")
add_custom_target(shell_python3_install DEPENDS python3_venv shell_pypi_test_package
  COMMAND "${PYTHON3_VENV}/bin/pip" install --cache-dir "${PIP_LOC}/python3" "${SHELL_TEST_PKG}"
)

add_custom_target(shell_tarball DEPENDS gen-deps shell_pypi_test_package "${IMPALA_PYTHON_BUILD_VENVS}"
  COMMAND "${CMAKE_SOURCE_DIR}/shell/make_shell_tarball.sh" "${SHELL_TEST_PKG}" ${PYTHON_EXES}
)
