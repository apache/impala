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

set -euo pipefail

# This script runs the impala shell from a dev environment.
PYTHONPATH=${PYTHONPATH:-}
SHELL_HOME=${IMPALA_SHELL_HOME:-${IMPALA_HOME}/shell}

# ${IMPALA_HOME}/bin has bootstrap_toolchain.py, required by bootstrap_virtualenv.py
PYTHONPATH=${PYTHONPATH}:${IMPALA_HOME}/bin

# Default version of thrift for the impala-shell is thrift >= 0.11.0.
PYTHONPATH=${PYTHONPATH}:${SHELL_HOME}/build/thrift-11-gen/gen-py
IMPALA_THRIFT_PY_VERSION=${IMPALA_THRIFT11_VERSION}

THRIFT_PY_ROOT="${IMPALA_TOOLCHAIN}/thrift-${IMPALA_THRIFT_PY_VERSION}"

LD_LIBRARY_PATH+=":$(PYTHONPATH=${PYTHONPATH} \
  python "$IMPALA_HOME/infra/python/bootstrap_virtualenv.py" \
  --print-ld-library-path)"

IMPALA_PY_DIR="$(dirname "$0")/../infra/python"
IMPALA_PYTHON_EXECUTABLE="${IMPALA_PY_DIR}/env/bin/python"

for PYTHON_LIB_DIR in ${THRIFT_PY_ROOT}/python/lib{64,}; do
  [[ -d ${PYTHON_LIB_DIR} ]] || continue
  for PKG_DIR in ${PYTHON_LIB_DIR}/python*/site-packages; do
    PYTHONPATH=${PYTHONPATH}:${PKG_DIR}/
  done
done

# Note that this uses the external system python executable
PYTHONPATH=${PYTHONPATH} python "${IMPALA_PY_DIR}/bootstrap_virtualenv.py"

# This uses the python executable in the impala python env
PYTHONIOENCODING='utf-8' PYTHONPATH=${PYTHONPATH} \
  exec "${IMPALA_PYTHON_EXECUTABLE}" ${SHELL_HOME}/impala_shell.py "$@"
