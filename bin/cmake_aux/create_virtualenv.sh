#!/bin/bash
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
# Create a python virtualenv. When system python is 3.6 or higher,
# we can just use the built-in venv module to create the virtualenv.
# If system python is older or the built-in venv module fails, then
# this falls back to impala-virtualenv, which uses python2 to
# initialize a virtualenv using the specified interpeter.
#
# This takes two arguments, which is the interpreter to use and the
# destination directory for the virtualenv:
# create_virtualenv.sh pythonX venv_dir

set -euo pipefail

PYTHON_EXE=$1
VENV_DIR=$2

IS_PY36_OR_HIGHER=$(${PYTHON_EXE} -c "import sys; print(\
  'true' if sys.version_info.major >= 3 and sys.version_info.minor >= 6 else 'false')")

if ! $IS_PY36_OR_HIGHER ; then
  echo "ERROR: $0 only supports Python >= 3.6"
  exit 1
fi

# Rely on the builtin venv package for Python >= 3.6
${PYTHON_EXE} -m venv ${VENV_DIR}
