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

# Sets up a python 3 virtual environment with all necessary dependencies
# available for the jwt-util.py script.

set -euo pipefail

WORK_DIR="$(mktemp -d)"
trap "rm -rf ${WORK_DIR}" EXIT
echo "Using working directory: ${WORK_DIR}"

MOD_DIR="${WORK_DIR}/python_modules"
VENV_DIR="${WORK_DIR}/.venv"
DATA_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/../jwt"

# dependecies for creating a python virtual environment
mkdir -p "${MOD_DIR}"
pip3 install virtualenv --target="${MOD_DIR}"

# turn off the prompt setting since the virtual environment is loaded in a
# non-interactive script
VIRTUAL_ENV_DISABLE_PROMPT=1
export VIRTUAL_ENV_DISABLE_PROMPT

# create and active the python virtual environment
"${MOD_DIR}/bin/virtualenv" --python python3 "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"

# install necessary dependencies for the jwt generation python script
python -m pip install -r "$(dirname "${0}")/jwt_requirements.txt"

python "$(dirname "${0}")/jwt-util.py" "${DATA_DIR}"
