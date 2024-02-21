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

# Sets up the python path for impala-python. This is needed because tests and other
# utility scripts depend on some modules external to infra/python/env-*.
# TODO: we should try to reduce our reliance on PYTHONPATH if possible.
#
# Used to allow importing testdata, test, etc modules from other scripts.

# ${IMPALA_HOME}/bin has bootstrap_toolchain.py, required by bootstrap_virtualenv.py
export PYTHONPATH=${IMPALA_HOME}:${IMPALA_HOME}/bin

# Generated Thrift files are used by tests and other scripts.
PYTHONPATH=${PYTHONPATH}:${IMPALA_HOME}/shell/gen-py

PYTHONPATH=${PYTHONPATH}:${IMPALA_HOME}/infra/python/env-gcc${IMPALA_GCC_VERSION}/lib
