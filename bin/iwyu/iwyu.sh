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

set -euo pipefail

IMPALA_HOME=$(cd $(dirname "$0")/../.. && pwd)

usage() {
  echo "Usage: iwyu.sh"
  echo "This will run include-what-you-use on the Impala codebase."
  echo "Results are printed to stdout"
  echo "The following environment variables must be set:"
  echo "  KUDU_SOURCE: the root directory of a Kudu source tree"
  echo "  IWYU_BUILD_DIR: the root directory of a IWYU build tree"
  echo "  IWYU_SOURCE: the root directory of a IWYU source tree"
  echo ""
  echo "See IMPALA-9371 for one way to build IWYU for toolchain clang."
  echo ""
  echo "Example Invocation:"
  echo "    KUDU_SOURCE=~/kudu IWYU_SOURCE=~/include-what-you-use \\"
  echo "    IWYU_BUILD_DIR=~/include-what-you-use/build \\"
  echo "    \$IMPALA_HOME/bin/iwyu/iwyu.sh"
}

if [[ ! -v KUDU_SOURCE || ! -d "$KUDU_SOURCE" ]]; then
  echo "KUDU_SOURCE must be set to a Kudu source directory"
  usage
  exit 1
fi

if [[ ! -v IWYU_BUILD_DIR || ! -d "$IWYU_BUILD_DIR" ]]; then
  echo "IWYU_BUILD_DIR must be set to a IWYU build directory"
  usage
  exit 1
fi

if [[ ! -v IWYU_SOURCE || ! -d "$IWYU_SOURCE" ]]; then
  echo "IWYU_SOURCE must be set to a IWYU source directory"
  usage
  exit 1
fi

if [[ ! -f "$IMPALA_HOME/compile_commands.json" ]]; then
  echo "$IMPALA_HOME/compile_commands.json is required for IWYU."
  echo "Please run buildall.sh (or CMake directly) to generate it"
  exit 1
fi


IWYU_ARGS="--mapping_file=iwyu_mappings.imp"

# Make use of Kudu's pre-existing mappings files that are relevant.
# TODO: consider importing into Impala codebase.
for FILE in gflags.imp gtest.imp kudu.imp libstdcpp.imp libunwind.imp system-linux.imp; do
  IWYU_ARGS+=" --mapping_file=$KUDU_SOURCE/build-support/iwyu/mappings/${FILE}"
done

cd "$IMPALA_HOME"
PATH=$IWYU_BUILD_DIR:$PATH $IWYU_SOURCE/iwyu_tool.py -p . -j $(nproc) -- $IWYU_ARGS
