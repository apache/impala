#!/usr/bin/env bash
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

DRY_RUN=0

if [[ "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage: omake.sh [--dry-run] <file_name>

Build a single C++ object file using the generated CMake build.make. The only parameter is
the name of the C++ source file to build without any extension. The script will search for
the corresponding build.make file and invoke make on that file.

Example (for building be/src/runtime/runtime-state.cc):
  omake.sh runtime-state
  omake.sh --dry-run runtime-state
EOF
  exit 0
fi

if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=1
  shift
fi

BASENAME="${1%.*}"
BASENAME=${BASENAME##*/}
MAKEFILE=$(grep -l "$BASENAME.cc.o:" $IMPALA_HOME/be/src/*/CMakeFiles/*.dir/build.make)
MAKEFILE=${MAKEFILE#$IMPALA_HOME/}
MAKE_CMD=(make DEBUG_NOOPT=1 -s -f "$MAKEFILE" "${MAKEFILE%/*}/$BASENAME.cc.o")

echo "${MAKE_CMD[*]}"
if [[ "$DRY_RUN" -eq 1 ]]; then
  exit 0
fi

"${MAKE_CMD[@]}"
