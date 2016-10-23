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

if [[ -z "$DISTCC_HOSTS" || -z "$IMPALA_REAL_CXX_COMPILER" ]]; then
  # This could be sourced here and the build would work but the parallelization (-j)
  # should be wrong at this point and it's too late to fix.
  DIR=$(dirname "$0")
  echo "You must source '$DIR/distcc_env.sh' before attempting to build." 1>&2
  exit 1
fi

TOOLCHAIN_DIR=/opt/Impala-Toolchain
if [[ ! -d "$TOOLCHAIN_DIR" ]]; then
  if [[ -n "$IMPALA_TOOLCHAIN" && -d "$IMPALA_TOOLCHAIN" ]]; then
    if ! sudo -n -- ln -s "$IMPALA_TOOLCHAIN" "$TOOLCHAIN_DIR" &>/dev/null; then
      echo The toolchain must be available at $TOOLCHAIN_DIR for distcc. \
          Try running '"sudo ln -s $IMPALA_TOOLCHAIN $TOOLCHAIN_DIR"'. 1>&2
      exit 1
    fi
  fi
  echo "The toolchain wasn't found at '$TOOLCHAIN_DIR' and IMPALA_TOOLCHAIN is not set." \
      Make sure the toolchain is available at $TOOLCHAIN_DIR and try again. 1>&2
  exit 1
fi

CMD=
CMD_POST_ARGS=
if $IMPALA_USE_DISTCC; then
  CMD="distcc ccache"
fi

GCC_ROOT="$TOOLCHAIN_DIR/gcc-$IMPALA_GCC_VERSION"
case "$IMPALA_REAL_CXX_COMPILER" in
  gcc) CMD+=" $GCC_ROOT/bin/g++";;
  clang) # Assume the compilation options were setup for gcc, which would happen using
         # default build options. Now some additional options need to be added for clang.
         CMD+=" $TOOLCHAIN_DIR/llvm-$IMPALA_LLVM_ASAN_VERSION/bin/clang++"
         CMD+=" --gcc-toolchain=$GCC_ROOT"
         # -Wno-unused-local-typedef needs to go after -Wall
         # -Wno-error is needed, clang generates more warnings than gcc.
         CMD_POST_ARGS+=" -Wno-unused-local-typedef -Wno-error";;
  *) echo "Unexpected IMPALA_REAL_CXX_COMPILER: '$IMPALA_REAL_CXX_COMPILER'" 1>&2
     exit 1;;
esac

exec $CMD "$@" $CMD_POST_ARGS
