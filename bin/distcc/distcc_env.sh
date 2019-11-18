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

# This file is intended to be sourced by a shell (zsh and bash have been tested).

if [[ -z $BUILD_FARM ]]
then
  echo "BUILD_FARM must be set to configure distcc" >&2
  return 1
fi

if [[ ! -z $ZSH_NAME ]]; then
  DISTCC_ENV_DIR=$(cd $(dirname ${(%):-%x}) && pwd)
else
  DISTCC_ENV_DIR=$(cd $(dirname "${BASH_SOURCE[0]}") && pwd)
fi

function cmd_exists {
  which $1 &>/dev/null
}

INSTALLER=
if cmd_exists apt-get; then
  INSTALLER=apt-get
elif cmd_exists yum; then
  INSTALLER=yum
fi

if ! cmd_exists distcc; then
  echo distcc command not found, attempting installation
  if [[ -z $INSTALLER ]] || ! sudo $INSTALLER -y install distcc; then
    echo Unable to automatically install distcc. You need to install it manually. 1>&2
    return 1
  fi
fi

# Cleans old CMake files, this is required after installing ccache, since we need
# CMake to insert ccache into all the command lines.
function clean_cmake_files {
  if [[ -z "$IMPALA_HOME" || ! -d "$IMPALA_HOME" ]]; then
    echo IMPALA_HOME=$IMPALA_HOME is not valid. 1>&2
    return 1
  fi
  $IMPALA_HOME/bin/clean-cmake.sh
}

# Install CCache if necessary.
if ! cmd_exists ccache; then
  echo "ccache command not found, attempting installation"
  if [[ -z $INSTALLER ]] || ! sudo $INSTALLER -y install ccache; then
    echo "Unable to automatically install ccache"
    return 1
  fi
  if ! clean_cmake_files; then
    echo Failed to clean cmake files. 1>&2
    return 1
  fi
  echo "distcc is not fully enabled, run 'buildall.sh' to complete the change." \
    "Run 'disable_distcc' or 'switch_compiler local' to disable."
fi

# Don't include localhost in the list. It is already the slowest part of the build because
# it needs to do preprocessing and linking. There shouldn't be a need to add an extra
# compilation worker.
export DISTCC_HOSTS=
DISTCC_HOSTS+=" --localslots=$(nproc)"
DISTCC_HOSTS+=" --localslots_cpp=$(nproc)"
DISTCC_HOSTS+=" --randomize"
DISTCC_HOSTS+=" ${BUILD_FARM}"

# Set to false to make distcc.sh use local compilation instead of distcc. Takes effect
# immediately if the distcc.sh wrapper script is used.
: ${IMPALA_DISTCC_LOCAL=}
export IMPALA_DISTCC_LOCAL

# This is picked up by ccache as the command it should prefix all compiler invocations
# with. We only support enabling distcc if ccache is available, so pointing this at
# our distcc wrapper script is sufficient to enable distcc.
export CCACHE_PREFIX=""

# Even after generating make files, some state about compiler options would only exist in
# environment vars. Any such vars should be saved to this file so they can be restored.
if [[ -z "$IMPALA_HOME" ]]; then
  echo '$IMPALA_HOME must be set before sourcing this file.' 1>&2
  return 1
fi

export IMPALA_DISTCC_ENV_VERSION=3
IMPALA_COMPILER_CONFIG_FILE="$IMPALA_HOME/.impala_compiler_opts_v${IMPALA_DISTCC_ENV_VERSION}"

# Completely disable anything that could have been setup using this script and clean
# the make files.
function disable_distcc {
  export IMPALA_BUILD_THREADS=$(nproc)
  save_compiler_opts
}

function enable_distcc {
  switch_compiler distcc
}

function switch_compiler {
  for ARG in "$@"; do
    case "$ARG" in
      "local")
        export
        IMPALA_DISTCC_LOCAL=false
        IMPALA_BUILD_THREADS=$(nproc)
        CCACHE_PREFIX=""
        ;;
      distcc)
        IMPALA_DISTCC_LOCAL=true
        local DISTCC_SLOT_COUNT=$(distcc -j)
        # Set the parallelism based on the number of distcc slots, but cap it to avoid
        # overwhelming this host  if a large distcc cluster is configured.
        IMPALA_BUILD_THREADS=$((DISTCC_SLOT_COUNT < 128 ? DISTCC_SLOT_COUNT : 128))
        CCACHE_PREFIX="${IMPALA_HOME}/bin/distcc/distcc.sh"
        ;;
      *) echo "Valid compiler options are:
    'local'  - Don't use distcc and set -j value to $(nproc).
    'distcc' - Use distcc and set -j value to $(distcc -j)." 2>&1
        return 1;;
    esac
  done
  save_compiler_opts
}

function save_compiler_opts {
  rm -f "$IMPALA_COMPILER_CONFIG_FILE"
  cat <<EOF > "$IMPALA_COMPILER_CONFIG_FILE"
CCACHE_PREFIX=$CCACHE_PREFIX
IMPALA_BUILD_THREADS=$IMPALA_BUILD_THREADS
IMPALA_DISTCC_LOCAL=$IMPALA_DISTCC_LOCAL
EOF
}

if [[ -e "$IMPALA_COMPILER_CONFIG_FILE" ]]; then
  source "$IMPALA_COMPILER_CONFIG_FILE"
else
  enable_distcc
fi
