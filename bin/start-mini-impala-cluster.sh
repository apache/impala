#!/bin/sh
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts up an in-process impala cluster. The number of backeds is 3 by default, 
# and can be controled by the -num_backends flag.
# -build_type parameter can be passed to determine the build type to use for the
# in-process cluster.

BUILD_TYPE=debug
IMPALAD_ARGS=""
CLASSPATH_PREFIX=""
# Everything except for -build_type should be passed as an Impalad argument
for ARG in $*
do
  case "$ARG" in
    -build_type=debug)
      BUILD_TYPE=debug
      ;;
    -build_type=release)
      BUILD_TYPE=release
      ;;
    -classpath_prefix=*)
      CLASSPATH_PREFIX=`echo $ARG | sed 's/-classpath_prefix=//'`
      ;;
    -build_type=*)
      echo "Invalid build type. Valid values are: debug, release"
      exit 1
      ;;
    *)
      IMPALAD_ARGS="${IMPALAD_ARGS} ${ARG}"
  esac
done

. ${IMPALA_HOME}/bin/set-classpath.sh

CLASSPATH=$CLASSPATH_PREFIX:$CLASSPATH \
  $IMPALA_HOME/be/build/${BUILD_TYPE}/testutil/mini-impala-cluster ${IMPALAD_ARGS}
