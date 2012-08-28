#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Starts up a single Impalad with the specified command line arguments. An optional
# -build_type parameter can be passed to determine the build type to use for the
# impalad instance.

. ${IMPALA_HOME}/bin/set-classpath.sh

BUILD_TYPE=debug
IMPALAD_ARGS=""

# Everything except for -build_type should be passed an an Impalad argument
for ARG in $*
do
  case "$ARG" in
    -build_type=debug)
      BUILD_TYPE=debug
      ;;
    -build_type=release)
      BUILD_TYPE=release
      ;;
    -build_type=*)
      echo "Invalid build type. Valid values are: debug, release"
      exit 1
      ;;
    *)
      IMPALAD_ARGS="${IMPALAD_ARGS} ${ARG}"
  esac
done

$IMPALA_HOME/be/build/${BUILD_TYPE}/service/impalad ${IMPALAD_ARGS}
