#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Generates the impala version and build information.
VERSION=0.1
GIT_HASH=$(git rev-parse HEAD)
BUILD_TIME=`date`
HEADER="# Generated version information from save-version.sh"
echo -e \
"${HEADER}\nVERSION: ${VERSION}\nGIT_HASH: ${GIT_HASH}\nBUILD_TIME: ${BUILD_TIME}"\
> $IMPALA_HOME/bin/version.info
