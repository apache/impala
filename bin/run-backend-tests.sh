#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Prepare output directory
mkdir -p $IMPALA_TEST_CLUSTER_LOG_DIR/be_test
export GTEST_OUTPUT="xml:$IMPALA_TEST_CLUSTER_LOG_DIR/be_test/"

: ${SKIP_BE_TEST_PATTERN:=}

# The backend unit tests currently do not work when HEAPCHECK is enabled.
export HEAPCHECK=
set -e
set -u

BE_TEST_ARGS=""
if [[ -n "$SKIP_BE_TEST_PATTERN" ]]; then
  BE_TEST_ARGS="-E ${SKIP_BE_TEST_PATTERN}"
fi

cd ${IMPALA_BE_DIR}
. ${IMPALA_HOME}/bin/set-classpath.sh

make test ARGS="${BE_TEST_ARGS}"
