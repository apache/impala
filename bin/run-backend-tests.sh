#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Prepare output directory
mkdir -p $IMPALA_TEST_CLUSTER_LOG_DIR/be_test
export GTEST_OUTPUT="xml:$IMPALA_TEST_CLUSTER_LOG_DIR/be_test/"


# The backend unit tests currently do not work when HEAPCHECK is enabled.
export HEAPCHECK=
set -e
set -u

cd ${IMPALA_BE_DIR}
. ${IMPALA_HOME}/bin/set-classpath.sh
make test
