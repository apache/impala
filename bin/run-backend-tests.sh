#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

export GTEST_OUTPUT="xml:$IMPALA_BE_TEST_LOGS_DIR/"

: ${SKIP_BE_TEST_PATTERN:=}

# The backend unit tests currently do not work when HEAPCHECK is enabled.
export HEAPCHECK=

BE_TEST_ARGS=""
if [[ -n "$SKIP_BE_TEST_PATTERN" ]]; then
  BE_TEST_ARGS="-E ${SKIP_BE_TEST_PATTERN}"
fi

cd ${IMPALA_BE_DIR}
. ${IMPALA_HOME}/bin/set-classpath.sh

make test ARGS="${BE_TEST_ARGS}"
