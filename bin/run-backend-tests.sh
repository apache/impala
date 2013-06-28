#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# The backend unit tests currently do not work when HEAPCHECK is enabled.
export HEAPCHECK=
set -e
set -u

cd ${IMPALA_BE_DIR}
. ${IMPALA_HOME}/bin/set-classpath.sh
make test
