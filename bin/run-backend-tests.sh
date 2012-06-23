#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

cd ${IMPALA_BE_DIR}
. ${IMPALA_HOME}/bin/set-classpath.sh  
make test

