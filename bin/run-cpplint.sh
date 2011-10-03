#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

files=`find $IMPALA_BE_DIR/src -regex '.*\(cc\|h\)' -printf '%p '`
cpplint.py $@ ${files}
