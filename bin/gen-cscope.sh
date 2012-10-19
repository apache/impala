#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

# Generate list of files for Cscope to index
cd $IMPALA_HOME
find . -regex '.*\.\(cc\|c\|hh\|h\|java\)$' > cscope.files

