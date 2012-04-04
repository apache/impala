#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

set -e

echo "Copying data files from the filer.  If the file already exists locally, the files will not be copied.  It's not check summing the files or anything like that, if you need to force a copy, delete the local directory: impala/testdata/data/hive_benchmark"

# TODO: this should be moved somewhere more reasonable.
DATASRC="haus01.sf.cloudera.com:/home/nong/impala-data"
DATADST=$IMPALA_HOME/testdata/hive_benchmark
mkdir -p $DATADST

scp -r $DATASRC/* $DATADST/
