#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

cd $IMPALA_FE_DIR
mvn -e exec:java -Dexec.mainClass=com.cloudera.impala.testutil.BlockIdGenerator \
              -Dexec.classpathScope=test -Dexec.args="../testdata/block-ids"
