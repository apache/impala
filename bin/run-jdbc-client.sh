#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

export CLASSPATH=`hadoop classpath`
java -cp $HIVE_JDBC_DRIVER_CLASSPATH:$IMPALA_FE_DIR/target/test-classes \
    com.cloudera.impala.testutil.ImpalaJdbcClient "$@"
