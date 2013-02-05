#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

JDBC_CP=$HIVE_JDBC_DRIVER_CLASSPATH
JDBC_CP=$JDBC_CP:$IMPALA_FE_DIR/target/test-classes
JDBC_CP=$JDBC_CP:$IMPALA_FE_DIR/target/dependency/*
java -cp $JDBC_CP com.cloudera.impala.testutil.ImpalaJdbcClient "$@"
