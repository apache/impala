#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Split hbasealltypesagg and hbasealltypessmall and assign their splits
set -e

cd $IMPALA_HOME/testdata
mvn clean package

. ${IMPALA_HOME}/bin/set-classpath.sh
export CLASSPATH=$IMPALA_HOME/testdata/target/impala-testdata-0.1-SNAPSHOT.jar:$CLASSPATH

java com.cloudera.impala.datagenerator.HBaseTestDataRegionAssigment functional_hbase.alltypesagg functional_hbase.alltypessmall
