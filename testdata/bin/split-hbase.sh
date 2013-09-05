#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Split hbasealltypesagg and hbasealltypessmall and assign their splits
set -u

cd $IMPALA_HOME/testdata
mvn clean package

. ${IMPALA_HOME}/bin/set-classpath.sh
export CLASSPATH=$IMPALA_HOME/testdata/target/impala-testdata-0.1-SNAPSHOT.jar:$CLASSPATH

RESULT=1
RETRY_COUNT=0
while [ $RESULT -ne 0 ] && [ $RETRY_COUNT -le 10 ]; do
  java com.cloudera.impala.datagenerator.HBaseTestDataRegionAssigment \
      functional_hbase.alltypesagg functional_hbase.alltypessmall
  RESULT=$?

  if [ $RESULT -ne 0 ]; then
    ((RETRY_COUNT++))
    # If the split failed, force reload the hbase tables before trying the next split
    $IMPALA_HOME/bin/start-impala-cluster.py
    $IMPALA_HOME/bin/load-data.py -w functional-query \
        --table_names=alltypesagg,alltypessmall --table_formats=hbase/none --force
  fi
done

exit $RESULT
