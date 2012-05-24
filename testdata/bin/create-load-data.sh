#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

if [ x${JAVA_HOME} == x ]; then
  echo JAVA_HOME not set
  exit 1
fi

set -e

# Load the data set
pushd ${IMPALA_HOME}/bin

./load-data.sh functional exhaustive
if [ $? != 0 ]; then
  echo LOAD OF FUNCTIONAL DATA FAILED
  exit 1
fi

./load-data.sh tpch core
if [ $? != 0 ]; then
  echo LOAD OF TPCH DATA FAILED
  exit 1
fi
popd

# TODO: The multi-format table will move these files. So we need to copy them to a
# temporary location for that table to use. Should find a better way to handle this.
echo COPYING DATA FOR DEPENDENT TABLES
hadoop fs -rm -r -f /test-warehouse/alltypesmixedformat
hadoop fs -rm -r -f /tmp/alltypes_rc
hadoop fs -rm -r -f /tmp/alltypes_seq
hadoop fs -mkdir -p /tmp/alltypes_seq/year=2009
hadoop fs -mkdir -p /tmp/alltypes_rc/year=2009
hadoop fs -cp  /test-warehouse/alltypes_seq/year=2009/month=2/ /tmp/alltypes_seq/year=2009
hadoop fs -cp  /test-warehouse/alltypes_rc/year=2009/month=3/ /tmp/alltypes_rc/year=2009
hadoop fs -cp  /test-warehouse/alltypes_trevni/year=2009/month=4/ \
  /tmp/alltypes_rc/year=2009

# For tables that rely on loading data from local fs test-warehouse
# TODO: Find a good way to integrate this with the normal data loading scripts
${HIVE_HOME}/bin/hive -hiveconf hive.root.logger=WARN,console -v \
  -f ${IMPALA_HOME}/testdata/bin/load-dependent-tables.sql
if [ $? != 0 ]; then
  echo DEPENDENT LOAD FAILED
  exit 1
fi
