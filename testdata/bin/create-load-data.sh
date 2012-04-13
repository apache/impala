#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

if [ x${JAVA_HOME} == x ]; then
  echo JAVA_HOME not set
  exit -1
fi

${HIVE_HOME}/bin/hive -hiveconf hive.root.logger=WARN,console -v \
  -f ${IMPALA_HOME}/testdata/bin/create.sql 
if [ $? != 0 ]; then
  echo CREATE FAILED
  exit -1
fi
if [ -d ${IMPALA_HOME}/testdata/data/test-warehouse ] ; then 
  # The data has already been created, just load it.
  ${HIVE_HOME}/bin/hive -hiveconf hive.root.logger=WARN,console -v \
    -f ${IMPALA_HOME}/testdata/bin/load.sql 
  if [ $? != 0 ]; then
    echo LOAD FAILED
    exit -1
  fi
else
  ${HIVE_HOME}/bin/hive -hiveconf hive.root.logger=WARN,console -v \
    -f ${IMPALA_HOME}/testdata/bin/load-raw-data.sql
  if [ $? != 0 ]; then 
    echo RAW DATA LOAD FAILED
    exit -1
  fi
  cd ${IMPALA_HOME}/testdata/data
  hadoop fs -get /test-warehouse
  if [ $? != 0 ]; then 
    echo HADOOP GET FAILED
    exit -1
  fi
fi
