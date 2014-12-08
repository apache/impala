#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

# To work around the HBase bug (HBASE-4467), unset $HADOOP_HOME before calling hbase
HADOOP_HOME=

# load the HBase data
yes exit | $HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/HBaseAllTypesError/functional_hbase.hbasealltypeserror.hbase
yes exit | $HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/HBaseAllTypesErrorNoNulls/functional_hbase.hbasealltypeserrornonulls.hbase
