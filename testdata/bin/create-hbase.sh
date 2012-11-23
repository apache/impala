#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# To work around the HBase bug (HBASE-4467), unset $HADOOP_HOME before calling hbase
HADOOP_HOME=

# create the HBase tables
yes exit | $HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/bin/create.hbase

# load the HBase data
yes exit | $HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/HBaseAllTypesError/hbasealltypeserror.hbase
yes exit | $HBASE_HOME/bin/hbase shell $IMPALA_HOME/testdata/HBaseAllTypesErrorNoNulls/hbasealltypeserrornonulls.hbase
