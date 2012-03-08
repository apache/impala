#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# kill HBase, then DFS
$IMPALA_HOME/testdata/bin/kill-hbase.sh
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh
