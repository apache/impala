#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# kill HBase, then DFS
$IMPALA_HOME/testdata/bin/kill-hbase.sh
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh
$IMPALA_HOME/testdata/bin/kill-hive-server.sh

# kill all impalad and statestored processes
killall -9 impalad
killall -9 statestored
killall -9 mini-impala-cluster
