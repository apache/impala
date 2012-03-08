#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-all.sh;

# Start up DFS, then Hbase
$IMPALA_HOME/testdata/bin/run-mini-dfs.sh;
$IMPALA_HOME/testdata/bin/run-hbase.sh;

