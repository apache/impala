#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# kill DFS
jps | grep MiniHadoopClusterManager | awk '{print $1}' | xargs kill -9;
sleep 2;

# clear up dfs data to avoid recovery when it starts
rm -rf /tmp/hadoop-*;
