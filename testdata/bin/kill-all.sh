#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# kill HBase, then MiniLlama (which includes a MiniDfs, a Yarn RM several NMs)
$IMPALA_HOME/testdata/bin/kill-sentry-service.sh
$IMPALA_HOME/testdata/bin/kill-hive-server.sh
$IMPALA_HOME/testdata/bin/kill-hbase.sh
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh


# kill all impalad and statestored processes
killall -9 -u $USER impalad
killall -9 -u $USER statestored
killall -9 -u $USER catalogd
killall -9 -u $USER mini-impala-cluster
