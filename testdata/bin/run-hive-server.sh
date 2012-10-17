#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
HIVE_SERVER_PORT=10000
set -u

# Kill for a clean start.
$IMPALA_HOME/testdata/bin/kill-hive-server.sh

# Starts hive-server on the specified port
hive --service hiveserver -p $HIVE_SERVER_PORT &
sleep 1
