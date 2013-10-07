#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
HIVE_SERVER_PORT=10000
export HIVE_SERVER2_THRIFT_PORT=11050
HIVE_METASTORE_PORT=9083
set -u

# Kill for a clean start.
$IMPALA_HOME/testdata/bin/kill-hive-server.sh

# Starts a Hive Metastore Server on the specified port.
hive --service metastore -p $HIVE_METASTORE_PORT &

# Starts a HiveServer2 instance on the port specified by the HIVE_SERVER2_THRIFT_PORT
# environment variable.
hive --service hiveserver2 &

# Starts hive-server (1) on the specified port.
hive --service hiveserver -p $HIVE_SERVER_PORT &
sleep 5
