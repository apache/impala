#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
HIVE_SERVER_PORT=10000
export HIVE_SERVER2_THRIFT_PORT=11050
HIVE_METASTORE_PORT=9083
set -u

# TODO: We should have a retry loop for every service we start.
# Kill for a clean start.
$IMPALA_HOME/testdata/bin/kill-hive-server.sh

# Starts a Hive Metastore Server on the specified port.
HADOOP_CLIENT_OPTS=-Xmx2024m hive --service metastore -p $HIVE_METASTORE_PORT &

# Starts a HiveServer2 instance on the port specified by the HIVE_SERVER2_THRIFT_PORT
# environment variable.
hive --service hiveserver2 &

sleep 10

$IMPALA_HOME/testdata/bin/avoid_hive_replication_bug.sh
