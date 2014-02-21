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

# Wait for the Metastore to come up because HiveServer2 relies on it being live.
python $IMPALA_HOME/testdata/bin/wait-for-metastore.py

# Starts a HiveServer2 instance on the port specified by the HIVE_SERVER2_THRIFT_PORT
# environment variable.
hive --service hiveserver2 &

# Wait for the HiveServer2 service to come up because callers of this script
# may rely on it being available.
python $IMPALA_HOME/testdata/bin/wait-for-hiveserver2.py --transport=plain_sasl

$IMPALA_HOME/testdata/bin/avoid_hive_replication_bug.sh
