#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

DIR=$(dirname "$0")
echo Stopping Hbase
# Kill region server first, then hmaster, and zookeeper.
"$DIR"/kill-java-service.sh -c HRegionServer -c HMaster -c HQuorumPeer -s 2

# Clear up data so that zookeeper/hbase won't do recovery when it starts.
rm -rf /tmp/hbase-*
