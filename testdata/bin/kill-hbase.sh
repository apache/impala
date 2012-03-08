#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# kill region server first, then hmaster, zookeeper
jps | grep HRegionServer | awk '{print $1}' | xargs kill -9;
jps | grep HMaster | awk '{print $1}' | xargs kill -9;
jps | grep HQuorumPeer | awk '{print $1}' | xargs kill -9;
sleep 2;

# clear up data so that zookeeper/hbase won't do recovery when it starts
rm -rf /tmp/hbase-*;
