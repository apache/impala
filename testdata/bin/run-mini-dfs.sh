#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Starts up a three-node single-process cluster; the NN listens on port 20000.

./kill-mini-dfs.sh > /dev/null 2>&1

pushd ${HADOOP_HOME}
CONF_FILE=`pwd`/core-site.xml
rm -f ${CONF_FILE} > /dev/null 2>&1
CLASSPATH=./*:./lib/jsp-2.1/*:./lib/* java org.apache.hadoop.test.MiniHadoopClusterManager -datanodes 3 -nomr -nnport=20500 -writeConfig ${CONF_FILE} &

for i in `seq 30`; do
  sleep 2
  echo "Looking for ${CONF_FILE}"
  if [ -e ${CONF_FILE} ]; then
    echo "Found ${CONF_FILE}"
    exit
  fi;
done;
