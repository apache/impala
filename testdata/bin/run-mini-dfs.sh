#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# Runs a three-node single-process cluster and loads it with data from
# AllTypesAgg in /impala-dist-test
# A HDFS conf file for the cluster is written to ${CONF_FILE} for
# later use

CONF_FILE=`pwd`/core-site.xml
HDFS_DATA_DIR=/impala-dist-test

./kill-mini-dfs.sh > /dev/null 2>&1

rm -f ${CONF_FILE} > /dev/null 2>&1

pushd ${HADOOP_HOME}
CLASSPATH=./*:./lib/jsp-2.1/*:./lib/* java org.apache.hadoop.test.MiniHadoopClusterManager -datanodes 3 -nomr -writeConfig ${CONF_FILE} &

for i in `seq 30`; do
  sleep 2
  echo "Looking for ${CONF_FILE}"
  if [ -e ${CONF_FILE} ]; then
    echo "MiniDFSCluster started, loading data.";
    hadoop fs -conf ${CONF_FILE} -rmr ${HDFS_DATA_DIR}
    hadoop fs -conf ${CONF_FILE} -mkdir ${HDFS_DATA_DIR}
    hadoop fs -conf ${CONF_FILE} -Ddfs.replication=3 -put ${IMPALA_HOME}/testdata/target/AllTypesAgg/* ${HDFS_DATA_DIR}
    echo "Data loaded. Config file written to: ${CONF_FILE}"
    popd
    HADOOP_CONF_DIR=./ ${HIVE_HOME}/bin/hive -v -f ${IMPALA_HOME}/testdata/bin/create-mini.sql
    echo "Created table in hive"
    exit
  fi;
done;

echo "MiniDFSCluster did not come up"
exit 1
