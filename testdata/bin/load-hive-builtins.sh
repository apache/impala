#!/bin/bash

# TODO: remove this once we understand why Hive 0.8.1 looks in HDFS for its builtins jar
${HADOOP_HOME}/bin/hadoop fs -rm -r -f ${HIVE_HOME}/lib/ 
${HADOOP_HOME}/bin/hadoop fs -mkdir ${HIVE_HOME}/lib/ 
${HADOOP_HOME}/bin/hadoop fs -put ${HIVE_HOME}/lib/*builtins*.jar ${HIVE_HOME}/lib/
