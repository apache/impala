#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

hadoop jar ${HADOOP_LZO}/build/hadoop-lzo-0.4.15.jar  com.hadoop.compression.lzo.DistributedLzoIndexer $*
