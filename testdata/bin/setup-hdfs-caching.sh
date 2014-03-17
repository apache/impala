#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#

# Make a pool and cache some of the tables in memory.
hdfs cacheadmin -removePool testPool

set -e
hdfs cacheadmin -addPool testPool
