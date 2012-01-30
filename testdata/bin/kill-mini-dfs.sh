#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
jps | grep MiniHadoopClusterManager | awk '{print $1}' | xargs kill -9
