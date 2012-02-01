#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
ps a | grep java | grep MiniHadoopClusterManager | awk '{print $1}' | xargs kill -9
sleep 2
