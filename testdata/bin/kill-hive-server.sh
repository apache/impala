#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

jps -m | grep HiveServer | awk '{print $1}' | xargs kill -9;
jps -m | grep HiveMetaStore | awk '{print $1}' | xargs kill -9;
sleep 1;
