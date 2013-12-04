#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# kill MiniLlama
jps | grep MiniLlama | awk '{print $1}' | xargs kill -9;
sleep 2;
