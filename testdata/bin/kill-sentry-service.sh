#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

jps -m | grep org.apache.sentry.SentryMain | awk '{print $1}' | xargs kill -9;
sleep 1;
