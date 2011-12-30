#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# run backend tests For some reason this does not work on Jenkins
cd $IMPALA_FE_DIR
mvn exec:java -Dexec.mainClass=com.cloudera.impala.testutil.PlanService \
            -Dexec.classpathScope=test & 
PID=$!
# Wait for planner to startup TODO: can we do something better than wait arbitrarily for
# 3 seconds.  Not a huge deal if it's not long enough, BE tests will just wait a bit
sleep 3
cd $IMPALA_HOME
make test
kill $PID
