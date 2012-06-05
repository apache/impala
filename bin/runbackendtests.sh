#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cd $IMPALA_FE_DIR
mvn exec:java -Dexec.mainClass=com.cloudera.impala.testutil.PlanService \
            -Dexec.classpathScope=test &
PID=$!
cd $IMPALA_HOME
make test
kill $PID
