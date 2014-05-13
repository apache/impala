#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
set -u
. ${IMPALA_HOME}/bin/set-classpath.sh

# First kill any running instances of the service.
$IMPALA_HOME/testdata/bin/kill-sentry-service.sh

# Start the service
SENTRY_SERVICE_CONFIG=$IMPALA_HOME/fe/src/test/resources/sentry-site.xml
java -cp $CLASSPATH com.cloudera.impala.testutil.SentryServiceWrapper \
    --config_file "${SENTRY_SERVICE_CONFIG}" &
