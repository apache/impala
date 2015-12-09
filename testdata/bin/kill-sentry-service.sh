#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

DIR=$(dirname "$0")
echo Stopping Sentry
"$DIR"/kill-java-service.sh -c org.apache.sentry.SentryMain
