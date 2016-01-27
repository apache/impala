#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

# Kill HBase, then MiniLlama (which includes a MiniDfs, a Yarn RM several NMs).
$IMPALA_HOME/testdata/bin/kill-sentry-service.sh
$IMPALA_HOME/testdata/bin/kill-hive-server.sh
$IMPALA_HOME/testdata/bin/kill-hbase.sh
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh

for BINARY in impalad statestored catalogd mini-impalad-cluster; do
  if pgrep -U $USER $BINARY; then
    killall -9 -u $USER -q $BINARY
  fi
done
