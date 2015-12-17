#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

if [[ $# -eq 1 && "$1" == -format ]]; then
  SHOULD_FORMAT=true
elif [[ $# -ne 0 ]]; then
  echo "Usage: $0 [-format]"
  echo "[-format] : Format the mini-dfs cluster before starting"
  exit 1
else
  SHOULD_FORMAT=false
fi

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh

if $SHOULD_FORMAT; then
  $IMPALA_HOME/testdata/cluster/admin delete_data
fi

$IMPALA_HOME/testdata/cluster/admin start_cluster
