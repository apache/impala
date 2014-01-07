#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -e

# Kill and clean data for a clean start.
$IMPALA_HOME/testdata/bin/kill-mini-dfs.sh

if [ "$1" == "-format" ]; then
  $IMPALA_HOME/testdata/cluster/admin delete_data
elif [[ $1 ]]; then
  echo "Usage: $0 [-format]"
  echo "[-format] : Format the mini-dfs cluster before starting"
  exit 1
fi

$IMPALA_HOME/testdata/cluster/admin start_cluster
