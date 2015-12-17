#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

DIR=$(dirname "$0")
echo Stopping Hive
"$DIR"/kill-java-service.sh -c HiveServer -c HiveMetaStore
