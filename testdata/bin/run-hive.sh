#!/bin/bash

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

CONF_DIR=$1
SCRIPT=$2
WH=$3
MS_URL=$4
MS_DRIVER=$5
MS_USERNAME=$6
MS_PASSWORD=$7

HIVE="hive "
HIVE="$HIVE -hiveconf \"test.hive.warehouse.dir=$WH\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.url=$MS_URL\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.driver=$MS_DRIVER\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.username=$MS_USERNAME\""
HIVE="$HIVE -hiveconf \"test.hive.metastore.jdbc.password=$MS_PASSWORD\""

HIVE_CONF_DIR=$CONF_DIR
export HIVE_CONF_DIR
$HIVE -f $SCRIPT -v
