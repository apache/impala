#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

set -e

hive_args="-hiveconf hive.root.logger=WARN,console -v -f"
cmd="$HIVE_HOME/bin/hive $hive_args $IMPALA_HOME/testdata/bin/create.sql"
$cmd
cmd="$HIVE_HOME/bin/hive $hive_args $IMPALA_HOME/testdata/bin/load.sql"
$cmd
