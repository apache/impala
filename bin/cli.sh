#!/usr/bin/env bash
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

export CLASSPATH=${HIVE_CONF_DIR}:${IMPALA_FE_DIR}/target:${IMPALA_FE_DIR}/target/dependency

EXT_DIRS=${HIVE_CONF_DIR}:${IMPALA_FE_DIR}/target:${IMPALA_FE_DIR}/target/dependency

java -Dtest.hive.warehouse.dir=${IMPALA_FE_DIR}/fe/target/test-warehouse \
     -Dtest.hive.metastore.jdbc.url=${IMPALA_METASTORE_DB_URL} \
     -Dtest.hive.metastore.jdbc.driver=org.apache.derby.jdbc.EmbeddedDriver \
     -Dtest.hive.metastore.jdbc.username=APP -Dtest.hive.metastore.jdbc.password=mine \
     -Djava.library.path=${IMPALA_BE_DIR}/build/service \
     -Djava.ext.dirs=${EXT_DIRS} \
     -jar $IMPALA_FE_DIR/lib/sqlline-1_0_2.jar \
     $IMPALA_HOME/bin/impala-cli.properties

