#!/bin/bash

if [ "$IMPALA_HOME" == "" ]; then
echo "Error: IMPALA_HOME is not set.";
exit 1;
fi

IMPALA_FE_DIR=${IMPALA_HOME}/fe
IMPALA_BE_DIR=${IMPALA_HOME}/be

export CLASSPATH=${IMPALA_FE_DIR}/src/test/resources:${IMPALA_FE_DIR}/target:${IMPALA_FE_DIR}/target/dependency

EXT_DIRS=${IMPALA_FE_DIR}/src/test/resources:${IMPALA_FE_DIR}/target:${IMPALA_FE_DIR}/target/dependency

java -Dtest.hive.warehouse.dir=${IMPALA_FE_DIR}/fe/target/test-warehouse -Dtest.hive.metastore.jdbc.url="jdbc:derby:;databaseName=${IMPALA_FE_DIR}/target/test_metastore_db;create=true;logDevice=${IMPALA_FE_DIR}/target/test_metastore_db" -Dtest.hive.metastore.jdbc.driver=org.apache.derby.jdbc.EmbeddedDriver -Dtest.hive.metastore.jdbc.username=APP -Dtest.hive.metastore.jdbc.password=mine -Djava.library.path=${IMPALA_BE_DIR}/build/service -Djava.ext.dirs=${EXT_DIRS} -jar ../../../lib/sqlline-1_0_2.jar
