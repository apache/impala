#!/bin/sh
CLASSPATH=\
$IMPALA_HOME/fe/src/test/resources:\
$IMPALA_HOME/fe/target/classes:\
$IMPALA_HOME/fe/target/dependency:\
$IMPALA_HOME/fe/target/test-classes:\
$IMPALA_HOME/thirdparty/hive-0.8.0-cdh4b1/lib/datanucleus-core-2.0.3.jar:\
$IMPALA_HOME/thirdparty/hive-0.8.0-cdh4b1/lib/datanucleus-enhancer-2.0.3.jar:\
$IMPALA_HOME/thirdparty/hive-0.8.0-cdh4b1/lib/datanucleus-rdbms-2.0.3.jar:\
$IMPALA_HOME/thirdparty/hive-0.8.0-cdh4b1/lib/datanucleus-connectionpool-2.0.3.jar

export CLASSPATH
$IMPALA_HOME/be/build/debug/service/impalad $@
