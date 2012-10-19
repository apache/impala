#!/bin/sh
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# This script explicitly sets the CLASSPATH for embedded JVMs (e.g. in
# Impalad or in runquery) Because embedded JVMs do not honour
# CLASSPATH wildcard expansion, we have to add every dependency jar
# explicitly to the CLASSPATH.

CLASSPATH=\
$IMPALA_HOME/fe/src/test/resources:\
$IMPALA_HOME/fe/target/classes:\
$IMPALA_HOME/fe/target/dependency:\
$IMPALA_HOME/fe/target/test-classes:\
${HIVE_HOME}/lib/datanucleus-core-2.0.3.jar:\
${HIVE_HOME}/lib/datanucleus-enhancer-2.0.3.jar:\
${HIVE_HOME}/lib/datanucleus-rdbms-2.0.3.jar:\
${HIVE_HOME}/lib/datanucleus-connectionpool-2.0.3.jar

for jar in `ls ${IMPALA_HOME}/fe/target/dependency/*.jar`; do
  CLASSPATH=${CLASSPATH}:$jar
done

export CLASSPATH
