#!/usr/bin/env bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh

# location of the generated data
DATALOC=$IMPALA_HOME/testdata/target

# regenerate the test data generator
cd $IMPALA_HOME/testdata
mvn clean
# on jenkins runs, resolve dependencies quietly to avoid log spew
if [ "${USER}" == "jenkins" ]; then
  echo "Quietly resolving testdata dependencies."
  mvn -q dependency:resolve
fi
mvn package

# find jars
CP=""
JARS=`find target/*.jar 2> /dev/null || true`
for i in $JARS; do
    if [ -n "$CP" ]; then
        CP=${CP}:${i}
    else
        CP=${i}
    fi
done

# run test data generator
echo $DATALOC
mkdir -p $DATALOC
"$JAVA" -cp $CP com.cloudera.impala.datagenerator.TestDataGenerator $DATALOC
"$JAVA" -cp $CP com.cloudera.impala.datagenerator.CsvToHBaseConverter
echo "SUCCESS, data generated into $DATALOC"
