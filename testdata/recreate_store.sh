#!/bin/sh

set -e

# location of the generated data
DATALOC=target

# regenerate the test data generator
mvn clean package

# run test data generator
mkdir -p $DATALOC
CP=""
JARS=`find target/*.jar 2> /dev/null || true`
for i in $JARS; do
    if [ -n "$CP" ]; then
        CP=${CP}:${i}
    else
        CP=${i}
    fi
done

java -cp $CP com.cloudera.impala.datagenerator.TestDataGenerator $DATALOC

echo "SUCCESS, data generated into $DATALOC"
