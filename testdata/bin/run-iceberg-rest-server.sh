#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cd $IMPALA_HOME
. bin/impala-config.sh

CP_FILE="$IMPALA_HOME/java/iceberg-rest-catalog-test/target/build-classpath.txt"

if [ ! -s "$CP_FILE" ]; then
  >&2 echo Iceberg REST Catalog classpath file $CP_FILE missing.
  >&2 echo Build java/iceberg-rest-catalog-test first.
  return 1
fi

CLASSPATH=$(cat $CP_FILE):"$CLASSPATH"

$JAVA -cp java/iceberg-rest-catalog-test/target/impala-iceberg-rest-catalog-test-${IMPALA_VERSION}.jar:$CLASSPATH \
    org.apache.iceberg.rest.IcebergRestCatalogTest $@

