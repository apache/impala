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
#
# This script download the Impala jdbc driver and copy it to Hadoop FS.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

. ${IMPALA_HOME}/bin/impala-config.sh > /dev/null 2>&1

EXT_DATA_SOURCES_HDFS_PATH=${FILESYSTEM_PREFIX}/test-warehouse/data-sources
JDBC_DRIVERS_HDFS_PATH=${EXT_DATA_SOURCES_HDFS_PATH}/jdbc-drivers
SIMBA_DRIVER_ZIP_FILENAME=ClouderaImpala_JDBC${IMPALA_SIMBA_JDBC_DRIVER_VERSION}
INNER_SIMBA_DRIVER_ZIP_FILENAME=ClouderaImpalaJDBC${IMPALA_SIMBA_JDBC_DRIVER_VERSION}
DRIVER_JAR_VERSION=${IMPALA_SIMBA_JDBC_DRIVER_VERSION%-*}
SIMBA_DRIVER_JAR_FILENAME=ImpalaJDBC${DRIVER_JAR_VERSION}.jar

found=$(hadoop fs -find ${JDBC_DRIVERS_HDFS_PATH} -name ${SIMBA_DRIVER_JAR_FILENAME})
if [ ! -z "$found" ]; then
  echo "JDBC driver jar file already exists"
  exit 0
fi

hadoop fs -mkdir -p ${JDBC_DRIVERS_HDFS_PATH}
pushd /tmp

mkdir -p impala_jdbc_driver
cd impala_jdbc_driver

# Download Impala jdbc driver.
wget "https://downloads.cloudera.com/connectors/${SIMBA_DRIVER_ZIP_FILENAME}.zip"

# Use Python modules to unzip zip file since 'unzip' command is not available in some
# testing environments.
cat > unzip.py <<__EOT__
import sys
from zipfile import PyZipFile
pzf = PyZipFile(sys.argv[1])
pzf.extractall()
__EOT__

# Extract driver jar file from zip file.
python ./unzip.py ${SIMBA_DRIVER_ZIP_FILENAME}.zip
python ./unzip.py ${SIMBA_DRIVER_ZIP_FILENAME}/${INNER_SIMBA_DRIVER_ZIP_FILENAME}.zip

# Copy driver jar file to Hadoop FS.
hadoop fs -put -f /tmp/impala_jdbc_driver/${SIMBA_DRIVER_JAR_FILENAME} \
    ${JDBC_DRIVERS_HDFS_PATH}/${SIMBA_DRIVER_JAR_FILENAME}

echo "Copied ${SIMBA_DRIVER_JAR_FILENAME} into HDFS ${JDBC_DRIVERS_HDFS_PATH}"

cd ..
rm -rf impala_jdbc_driver
popd

