#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script makes a tarball of the Python-based tests and benchmark scripts
# that can be unpacked and run out-of-the-box. The final tarball is left in
# ${IMPALA_HOME}/tests/build. Assumes Impala and /thirdparty have been built
# built prior to running this script.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

if [ "x${IMPALA_HOME}" == "x" ]; then
  echo "\$IMPALA_HOME must be set"
  exit 1
fi

# Parse the version info
IMPALA_VERSION_INFO_FILE=${IMPALA_HOME}/bin/version.info

if [ ! -f ${IMPALA_VERSION_INFO_FILE} ]; then
  echo "No version.info file found. Generating new version info"
  ${IMPALA_HOME}/bin/save-version.sh
else
  echo "Using existing version.info file."
fi

VERSION=$(grep "VERSION: " ${IMPALA_VERSION_INFO_FILE} | awk '{print $2}')
cat ${IMPALA_VERSION_INFO_FILE}

TMP_ROOT_DIR=$(mktemp -dt "impala_test_tmp.XXXXXX")
TARBALL_ROOT=${TMP_ROOT_DIR}/impala-tests-${VERSION}
OUTPUT_DIR=${IMPALA_HOME}/tests/build

echo "Root of temp output dir: ${TMP_ROOT_DIR}"

echo "Creating required directories"
mkdir -p ${OUTPUT_DIR}
mkdir -p ${TARBALL_ROOT}/bin
mkdir -p ${TARBALL_ROOT}/testdata
mkdir -p ${TARBALL_ROOT}/thirdparty

echo "Generating environment setup script"
cat > ${TARBALL_ROOT}/bin/set-env.sh <<EOF
# Copyright 2014 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ -z \${IMPALA_HOME} ]; then
  export IMPALA_HOME=\`pwd\`
fi

export IMPALA_WORKLOAD_DIR=\${IMPALA_HOME}/testdata/workloads
export IMPALA_DATASET_DIR=\${IMPALA_HOME}/testdata/datasets
export THRIFT_HOME=\${IMPALA_HOME}/thirdparty/thrift-${IMPALA_THRIFT_VERSION}

# Set these based on your cluster environment
# export HIVE_HOME=/usr/lib/hive/
# export HIVE_CONF_DIR=/etc/hive/conf
# export HADOOP_HOME=/usr/lib/hadoop/
# export HADOOP_CONF_DIR=/etc/hadoop/conf
# export HBASE_HOME=/usr/lib/hbase/
# export HBASE_CONF_DIR=/etc/hbase/conf
# export PATH=$HBASE_HOME/bin:$PATH

# Build the Python path
. \${IMPALA_HOME}/bin/set-pythonpath.sh

echo IMPALA_HOME=\${IMPALA_HOME}
echo PYTHONPATH=\${PYTHONPATH}
echo THRIFT_HOME=\${THRIFT_HOME}
EOF

echo "Copying required files and dependencies"
cp ${IMPALA_VERSION_INFO_FILE} ${TARBALL_ROOT}/bin/
cp ${IMPALA_HOME}/bin/load-data.py ${TARBALL_ROOT}/bin/
cp ${IMPALA_HOME}/bin/run-workload.py ${TARBALL_ROOT}/bin/
cp ${IMPALA_HOME}/bin/set-pythonpath.sh ${TARBALL_ROOT}/bin/
cp -a ${IMPALA_HOME}/testdata/workloads/ ${TARBALL_ROOT}/testdata/
cp -a ${IMPALA_HOME}/testdata/datasets/ ${TARBALL_ROOT}/testdata/
cp -a ${IMPALA_HOME}/testdata/bin/ ${TARBALL_ROOT}/testdata/
cp -a ${IMPALA_HOME}/testdata/avro_schema_resolution/ ${TARBALL_ROOT}/testdata/
cp -a ${IMPALA_HOME}/tests/ ${TARBALL_ROOT}/
cp -a ${IMPALA_HOME}/shell/ ${TARBALL_ROOT}/

# Bundle thrift
cp -a ${THRIFT_HOME} ${TARBALL_ROOT}/thirdparty/thrift-${IMPALA_THRIFT_VERSION}

echo "Making tarball in ${TMP_ROOT_DIR} and copying to: ${OUTPUT_DIR}"
pushd ${TMP_ROOT_DIR} 2>&1 > /dev/null
# Exclude dirs that are not needed.
rm -rf ${TMP_ROOT_DIR}/impala-tests-${VERSION}/tests/results/*
rm -rf ${TMP_ROOT_DIR}/impala-tests-${VERSION}/tests/build/*
tar czf ${TMP_ROOT_DIR}/impala-tests-${VERSION}.tar.gz ./impala-tests-${VERSION}/\
    --exclude="*.pyc" || popd 2>&1 > /dev/null
cp ${TMP_ROOT_DIR}/impala-tests-${VERSION}.tar.gz ${OUTPUT_DIR}

echo "Cleaning up ${TMP_ROOT_DIR}"
rm -rf ${TMP_ROOT_DIR}
