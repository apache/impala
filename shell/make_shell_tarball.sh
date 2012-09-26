#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# This script makes a tarball of the Python-based shell that can be unzipped and
# run out-of-the-box with no configuration. The final tarball is left in
# ${IMPALA_HOME}/shell/build. 

if [ "x${IMPALA_HOME}" == "x" ]; then
  echo "\$IMPALA_HOME must be set"
  exit 1
fi

SHELL_HOME=${IMPALA_HOME}/shell
BUILD_DIR=${SHELL_HOME}/build

echo "Deleting all files in ${BUILD_DIR}/{gen-py,lib}"
rm -rf ${BUILD_DIR}/lib/* 2>&1 > /dev/null
rm -rf ${BUILD_DIR}/gen-py/* 2>&1 > /dev/null
mkdir -p ${BUILD_DIR}/lib

# Copy all the shell files into the build dir
cp -r ${HIVE_HOME}/lib/py/* ${BUILD_DIR}/lib
cp -r ${IMPALA_HOME}/thirdparty/python-thrift-0.7.0/thrift ${BUILD_DIR}/lib
cp -r ${SHELL_HOME}/gen-py ${BUILD_DIR}
cp ${SHELL_HOME}/impala-shell ${BUILD_DIR}
cp ${SHELL_HOME}/impala_shell.py ${BUILD_DIR}

GIT_HASH=$(git rev-parse HEAD)
BUILD_DATE=$(date)
rm -f ${BUILD_DIR}/lib/impala_build_version.py

cat > ${BUILD_DIR}/lib/impala_build_version.py <<EOF
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Impala version string

def get_version_string():
  return "${GIT_HASH}"

def get_build_date():
  return "${BUILD_DATE}"
EOF

pushd ${BUILD_DIR} > /dev/null
echo "Making tarball in ${BUILD_DIR}"
tar czf ${BUILD_DIR}/impala-shell-0.1.tar.gz ./lib ./gen-py \
  ./impala-shell ./impala_shell.py --exclude="*.pyc" || popd 2>&1 > /dev/null
