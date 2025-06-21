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

set -e

VERSION=${IMPALA_VERSION%%-*}
PKG_NAME=impala-udf-devel
TOPDIR=${IMPALA_HOME}/${PKG_NAME}-rpmbuild
SRC_DIR=${IMPALA_HOME}/${PKG_NAME}-${VERSION}
INCLUDE_DIR=${SRC_DIR}/usr/include/impala_udf
LIB_DIR=${SRC_DIR}/usr/lib64
SPEC_FILE=impala-udf-devel.spec

rm -rf "$SRC_DIR"
mkdir -p "$INCLUDE_DIR" "$LIB_DIR"

cp be/src/udf/uda-test-harness-impl.h \
  be/src/udf/uda-test-harness.h \
  be/src/udf/udf-debug.h \
  be/src/udf/udf-test-harness.h \
  be/src/udf/udf.h "$INCLUDE_DIR/"

# UDF SDK uses impala_udf namespace to avoid namespace collision
sed -i 's|#include "udf/\(.*\)"|#include <impala_udf/\1>|' ${INCLUDE_DIR}/*.h

SO_ORIG=$(find be/build/release/udf -name libImpalaUdf.a | head -n1)
if [ -z "$SO_ORIG" ]; then
  echo "be/build/release/udf/libImpalaUdf.a not found."
  echo "Build with ./buildall.sh -release_and_debug -notests -udf_devel_package."
  exit 1
fi

cp "$SO_ORIG" "$LIB_DIR/libImpalaUdf-retail.a"

SO_ORIG=$(find be/build/debug/udf -name libImpalaUdf.a | head -n1)
if [ -z "$SO_ORIG" ]; then
  echo "be/build/debug/udf/libImpalaUdf.a not found."
  echo "Build with ./buildall.sh -release_and_debug -notests -udf_devel_package."
  exit 1
fi

cp "$SO_ORIG" "$LIB_DIR/libImpalaUdf-debug.a"

cd ~
mkdir -p "$TOPDIR"/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
tar czf "$TOPDIR/SOURCES/${PKG_NAME}-${VERSION}.tar.gz" -C \
  "${IMPALA_HOME}" "${PKG_NAME}-${VERSION}"

echo "Copying .spec file..."
cp "${IMPALA_HOME}/bin/$SPEC_FILE" "$TOPDIR/SPECS/"

echo "Building RPM..."
rpmbuild -ba "$TOPDIR/SPECS/$SPEC_FILE" --define "_topdir $TOPDIR" \
  --define "version ${VERSION}"

echo "Done. RPM available at:"
find "$TOPDIR/RPMS" -name "${PKG_NAME}*.rpm"

