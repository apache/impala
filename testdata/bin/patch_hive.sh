#!/usr/bin/env bash
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

# This script is used to repair service startup and task running problems that occur when
# Apache Hive 3 is integrated.
# Repair method:
# - Replace jar
# - Backport the patch (Hive 3.x has not been released for a long time, apply the patch
# as a transitional solution until the new version is released).

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

if [[ "${USE_APACHE_HIVE}" != true ]]; then
  exit 0
fi

# Rebuild tag
HIVE_REBUILD=${HIVE_REBUILD-false}
# Cache applied patches
PATCHED_CACHE_FILE="$HIVE_SRC_DIR/.patched"
if [ ! -f "$PATCHED_CACHE_FILE" ]; then touch "$PATCHED_CACHE_FILE"; fi
# Apache Hive patch dir
HIVE_PARCH_DIR="${IMPALA_HOME}/testdata/cluster/hive"

# Apply the patch and save the patch name to .patched
function apply_patch {
  p="$1"
  status=1
  while IFS= read -r line
  do
    if [ "$line" == "$p" ]; then
      status=0
      break
    fi
  done < $PATCHED_CACHE_FILE
  if [ $status = "1" ] ;then
    echo "Apply patch: $p"
    patch -p1 < ${HIVE_PARCH_DIR}/$p
    echo $p >> $PATCHED_CACHE_FILE
    HIVE_REBUILD=true
  fi
}

# 1. Fix HIVE-22915
echo "Fix HIVE-22915"
rm $HIVE_HOME/lib/guava-*jar
cp $HADOOP_HOME/share/hadoop/hdfs/lib/guava-*.jar $HIVE_HOME/lib/

# 2. Apply patches
pushd "$HIVE_SRC_DIR"
for file in `ls ${HIVE_PARCH_DIR}/patch*.diff | sort`
do
  p=$(basename $file)
  apply_patch $p
done

# 3. Repackage the hive submodules affected by the patch
if [[ "${HIVE_REBUILD}" = "true" ]]; then
  echo "Repackage the hive-exec module"
  ${IMPALA_HOME}/bin/mvn-quiet.sh -pl ql clean package -Dmaven.test.skip
  cp $HIVE_SRC_DIR/ql/target/hive-exec-${APACHE_HIVE_VERSION}.jar $HIVE_HOME/lib/
fi
popd
