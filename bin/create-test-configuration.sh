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
#
# Create the test environment needed by Impala. Includes generation of the
# Hadoop config files: core-site.xml, hbase-site.xml, hive-site.xml as well
# as creation of the Hive metastore.

set -e
CREATE_METASTORE=0

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -create_metastore)
      CREATE_METASTORE=1
      ;;
    -help|*)
      echo "[-create_metastore] : If true, creates a new metastore."
      exit 1
      ;;
  esac
done

# If a specific metastore db is defined, use that. Otherwise create unique metastore
# DB name based on the current directory.
if [ -z "${METASTORE_DB}" ]; then
  METASTORE_DB=`basename ${IMPALA_HOME} | sed -e "s/\\./_/g" | sed -e "s/[.-]/_/g"`
fi

set -u

# Convert Metastore DB name to be lowercase
export METASTORE_DB=`echo $METASTORE_DB | tr '[A-Z]' '[a-z]'`
export CURRENT_USER=`whoami`

CONFIG_DIR=${IMPALA_HOME}/fe/src/test/resources
echo "Config dir: ${CONFIG_DIR}"
echo "Current user: ${CURRENT_USER}"
echo "Metastore DB: hive_${METASTORE_DB}"

pushd ${CONFIG_DIR}
# Cleanup any existing files
rm -f {core,hbase,hive}-site.xml
rm -f authz-provider.ini

# TODO: Throw an error if the template references an undefined environment variable
if [ $CREATE_METASTORE -eq 1 ]; then
  echo "Creating postgresql database for Hive metastore"
  set +o errexit
  dropdb -U hiveuser hive_$METASTORE_DB
  set -e
  createdb -U hiveuser hive_$METASTORE_DB

  psql -U hiveuser -d hive_$METASTORE_DB \
       -f ${HIVE_HOME}/scripts/metastore/upgrade/postgres/hive-schema-0.10.0.postgres.sql
fi

function generate_config {
  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' $1 > $2
}

echo "Generating hive-site.xml using postgresql for metastore"
generate_config postgresql-hive-site.xml.template hive-site.xml

echo "Generating hbase-site.xml"
generate_config hbase-site.xml.template hbase-site.xml

echo "Generating core-site.xml"
# Update dfs.block.local-path-access.user with the current user
generate_config core-site.xml.template core-site.xml

echo "Generating authorization policy file"
generate_config authz-policy.ini.template authz-policy.ini
popd

echo "Completed config generation"

# Creates a symlink in TARGET_DIR to all subdirectories under SOURCE_DIR
function symlink_subdirs {
  SOURCE_DIR=$1
  TARGET_DIR=$2
  if [ -d "${SOURCE_DIR}" ]; then
    find ${SOURCE_DIR}/ -maxdepth 1 -mindepth 1 -type d -exec ln -f -s {} ${TARGET_DIR} \;
  else
    echo "No auxiliary tests found at: ${SOURCE_DIR}"
  fi
}

# The Impala test framework support running additional tests outside of the main repo.
# This is an optional feature that can be enabled by setting the IMPALA_AUX_* environment
# variables to valid locations.
echo "Searching for auxiliary tests, workloads, and datasets (if any exist)."
symlink_subdirs ${IMPALA_AUX_WORKLOAD_DIR} ${IMPALA_WORKLOAD_DIR}
symlink_subdirs ${IMPALA_AUX_DATASET_DIR} ${IMPALA_DATASET_DIR}
symlink_subdirs ${IMPALA_AUX_TEST_HOME}/tests ${IMPALA_HOME}/tests
