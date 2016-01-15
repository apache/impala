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

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

CREATE_METASTORE=0
CREATE_SENTRY_POLICY_DB=0
: ${IMPALA_KERBERIZE=}

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -create_metastore)
      CREATE_METASTORE=1
      ;;
    -create_sentry_policy_db)
      CREATE_SENTRY_POLICY_DB=1
      ;;
    -k|-kerberize|-kerberos|-kerb)
      # This could also come in through the environment...
      export IMPALA_KERBERIZE=1
      ;;
    -help|*)
      echo "[-create_metastore] : If true, creates a new metastore."
      echo "[-create_sentry_policy_db] : If true, creates a new sentry policy db."
      echo "[-kerberize] : Enable kerberos on the cluster"
      exit 1
      ;;
  esac
done

# If this isn't sourced, bad things will always happen
if [ "${IMPALA_CONFIG_SOURCED}" != "1" ]; then
  echo "You must source bin/impala-config.sh"
  exit 1
fi

# If a specific metastore db is defined, use that. Otherwise create unique metastore
# DB name based on the current directory.
: ${METASTORE_DB=`basename ${IMPALA_HOME} | sed -e "s/\\./_/g" | sed -e "s/[.-]/_/g"`}

${CLUSTER_DIR}/admin create_cluster

if [ ! -z "${IMPALA_KERBERIZE}" ]; then
  # Sanity check...
  if ! ${CLUSTER_DIR}/admin is_kerberized; then
    echo "Kerberized cluster not created, even though told to."
    exit 1
  fi

  # Set some more environment variables.
  . ${MINIKDC_ENV}

  # For hive-site.xml further down...
  export HIVE_S2_AUTH=KERBEROS
else
  export HIVE_S2_AUTH=NONE
fi

# Convert Metastore DB name to be lowercase
export METASTORE_DB=`echo $METASTORE_DB | tr '[A-Z]' '[a-z]'`
export CURRENT_USER=`whoami`

CONFIG_DIR=${IMPALA_HOME}/fe/src/test/resources
echo "Config dir: ${CONFIG_DIR}"
echo "Current user: ${CURRENT_USER}"
echo "Metastore DB: hive_${METASTORE_DB}"

pushd ${CONFIG_DIR}
# Cleanup any existing files
rm -f {core,hdfs,hbase,hive,yarn,mapred}-site.xml
rm -f authz-provider.ini

if [ $CREATE_METASTORE -eq 1 ]; then
  echo "Creating postgresql database for Hive metastore"
  dropdb -U hiveuser hive_$METASTORE_DB 2> /dev/null || true
  createdb -U hiveuser hive_$METASTORE_DB

  psql -U hiveuser -d hive_$METASTORE_DB \
       -f ${HIVE_HOME}/scripts/metastore/upgrade/postgres/hive-schema-0.13.0.postgres.sql
  # Increase the size limit of PARAM_VALUE from SERDE_PARAMS table to be able to create
  # HBase tables with large number of columns.
  echo "alter table \"SERDE_PARAMS\" alter column \"PARAM_VALUE\" type character varying" \
      | psql -U hiveuser -d hive_$METASTORE_DB
fi

if [ $CREATE_SENTRY_POLICY_DB -eq 1 ]; then
  echo "Creating Sentry Policy Server DB"
  dropdb -U hiveuser sentry_policy 2> /dev/null || true
  createdb -U hiveuser sentry_policy
fi

# Perform search-replace on $1, output to $2.
# Search $1 ($GCIN) for strings that look like "${FOO}".  If FOO is defined in
# the environment then replace "${FOO}" with the environment value.  Also
# remove or leave special kerberos settings as desired.  Sanity check at end.
function generate_config {
  GCIN="$1"
  GCOUT="$2"

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
      "${GCIN}" > "${GCOUT}.tmp"

  if [ "${IMPALA_KERBERIZE}" = "" ]; then
    sed '/<!-- BEGIN Kerberos/,/END Kerberos settings -->/d' \
        "${GCOUT}.tmp" > "${GCOUT}"
  else
    cp "${GCOUT}.tmp" "${GCOUT}"
  fi
  rm -f "${GCOUT}.tmp"

  # Check for anything that might have been missed.
  # Assumes that environment variables will be ALL CAPS...
  if grep '\${[A-Z_]*}' "${GCOUT}"; then
    echo "Found undefined variables in ${GCOUT}, aborting"
    exit 1
  fi

  echo "Generated `pwd`/${GCOUT}"
}

echo "Linking core-site.xml from local cluster"
CLUSTER_HADOOP_CONF_DIR=$(${CLUSTER_DIR}/admin get_hadoop_client_conf_dir)
ln -s ${CLUSTER_HADOOP_CONF_DIR}/core-site.xml

echo "Linking hdfs-site.xml from local cluster"
ln -s ${CLUSTER_HADOOP_CONF_DIR}/hdfs-site.xml

if ${CLUSTER_DIR}/admin is_kerberized; then
  # KERBEROS TODO: Without this, the yarn daemons can see these
  # files, but mapreduce jobs *cannot* see these files.  This seems
  # strange, but making these symlinks also results in data loading
  # failures in the non-kerberized case.  Without these, mapreduce
  # jobs die in a kerberized cluster because they can't find their
  # kerberos principals.  Obviously this has to be sorted out before
  # a kerberized cluster can load data.
  echo "Linking yarn and mapred from local cluster"
  ln -s ${CLUSTER_HADOOP_CONF_DIR}/yarn-site.xml
  ln -s ${CLUSTER_HADOOP_CONF_DIR}/mapred-site.xml
fi

generate_config postgresql-hive-site.xml.template hive-site.xml
generate_config log4j.properties.template log4j.properties
generate_config hive-log4j.properties.template hive-log4j.properties
generate_config hbase-site.xml.template hbase-site.xml
generate_config authz-policy.ini.template authz-policy.ini
generate_config sentry-site.xml.template sentry-site.xml

if [ ! -z "${IMPALA_KERBERIZE}" ]; then
  generate_config hbase-jaas-server.conf.template hbase-jaas-server.conf
  generate_config hbase-jaas-client.conf.template hbase-jaas-client.conf
fi

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
