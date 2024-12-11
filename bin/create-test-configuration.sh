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
# Create the test environment needed by Impala. Includes generation of the
# Hadoop config files: core-site.xml, hbase-site.xml, hive-site.xml as well
# as creation of the Hive metastore.

set -euo pipefail
. $IMPALA_HOME/bin/report_build_error.sh
setup_report_build_error

# Perform search-replace on $1, output to $2.
# Search $1 ($GCIN) for strings that look like "${FOO}".  If FOO is defined in
# the environment then replace "${FOO}" with the environment value.  Also
# remove or leave special kerberos settings as desired.  Sanity check at end.
#
# NOTE: for Hadoop-style XML configuration files (foo-site.xml) prefer using
# bin/generate_xml_config.py instead of this method. This method is useful for
# ini-style or other configuration formats.
#
# TODO(todd): convert remaining 'foo-site.xml' files to use the preferred
# mechanism.
#
# TODO(todd): consider a better Python-based templating system for the other
# configuration files as well.
function generate_config {
  GCIN="$1"
  GCOUT="$2"

  perl -wpl -e 's/\$\{([^}]+)\}/defined $ENV{$1} ? $ENV{$1} : $&/eg' \
      "${GCIN}" > "${GCOUT}.tmp"

  if [[ "${IMPALA_KERBERIZE}" != "true" ]]; then
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

CREATE_METASTORE=0
CREATE_RANGER_POLICY_DB=0
UPGRADE_METASTORE_DB=0

# parse command line options
for ARG in $*
do
  case "$ARG" in
    -create_metastore)
      CREATE_METASTORE=1
      ;;
    -create_ranger_policy_db)
      CREATE_RANGER_POLICY_DB=1
      ;;
    -upgrade_metastore_db)
      UPGRADE_METASTORE_DB=1
      ;;
    -help|*)
      echo "[-create_metastore] : If true, creates a new metastore."
      echo "[-create_ranger_policy_db] : If true, creates a new Ranger policy db."
      echo "[-upgrade_metastore_db] : If true, upgrades the schema of HMS db."
      exit 1
      ;;
  esac
done

# If this isn't sourced, bad things will always happen
if [ "${IMPALA_CONFIG_SOURCED}" != "1" ]; then
  echo "You must source bin/impala-config.sh"
  exit 1
fi

${CLUSTER_DIR}/admin create_cluster

if [[ "${IMPALA_KERBERIZE}" = "true" ]]; then
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

export CURRENT_USER=`whoami`

CONFIG_DIR=${IMPALA_HOME}/fe/src/test/resources
RANGER_TEST_CONF_DIR="${IMPALA_HOME}/testdata/cluster/ranger"

echo "Config dir: ${CONFIG_DIR}"
echo "Current user: ${CURRENT_USER}"
echo "Metastore DB: ${METASTORE_DB}"
echo "Ranger DB   : ${RANGER_POLICY_DB}"

pushd ${CONFIG_DIR}
# Cleanup any existing files
rm -f {core,hdfs,hbase,hive,ozone,yarn,mapred}-site.xml
rm -f authz-provider.ini

# Generate hive configs first so that schemaTool can be used to init the metastore schema
# if needed

$IMPALA_HOME/bin/generate_xml_config.py hive-site.xml.py hive-site.xml
export HIVE_VARIANT=changed_external_dir
$IMPALA_HOME/bin/generate_xml_config.py hive-site.xml.py hive-site_ext.xml
mkdir -p hive-site-ext
rm -f hive-site-ext/hive-site.xml
ln -s "${CONFIG_DIR}/hive-site_ext.xml" hive-site-ext/hive-site.xml

export HIVE_VARIANT=without_hms_config
$IMPALA_HOME/bin/generate_xml_config.py hive-site.xml.py hive-site_without_hms.xml
mkdir -p hive-site-without-hms
rm -f hive-site-without-hms/hive-site.xml
ln -s "${CONFIG_DIR}/hive-site_without_hms.xml" hive-site-without-hms/hive-site.xml

export HIVE_VARIANT=events_cleanup
$IMPALA_HOME/bin/generate_xml_config.py hive-site.xml.py hive-site_events_cleanup.xml
mkdir -p hive-site-events-cleanup
rm -f hive-site-events-cleanup/hive-site.xml
ln -s "${CONFIG_DIR}/hive-site_events_cleanup.xml" hive-site-events-cleanup/hive-site.xml

export HIVE_VARIANT=housekeeping_on
$IMPALA_HOME/bin/generate_xml_config.py hive-site.xml.py hive-site_housekeeping_on.xml
mkdir -p hive-site-housekeeping-on
rm -f hive-site-housekeeping-on/hive-site.xml
ln -s "${CONFIG_DIR}/hive-site_housekeeping_on.xml" \
    hive-site-housekeeping-on/hive-site.xml

export HIVE_VARIANT=ranger_auth
HIVE_RANGER_CONF_DIR=hive-site-ranger-auth
$IMPALA_HOME/bin/generate_xml_config.py hive-site.xml.py hive-site_ranger_auth.xml

# Cleanup pycache if created
rm -rf __pycache__

rm -rf $HIVE_RANGER_CONF_DIR
mkdir -p $HIVE_RANGER_CONF_DIR
ln -s "${CONFIG_DIR}/hive-site_ranger_auth.xml" $HIVE_RANGER_CONF_DIR/hive-site.xml
# Link some neccessary config files for Hive.
for f in ranger-hive-security.xml ranger-hive-audit.xml log4j.properties \
    hive-log4j2.properties; do
  ln -s "${CONFIG_DIR}/$f" "$HIVE_RANGER_CONF_DIR/$f"
done

generate_config hive-log4j2.properties.template hive-log4j2.properties

if [ $CREATE_METASTORE -eq 1 ]; then
  echo "Creating postgresql database for Hive metastore"
  dropdb -U hiveuser ${METASTORE_DB} || true
  createdb -U hiveuser ${METASTORE_DB}

  # Use schematool to initialize the metastore db schema. It detects the Hive
  # version and invokes the appropriate scripts
  CLASSPATH={$CLASSPATH}:${CONFIG_DIR} ${HIVE_HOME}/bin/schematool -initSchema -dbType \
postgres 1>${IMPALA_CLUSTER_LOGS_DIR}/schematool.log 2>&1
  # TODO: We probably don't need to do this anymore
  # Increase the size limit of PARAM_VALUE from SERDE_PARAMS table to be able to create
  # HBase tables with large number of columns.
  echo "alter table \"SERDE_PARAMS\" alter column \"PARAM_VALUE\" type character varying" \
      | psql -q -U hiveuser -d ${METASTORE_DB}
fi

if [ $UPGRADE_METASTORE_DB -eq 1 ]; then
  echo "Upgrading the schema of metastore db ${METASTORE_DB}. Check \
${IMPALA_CLUSTER_LOGS_DIR}/schematool.log for details."
  CLASSPATH={$CLASSPATH}:${CONFIG_DIR} ${HIVE_HOME}/bin/schematool -upgradeSchema \
-dbType postgres 1>${IMPALA_CLUSTER_LOGS_DIR}/schematool.log 2>&1
fi

if [ $CREATE_RANGER_POLICY_DB -eq 1 ]; then
  echo "Creating Ranger Policy Server DB"
  dropdb -U hiveuser "${RANGER_POLICY_DB}" 2> /dev/null || true
  createdb -U hiveuser "${RANGER_POLICY_DB}"
  pushd "${RANGER_HOME}"
  generate_config "${RANGER_TEST_CONF_DIR}/install.properties.template" install.properties
# TODO : Need to change following python to ambari-python-wrap ?
  python ./db_setup.py
  popd
fi

echo "Copying common conf files from local cluster:"
CLUSTER_HADOOP_CONF_DIR=$(${CLUSTER_DIR}/admin get_hadoop_client_conf_dir)
for file in core-site.xml hdfs-site.xml ozone-site.xml yarn-site.xml ; do
  echo ... $file
  # These need to be copied instead of symlinked so that they can be accessed when the
  # directory is bind-mounted into /opt/impala/conf in docker containers.
  cp ${CLUSTER_HADOOP_CONF_DIR}/$file .
done

if [[ "${IMPALA_KERBERIZE}" = "true" ]]; then
  # KERBEROS TODO: Without this, the yarn daemons can see these
  # files, but mapreduce jobs *cannot* see these files.  This seems
  # strange, but making these symlinks also results in data loading
  # failures in the non-kerberized case.  Without these, mapreduce
  # jobs die in a kerberized cluster because they can't find their
  # kerberos principals. Obviously this has to be sorted out before
  # a kerberized cluster can load data.
  echo "Linking yarn and mapred from local cluster"
  ln -s ${CLUSTER_HADOOP_CONF_DIR}/mapred-site.xml
fi

generate_config log4j.properties.template log4j.properties
generate_config hbase-site.xml.template hbase-site.xml

if [[ "${IMPALA_KERBERIZE}" = "true" ]]; then
  generate_config hbase-jaas-server.conf.template hbase-jaas-server.conf
  generate_config hbase-jaas-client.conf.template hbase-jaas-client.conf
fi

popd

RANGER_SERVER_CONF_DIR="${RANGER_HOME}/ews/webapp/WEB-INF/classes/conf"
RANGER_SERVER_CONFDIST_DIR="${RANGER_HOME}/ews/webapp/WEB-INF/classes/conf.dist"
RANGER_SERVER_LIB_DIR="${RANGER_HOME}/ews/webapp/WEB-INF/lib"
RANGER_ADMIN_LOGBACK_CONF_FILE="${RANGER_SERVER_CONFDIST_DIR}/logback.xml"
RANGER_ADMIN_LOG4J2_CONF_FILE="${RANGER_HOME}/ews/webapp/WEB-INF/log4j2.properties"
RANGER_LOG_DIR="${IMPALA_CLUSTER_LOGS_DIR}/ranger"
if [[ ! -d "${RANGER_SERVER_CONF_DIR}" ]]; then
    mkdir -p "${RANGER_SERVER_CONF_DIR}"
fi

cp -f "${RANGER_TEST_CONF_DIR}/java_home.sh" "${RANGER_SERVER_CONF_DIR}"
cp -f "${RANGER_TEST_CONF_DIR}/ranger-admin-env-logdir.sh" "${RANGER_SERVER_CONF_DIR}"
cp -f "${RANGER_TEST_CONF_DIR}/ranger-admin-env-piddir.sh" "${RANGER_SERVER_CONF_DIR}"
cp -f "${RANGER_SERVER_CONFDIST_DIR}/security-applicationContext.xml" \
    "${RANGER_SERVER_CONF_DIR}"
# For Apache Ranger, we need logback.xml under ${RANGER_SERVER_CONF_DIR} so that the log
# files like ranger-admin-$(hostname)-$(whoami).log could be created under
# ${RANGER_LOG_DIR}.
if [[ -f ${RANGER_ADMIN_LOGBACK_CONF_FILE} ]]; then
  cp -f ${RANGER_ADMIN_LOGBACK_CONF_FILE} ${RANGER_SERVER_CONF_DIR}
fi
# For CDP Ranger, we change the value of the property 'log.dir' in the corresponding
# log4j2.properties so that the log files like ranger-admin-server.log could be created
# under ${RANGER_LOG_DIR}.
if [[ -f ${RANGER_ADMIN_LOG4J2_CONF_FILE} ]]; then
  # Use vertical bar instead of slash as the separator to prevent the slash(es) in
  # ${RANGER_LOG_DIR} from interfering with the parsing of sed.
  sed -i "s|property\.log\.dir=.*|property.log.dir=${RANGER_LOG_DIR}|g" \
      ${RANGER_ADMIN_LOG4J2_CONF_FILE}
fi

# Prepend the following 5 URL's to the line starting with "<intercept-url pattern="/**"".
# Before the end-to-end tests could be performed in a Kerberized environment
# automatically, we need to allow the requests for the following links so that the
# statements like CREATE/DROP ROLE <role_name>,
# GRANT/REVOKE ROLE <role_name> TO/FROM GROUP <group_name>, and SHOW ROLES could work in a
# non-Kerberized environment. It is better to add the allowed links using sed than to use
# a hardcoded configuration file consisting of those links since some other configurations
# could change after CDP_BUILD_NUMBER is bumped up, e.g., the version of jquery.
sed -i '/<intercept-url pattern="\/\*\*"/i \
    <intercept-url pattern="/service/public/v2/api/roles/*" access="permitAll"/> \
    <intercept-url pattern="/service/public/v2/api/roles/name/*" access="permitAll"/> \
    <intercept-url pattern="/service/public/v2/api/roles/grant/*" access="permitAll"/> \
    <intercept-url pattern="/service/public/v2/api/roles/revoke/*" access="permitAll"/> \
    <intercept-url pattern="/service/public/v2/api/roles/names/*" access="permitAll"/>' \
"${RANGER_SERVER_CONF_DIR}/security-applicationContext.xml"

if [[ -f "${POSTGRES_JDBC_DRIVER}" ]]; then
  cp -f "${POSTGRES_JDBC_DRIVER}" "${RANGER_SERVER_LIB_DIR}"
else
  # IMPALA-8261: Running this script should not fail when FE has not been built.
  MAVEN_URL="https://repo.maven.apache.org/maven2/org/postgresql/postgresql"
  JDBC_JAR="postgresql-${IMPALA_POSTGRES_JDBC_DRIVER_VERSION}.jar"
  wget -P "${RANGER_SERVER_LIB_DIR}" \
    "${MAVEN_URL}/${IMPALA_POSTGRES_JDBC_DRIVER_VERSION}/${JDBC_JAR}"
fi

pushd "${RANGER_SERVER_CONF_DIR}"
generate_config "${RANGER_TEST_CONF_DIR}/ranger-admin-default-site.xml.template" \
    ranger-admin-default-site.xml
generate_config "${RANGER_TEST_CONF_DIR}/ranger-admin-site.xml.template" \
    ranger-admin-site.xml
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

if [ -d ${IMPALA_AUX_TEST_HOME}/tests/functional ]; then
  symlink_subdirs ${IMPALA_AUX_TEST_HOME}/tests/functional ${IMPALA_HOME}/tests
else
  # For compatibility with older auxiliary tests, which aren't in the
  # functional subdirectory.
  symlink_subdirs ${IMPALA_AUX_TEST_HOME}/tests ${IMPALA_HOME}/tests
fi
