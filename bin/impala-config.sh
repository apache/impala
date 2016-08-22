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

# Source this file from the $IMPALA_HOME directory to
# setup your environment. If $IMPALA_HOME is undefined
# this script will set it to the current working directory.

# This file must be kept compatible with bash options "set -euo pipefail". Those options
# will be set by other scripts before sourcing this file. Those options are not set in
# this script because scripts outside this repository may need to be updated and that
# is not practical at this time.
export JAVA_HOME="${JAVA_HOME:-/usr/java/default}"
if [ ! -d "$JAVA_HOME" ]; then
  echo "JAVA_HOME must be set to the location of your JDK!"
  return 1
fi
export JAVA="$JAVA_HOME/bin/java"
if [[ ! -e "$JAVA" ]]; then
  echo "Could not find java binary at $JAVA" >&2
  return 1
fi

if [ -z $IMPALA_HOME ]; then
  if [[ ! -z $ZSH_NAME ]]; then
    export IMPALA_HOME=$(dirname $(cd $(dirname ${(%):-%x}) && pwd))
  else
    export IMPALA_HOME=$(dirname $(cd $(dirname "${BASH_SOURCE[0]}") && pwd))
  fi
fi

: ${IMPALA_TOOLCHAIN=$IMPALA_HOME/toolchain}
if [ -z $IMPALA_TOOLCHAIN ]; then
  echo "IMPALA_TOOLCHAIN must be specified. Please set it to a valid directory or"\
       "leave it unset."
  return 1
fi

# If true, will not call $IMPALA_HOME/bin/bootstrap_toolchain.py.
: ${SKIP_TOOLCHAIN_BOOTSTRAP=false}

# This flag is used in $IMPALA_HOME/cmake_modules/toolchain.cmake.
# If it's 0, Impala will be built with the compiler in the toolchain directory.
: ${USE_SYSTEM_GCC=0}

# Gold is available on newer systems and a full build with static linking is ~1.5 mins
# faster using gold. A shared object build using gold is a little faster than using ld.
: ${USE_GOLD_LINKER=false}

# Override the default compiler by setting a path to the new compiler. The default
# compiler depends on USE_SYSTEM_GCC and IMPALA_GCC_VERSION. The intended use case
# is to set the compiler to distcc, in that case the user would also set
# IMPALA_BUILD_THREADS to increase parallelism.
: ${IMPALA_CXX_COMPILER=default}

# If enabled, debug symbols are added to cross-compiled IR.
: ${ENABLE_IMPALA_IR_DEBUG_INFO=false}

if [ -d $IMPALA_HOME/thirdparty ]; then
  NO_THIRDPARTY=false
else
  NO_THIRDPARTY=true
fi
# If true, download and use the CDH components from S3 instead of the ones
# in $IMPALA_HOME/thirdparty.
: ${DOWNLOAD_CDH_COMPONENTS=$NO_THIRDPARTY}

export IMPALA_TOOLCHAIN
export SKIP_TOOLCHAIN_BOOTSTRAP
export USE_SYSTEM_GCC
export USE_GOLD_LINKER
export IMPALA_CXX_COMPILER
export ENABLE_IMPALA_IR_DEBUG_INFO
export DOWNLOAD_CDH_COMPONENTS
export IS_OSX=$(if [[ "$OSTYPE" == "darwin"* ]]; then echo true; else echo false; fi)

# To use a local build of Kudu, set KUDU_BUILD_DIR to the path Kudu was built in and
# set KUDU_CLIENT_DIR to the path KUDU was installed in.
# Example:
#   git clone https://github.com/cloudera/kudu.git
#   ...build 3rd party etc...
#   mkdir -p $KUDU_BUILD_DIR
#   cd $KUDU_BUILD_DIR
#   cmake <path to Kudu source dir>
#   make
#   DESTDIR=$KUDU_CLIENT_DIR make install
: ${KUDU_BUILD_DIR=}
: ${KUDU_CLIENT_DIR=}
export KUDU_BUILD_DIR
export KUDU_CLIENT_DIR
if [[ -n $KUDU_BUILD_DIR && -z $KUDU_CLIENT_DIR ]]; then
  echo When KUDU_BUILD_DIR is set KUDU_CLIENT_DIR must also be set. 1>&2
  return 1
fi
if [[ -z $KUDU_BUILD_DIR && -n $KUDU_CLIENT_DIR ]]; then
  echo When KUDU_CLIENT_DIR is set KUDU_BUILD_DIR must also be set. 1>&2
  return 1
fi

: ${USE_KUDU_DEBUG_BUILD=false}   # Only applies when using Kudu from the toolchain
export USE_KUDU_DEBUG_BUILD

# Kudu doesn't compile on some old Linux distros. KUDU_IS_SUPPORTED enables building Kudu
# into the backend. The frontend build is OS independent since it is Java.
if [[ -z "${KUDU_IS_SUPPORTED-}" ]]; then
  KUDU_IS_SUPPORTED=true
  if [[ -z $KUDU_BUILD_DIR ]]; then
    if ! $IS_OSX; then
      if ! which lsb_release &>/dev/null; then
        echo Unable to find the 'lsb_release' command. \
            Please ensure it is available in your PATH. 1>&2
        return 1
      fi
      DISTRO_VERSION=$(lsb_release -sir 2>&1)
      if [[ $? -ne 0 ]]; then
        echo lsb_release cammond failed, output was: "$DISTRO_VERSION" 1>&2
        return 1
      fi
      # Remove spaces, trim minor versions, and convert to lowercase.
      DISTRO_VERSION=$(tr -d ' \n' <<< "$DISTRO_VERSION" | cut -d. -f1 | tr "A-Z" "a-z")
      case "$DISTRO_VERSION" in
        # "enterprise" is Oracle
        centos5 | debian* | enterprise*5 | redhat*5 | suse* | ubuntu*12)
            KUDU_IS_SUPPORTED=false;;
      esac
    fi
  fi
fi
export KUDU_IS_SUPPORTED

export CDH_MAJOR_VERSION=5
export HADOOP_LZO=${HADOOP_LZO-$IMPALA_HOME/../hadoop-lzo}
export IMPALA_LZO=${IMPALA_LZO-$IMPALA_HOME/../Impala-lzo}
export IMPALA_AUX_TEST_HOME=${IMPALA_AUX_TEST_HOME-$IMPALA_HOME/../Impala-auxiliary-tests}
export TARGET_FILESYSTEM=${TARGET_FILESYSTEM-"hdfs"}
export FILESYSTEM_PREFIX=${FILESYSTEM_PREFIX-""}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY-"DummySecretAccessKey"}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID-"DummyAccessKeyId"}
export S3_BUCKET=${S3_BUCKET-""}
export HDFS_REPLICATION=${HDFS_REPLICATION-3}
export ISILON_NAMENODE=${ISILON_NAMENODE-""}
export DEFAULT_FS=${DEFAULT_FS-"hdfs://localhost:20500"}
export WAREHOUSE_LOCATION_PREFIX=${WAREHOUSE_LOCATION_PREFIX-""}
export LOCAL_FS="file:${WAREHOUSE_LOCATION_PREFIX}"
export METASTORE_DB="hive_impala"

if [ "${TARGET_FILESYSTEM}" = "s3" ]; then
  # Basic error checking
  if [[ "${AWS_ACCESS_KEY_ID}" = "DummyAccessKeyId" ||\
        "${AWS_SECRET_ACCESS_KEY}" = "DummySecretAccessKey" ]]; then
    echo "Both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
      need to be assigned valid values and belong to the owner of the s3
      bucket in order to access the file system"
    return 1
  fi
  # Check if the s3 bucket is NULL.
  if [[ "${S3_BUCKET}" = "" ]]; then
    echo "S3_BUCKET cannot be an empty string for s3"
    return 1
  fi
  aws s3 ls "s3://${S3_BUCKET}/" 1>/dev/null
  if [ $? != 0 ]; then
    echo "Access to ${S3_BUCKET} failed."
    return 1
  fi
  DEFAULT_FS="s3a://${S3_BUCKET}"
  export DEFAULT_FS
elif [ "${TARGET_FILESYSTEM}" = "isilon" ]; then
  if [ "${ISILON_NAMENODE}" = "" ]; then
    echo "In order to access the Isilon filesystem, ISILON_NAMENODE"
    echo "needs to be a non-empty and valid address."
    return 1
  fi
  DEFAULT_FS="hdfs://${ISILON_NAMENODE}:8020"
  export DEFAULT_FS
  # isilon manages its own replication.
  export HDFS_REPLICATION=1
elif [ "${TARGET_FILESYSTEM}" = "local" ]; then
  if [ ! -d "${WAREHOUSE_LOCATION_PREFIX}" ]; then
    echo "'$WAREHOUSE_LOCATION_PREFIX' is not a directory on the local filesystem."
    return 1
  elif [ ! -r "${WAREHOUSE_LOCATION_PREFIX}" ] || \
      [ ! -w "${WAREHOUSE_LOCATION_PREFIX}" ]; then
    echo "Current user does not have read/write permissions on local filesystem path "
        "'$WAREHOUSE_LOCATION_PREFIX'"
    return 1
  fi
  export DEFAULT_FS=${LOCAL_FS}
  export FILESYSTEM_PREFIX=${LOCAL_FS}
elif [ "${TARGET_FILESYSTEM}" != "hdfs" ]; then
  echo "Unsupported filesystem '$TARGET_FILESYSTEM'"
  echo "Valid values are: hdfs, isilon, s3, local"
  return 1
fi

# Directories where local cluster logs will go when running tests or loading data
export IMPALA_LOGS_DIR=${IMPALA_HOME}/logs
export IMPALA_CLUSTER_LOGS_DIR=${IMPALA_LOGS_DIR}/cluster
export IMPALA_DATA_LOADING_LOGS_DIR=${IMPALA_LOGS_DIR}/data_loading
export IMPALA_DATA_LOADING_SQL_DIR=${IMPALA_DATA_LOADING_LOGS_DIR}/sql
export IMPALA_FE_TEST_LOGS_DIR=${IMPALA_LOGS_DIR}/fe_tests
export IMPALA_FE_TEST_COVERAGE_DIR=${IMPALA_FE_TEST_LOGS_DIR}/coverage
export IMPALA_BE_TEST_LOGS_DIR=${IMPALA_LOGS_DIR}/be_tests
export IMPALA_EE_TEST_LOGS_DIR=${IMPALA_LOGS_DIR}/ee_tests
export IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR=${IMPALA_LOGS_DIR}/custom_cluster_tests
# List of all Impala log dirs and create them.
export IMPALA_ALL_LOGS_DIRS="${IMPALA_CLUSTER_LOGS_DIR}
  ${IMPALA_DATA_LOADING_LOGS_DIR} ${IMPALA_DATA_LOADING_SQL_DIR}
  ${IMPALA_EE_TEST_LOGS_DIR} ${IMPALA_FE_TEST_COVERAGE_DIR}
  ${IMPALA_BE_TEST_LOGS_DIR} ${IMPALA_EE_TEST_LOGS_DIR}
  ${IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR}"
mkdir -p $IMPALA_ALL_LOGS_DIRS

# Create symlinks Testing/Temporary and be/Testing/Temporary that point to the BE test
# log dir to capture the all logs of BE unit tests. Gtest has Testing/Temporary
# hardwired in its code, so we cannot change the output dir by configuration.
# We create two symlinks to capture the logs when running ctest either from
# ${IMPALA_HOME} or ${IMPALA_HOME}/be.
rm -rf ${IMPALA_HOME}/Testing
mkdir -p ${IMPALA_HOME}/Testing
ln -fs ${IMPALA_BE_TEST_LOGS_DIR} ${IMPALA_HOME}/Testing/Temporary
rm -rf ${IMPALA_HOME}/be/Testing
mkdir -p ${IMPALA_HOME}/be/Testing
ln -fs ${IMPALA_BE_TEST_LOGS_DIR} ${IMPALA_HOME}/be/Testing/Temporary

# Reduce the concurrency for local tests to half the number of cores in the system.
# Note than nproc may not be available on older distributions (centos5.5)
if type nproc >/dev/null 2>&1; then
  CORES=$(($(nproc) / 2))
else
  CORES='4'
fi
export NUM_CONCURRENT_TESTS=${NUM_CONCURRENT_TESTS-${CORES}}

# Versions of toolchain dependencies (or if toolchain is not used of dependencies in
# thirdparty)
export IMPALA_AVRO_VERSION=1.7.4-p4
export IMPALA_BINUTILS_VERSION=2.26-p1
export IMPALA_BOOST_VERSION=1.57.0
export IMPALA_BREAKPAD_VERSION=20150612-p1
export IMPALA_BZIP2_VERSION=1.0.6-p1
export IMPALA_CMAKE_VERSION=3.2.3-p1
export IMPALA_CYRUS_SASL_VERSION=2.1.23
export IMPALA_GCC_VERSION=4.9.2
export IMPALA_GFLAGS_VERSION=2.0
export IMPALA_GLOG_VERSION=0.3.2-p2
export IMPALA_GPERFTOOLS_VERSION=2.5
export IMPALA_GTEST_VERSION=1.6.0
export IMPALA_KUDU_VERSION=0.10.0-RC1
export IMPALA_LLVM_VERSION=3.8.0-p1
export IMPALA_LLVM_ASAN_VERSION=3.8.0-p1
# Debug builds should use the release+asserts build to get additional coverage.
# Don't use the LLVM debug build because the binaries are too large to distribute.
export IMPALA_LLVM_DEBUG_VERSION=3.8.0-asserts-p1
export IMPALA_LZ4_VERSION=svn
export IMPALA_OPENLDAP_VERSION=2.4.25
export IMPALA_OPENSSL_VERSION=0.9.8zf
export IMPALA_POSTGRES_JDBC_DRIVER_VERSION=9.0-801
export IMPALA_RAPIDJSON_VERSION=0.11
export IMPALA_RE2_VERSION=20130115-p1
export IMPALA_SNAPPY_VERSION=1.1.3
export IMPALA_SQUEASEL_VERSION=3.3
# TPC utilities used for test/benchmark data generation.
export IMPALA_TPC_DS_VERSION=2.1.0
export IMPALA_TPC_H_VERSION=2.17.0
export IMPALA_THRIFT_VERSION=0.9.0-p8
export IMPALA_THRIFT_JAVA_VERSION=0.9.0
export IMPALA_ZLIB_VERSION=1.2.8

export KUDU_MASTER=${KUDU_MASTER:-"127.0.0.1"}
export KUDU_MASTER_PORT=${KUDU_MASTER_PORT:-"7051"}
# TODO: Figure out a way to use a snapshot version without causing a lot of breakage due
#       to nightly changes from Kudu. The version below is the last released version but
#       before release this needs to be updated to the version about to be released.
export KUDU_JAVA_VERSION=0.10.0-SNAPSHOT

if [[ $OSTYPE == "darwin"* ]]; then
  IMPALA_CYRUS_SASL_VERSION=2.1.26
  IMPALA_GPERFTOOLS_VERSION=2.3
  IMPALA_OPENSSL_VERSION=1.0.1p
  IMPALA_THRIFT_VERSION=0.9.2
  IMPALA_THRIFT_JAVA_VERSION=0.9.2
fi

export IMPALA_HADOOP_VERSION=2.6.0-cdh5.10.0-SNAPSHOT
export IMPALA_HBASE_VERSION=1.2.0-cdh5.10.0-SNAPSHOT
export IMPALA_HIVE_VERSION=1.1.0-cdh5.10.0-SNAPSHOT
export IMPALA_SENTRY_VERSION=1.5.1-cdh5.10.0-SNAPSHOT
export IMPALA_LLAMA_VERSION=1.0.0-cdh5.10.0-SNAPSHOT
export IMPALA_PARQUET_VERSION=1.5.0-cdh5.10.0-SNAPSHOT
export IMPALA_LLAMA_MINIKDC_VERSION=1.0.0

export IMPALA_FE_DIR=$IMPALA_HOME/fe
export IMPALA_BE_DIR=$IMPALA_HOME/be
export IMPALA_WORKLOAD_DIR=$IMPALA_HOME/testdata/workloads
export IMPALA_AUX_WORKLOAD_DIR=$IMPALA_AUX_TEST_HOME/testdata/workloads
export IMPALA_DATASET_DIR=$IMPALA_HOME/testdata/datasets
export IMPALA_AUX_DATASET_DIR=$IMPALA_AUX_TEST_HOME/testdata/datasets
export IMPALA_COMMON_DIR=$IMPALA_HOME/common
export PATH=$IMPALA_HOME/bin:$IMPALA_TOOLCHAIN/cmake-$IMPALA_CMAKE_VERSION/bin/:$PATH

# The directory in which all the thirdparty CDH components live.
if [ "${DOWNLOAD_CDH_COMPONENTS}" = true ]; then
  export CDH_COMPONENTS_HOME=$IMPALA_TOOLCHAIN/cdh_components
else
  export CDH_COMPONENTS_HOME=$IMPALA_HOME/thirdparty
fi

# Hadoop dependencies are snapshots in the Impala tree
export HADOOP_HOME=$CDH_COMPONENTS_HOME/hadoop-${IMPALA_HADOOP_VERSION}/
export HADOOP_CONF_DIR=$IMPALA_FE_DIR/src/test/resources

: ${HADOOP_CLASSPATH=}
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:"${HADOOP_HOME}/share/hadoop/tools/lib/*"
# YARN is configured to use LZO so the LZO jar needs to be in the hadoop classpath.
export LZO_JAR_PATH="$HADOOP_LZO/build/hadoop-lzo-0.4.15.jar"
HADOOP_CLASSPATH+=":$LZO_JAR_PATH"

export MINI_DFS_BASE_DATA_DIR=$IMPALA_HOME/cdh-${CDH_MAJOR_VERSION}-hdfs-data
export PATH=$HADOOP_HOME/bin:$PATH

export LLAMA_HOME=$CDH_COMPONENTS_HOME/llama-${IMPALA_LLAMA_VERSION}/
export MINIKDC_HOME=$CDH_COMPONENTS_HOME/llama-minikdc-${IMPALA_LLAMA_MINIKDC_VERSION}
export SENTRY_HOME=$CDH_COMPONENTS_HOME/sentry-${IMPALA_SENTRY_VERSION}
export SENTRY_CONF_DIR=$IMPALA_HOME/fe/src/test/resources

export HIVE_HOME=$CDH_COMPONENTS_HOME/hive-${IMPALA_HIVE_VERSION}/
export PATH=$HIVE_HOME/bin:$PATH
export HIVE_CONF_DIR=$IMPALA_FE_DIR/src/test/resources

# Hive looks for jar files in a single directory from HIVE_AUX_JARS_PATH plus
# any jars in AUX_CLASSPATH. (Or a list of jars in HIVE_AUX_JARS_PATH.)
# The Postgres JDBC driver is downloaded by maven when building the frontend.
# Export the location of Postgres JDBC driver so Sentry can pick it up.
export POSTGRES_JDBC_DRIVER=${IMPALA_FE_DIR}/target/dependency/postgresql-${IMPALA_POSTGRES_JDBC_DRIVER_VERSION}.jdbc4.jar

export HIVE_AUX_JARS_PATH="$POSTGRES_JDBC_DRIVER"
export AUX_CLASSPATH="${LZO_JAR_PATH}"
### Tell hive not to use jline
export HADOOP_USER_CLASSPATH_FIRST=true

export HBASE_HOME=$CDH_COMPONENTS_HOME/hbase-${IMPALA_HBASE_VERSION}/
export PATH=$HBASE_HOME/bin:$PATH

# Add the jars so hive can create hbase tables.
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-common-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-client-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-server-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-protocol-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-hadoop-compat-${IMPALA_HBASE_VERSION}.jar

export HBASE_CONF_DIR=$HIVE_CONF_DIR

# Set $THRIFT_HOME to the Thrift directory in toolchain.
export THRIFT_HOME=${IMPALA_TOOLCHAIN}/thrift-${IMPALA_THRIFT_VERSION}

# ASAN needs a matching version of llvm-symbolizer to symbolize stack traces.
export ASAN_SYMBOLIZER_PATH=${IMPALA_TOOLCHAIN}/llvm-${IMPALA_LLVM_ASAN_VERSION}/bin/llvm-symbolizer

export CLUSTER_DIR=${IMPALA_HOME}/testdata/cluster

: ${IMPALA_BUILD_THREADS:=$(nproc)}
export IMPALA_BUILD_THREADS

# Some environments (like the packaging build) might not have $USER set.  Fix that here.
export USER=${USER-`id -un`}

# Configure python path
. $IMPALA_HOME/bin/set-pythonpath.sh

# These arguments are, despite the name, passed to every JVM created
# by an impalad.
# - Enable JNI check
# When running hive UDFs, this check makes it unacceptably slow (over 100x)
# Enable if you suspect a JNI issue
# TODO: figure out how to turn this off only the stuff that can't run with it.
#LIBHDFS_OPTS="-Xcheck:jni -Xcheck:nabounds"
# - Points to the location of libbackend.so.
LIBHDFS_OPTS="${LIBHDFS_OPTS:-}"
LIBHDFS_OPTS="${LIBHDFS_OPTS} -Djava.library.path=${HADOOP_HOME}/lib/native/"
# READER BEWARE: This always points to the debug build.
# TODO: Consider having cmake scripts change this value depending on
# the build type.
export LIBHDFS_OPTS="${LIBHDFS_OPTS}:${IMPALA_HOME}/be/build/debug/service"

export ARTISTIC_STYLE_OPTIONS=$IMPALA_BE_DIR/.astylerc

export IMPALA_SNAPPY_PATH=${IMPALA_TOOLCHAIN}/snappy-${IMPALA_SNAPPY_VERSION}/lib

export JAVA_LIBRARY_PATH=${IMPALA_SNAPPY_PATH}

# So that the frontend tests and PlanService can pick up libbackend.so
# and other required libraries
LIB_JAVA=`find ${JAVA_HOME}/   -name libjava.so | head -1`
LIB_JSIG=`find ${JAVA_HOME}/   -name libjsig.so | head -1`
LIB_JVM=` find ${JAVA_HOME}/   -name libjvm.so  | head -1`
LD_LIBRARY_PATH="${LD_LIBRARY_PATH-}"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:`dirname ${LIB_JAVA}`:`dirname ${LIB_JSIG}`"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:`dirname ${LIB_JVM}`"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${HADOOP_HOME}/lib/native"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_HOME}/be/build/debug/service"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_SNAPPY_PATH}"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_LZO}/build"

if [ $USE_SYSTEM_GCC -eq 0 ]; then
  IMPALA_TOOLCHAIN_GCC_LIB=${IMPALA_TOOLCHAIN}/gcc-${IMPALA_GCC_VERSION}/lib64
  LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_TOOLCHAIN_GCC_LIB}"
fi

export LD_LIBRARY_PATH
LD_PRELOAD="${LD_PRELOAD-}"
export LD_PRELOAD="${LD_PRELOAD}:${LIB_JSIG}"

CLASSPATH="${CLASSPATH-}"
CLASSPATH=$IMPALA_FE_DIR/target/dependency:$CLASSPATH
CLASSPATH=$IMPALA_FE_DIR/target/classes:$CLASSPATH
CLASSPATH=$IMPALA_FE_DIR/src/test/resources:$CLASSPATH
CLASSPATH=$LZO_JAR_PATH:$CLASSPATH
export CLASSPATH

# Setup aliases
# Helper alias to script that verifies and merges Gerrit changes
alias gerrit-verify-only="${IMPALA_AUX_TEST_HOME}/jenkins/gerrit-verify-only.sh"

# A marker in the environment to prove that we really did source this file
export IMPALA_CONFIG_SOURCED=1

echo "IMPALA_HOME             = $IMPALA_HOME"
echo "HADOOP_HOME             = $HADOOP_HOME"
echo "HADOOP_CONF_DIR         = $HADOOP_CONF_DIR"
echo "MINI_DFS_BASE_DATA_DIR  = $MINI_DFS_BASE_DATA_DIR"
echo "HIVE_HOME               = $HIVE_HOME"
echo "HIVE_CONF_DIR           = $HIVE_CONF_DIR"
echo "HBASE_HOME              = $HBASE_HOME"
echo "HBASE_CONF_DIR          = $HBASE_CONF_DIR"
echo "MINIKDC_HOME            = $MINIKDC_HOME"
echo "THRIFT_HOME             = $THRIFT_HOME"
echo "HADOOP_LZO              = $HADOOP_LZO"
echo "IMPALA_LZO              = $IMPALA_LZO"
echo "CLASSPATH               = $CLASSPATH"
echo "LIBHDFS_OPTS            = $LIBHDFS_OPTS"
echo "PYTHONPATH              = $PYTHONPATH"
echo "JAVA_HOME               = $JAVA_HOME"
echo "LD_LIBRARY_PATH         = $LD_LIBRARY_PATH"
echo "LD_PRELOAD              = $LD_PRELOAD"
echo "POSTGRES_JDBC_DRIVER    = $POSTGRES_JDBC_DRIVER"
echo "IMPALA_TOOLCHAIN        = $IMPALA_TOOLCHAIN"
echo "DOWNLOAD_CDH_COMPONENTS = $DOWNLOAD_CDH_COMPONENTS"

# Kerberos things.  If the cluster exists and is kerberized, source
# the required environment.  This is required for any hadoop tool to
# work.  Note that if impala-config.sh is sourced before the
# kerberized cluster is created, it will have to be sourced again
# *after* the cluster is created in order to pick up these settings.
export MINIKDC_ENV=${IMPALA_HOME}/testdata/bin/minikdc_env.sh
if ${CLUSTER_DIR}/admin is_kerberized; then
  . ${MINIKDC_ENV}
  echo " *** This cluster is kerberized ***"
  echo "KRB5_KTNAME            = $KRB5_KTNAME"
  echo "KRB5_CONFIG            = $KRB5_CONFIG"
  echo "KRB5_TRACE             = $KRB5_TRACE"
  echo "HADOOP_OPTS            = $HADOOP_OPTS"
  echo " *** This cluster is kerberized ***"
else
  # If the cluster *isn't* kerberized, ensure that the environment isn't
  # polluted with kerberos items that might screw us up.  We go through
  # everything set in the minikdc environment and explicitly unset it.
  unset `grep export ${MINIKDC_ENV} | sed "s/.*export \([^=]*\)=.*/\1/" \
      | sort | uniq`
fi
