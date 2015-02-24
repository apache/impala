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

# Source this file from the $IMPALA_HOME directory to
# setup your environment. If $IMPALA_HOME is undefined
# this script will set it to the current working directory.

export JAVA_HOME=${JAVA_HOME:-/usr/java/default}
if [ ! -d $JAVA_HOME ] ; then
    echo "JAVA_HOME must be set to the location of your JDK!"
    return 1
fi

if [ -z $IMPALA_HOME ]; then
    this=${0/-/} # login-shells often have leading '-' chars
    shell_exec=`basename $SHELL`
    if [ "$this" = "$shell_exec" ]; then
        # Assume we're already in IMPALA_HOME
        interactive=1
        export IMPALA_HOME=`pwd`
    else
        interactive=0
        while [ -h "$this" ]; do
            ls=`ls -ld "$this"`
            link=`expr "$ls" : '.*-> \(.*\)$'`
            if expr "$link" : '.*/.*' > /dev/null; then
                this="$link"
            else
                this=`dirname "$this"`/"$link"
            fi
        done

        # convert relative path to absolute path
        bin=`dirname "$this"`
        script=`basename "$this"`
        bin=`cd "$bin"; pwd`
        this="$bin/$script"

        export IMPALA_HOME=`dirname "$bin"`
    fi
fi

export CDH_MAJOR_VERSION=5
export HADOOP_LZO=${HADOOP_LZO-~/hadoop-lzo}
export IMPALA_LZO=${IMPALA_LZO-~/Impala-lzo}
export IMPALA_AUX_TEST_HOME=${IMPALA_AUX_TEST_HOME-~/impala-auxiliary-tests}
export TARGET_FILESYSTEM=${TARGET_FILESYSTEM-"hdfs"}
export FILESYSTEM_PREFIX=${FILESYSTEM_PREFIX-""}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY-"DummySecretAccessKey"}
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID-"DummyAccessKeyId"}

# The target filesystem should be one of hdfs or s3
if [[ "${TARGET_FILESYSTEM}" != "hdfs" && "${TARGET_FILESYSTEM}" != "s3" ]]; then
  echo "Unsupported filesystem '$TARGET_FILESYSTEM', valid values are: ('hdfs', 's3')"
  return 1
elif [ "${TARGET_FILESYSTEM}" = "s3" ]; then
  # Basic error checking
  if [[ "${AWS_ACCESS_KEY_ID}" = "DummyAccessKeyId" ||\
        "${AWS_SECRET_ACCESS_KEY}" = "DummySecretAccessKey" ]]; then
    echo "Both AWS_ACCESS_KEY_ID and AWS_SECRET_KEY_ID
      need to be assigned valid values and belong to the owner of the s3
      bucket in order to access the file system"
    return 1
  fi
  # Check if the s3 bucket is NULL.
  if [[ "${S3_BUCKET}" = "" ]]; then
    echo "The ${S3_BUCKET} cannot be an empty string for s3"
    return 1
  fi
  aws s3 ls "s3://${S3_BUCKET}/" 1>/dev/null
  if [ $? != 0 ]; then
    echo "Access to ${S3_BUCKET} failed."
    return 1
  fi
  # At this point, we've verified that:
  #   - All the required environment variables are set.
  #   - We are able to talk to the s3 bucket with the credentials provided.
  export FILESYSTEM_PREFIX="s3a://${S3_BUCKET}"
fi

# Directory where local cluster logs will go when running tests or loading data
export IMPALA_TEST_CLUSTER_LOG_DIR=${IMPALA_HOME}/cluster_logs
# Reduce the concurrency for local tests to half the number of cores in the system.
# Note than nproc may not be available on older distributions (centos5.5)
if type nproc >/dev/null 2>&1; then
  CORES=$(($(nproc) / 2))
else
  CORES='4'
fi
export NUM_CONCURRENT_TESTS=${NUM_CONCURRENT_TESTS-${CORES}}

export IMPALA_GFLAGS_VERSION=2.0
export IMPALA_GPERFTOOLS_VERSION=2.0
export IMPALA_GLOG_VERSION=0.3.2
export IMPALA_GTEST_VERSION=1.6.0
export IMPALA_SNAPPY_VERSION=1.0.5
export IMPALA_CYRUS_SASL_VERSION=2.1.23
export IMPALA_OPENLDAP_VERSION=2.4.25
export IMPALA_SQUEASEL_VERSION=3.3

# Sasl has problems with 'make install' if the path contains a ~. In our
# packaging jobs, the path contains ~ so we'll just install somewhere else.
export IMPALA_CYRUS_SASL_INSTALL_DIR=/tmp/impala-build/cyrus-sasl-${IMPALA_CYRUS_SASL_VERSION}/build

export IMPALA_HADOOP_VERSION=2.6.0-cdh5.4.0-SNAPSHOT
export IMPALA_HBASE_VERSION=0.98.6-cdh5.4.0-SNAPSHOT
export IMPALA_HIVE_VERSION=1.1.0-cdh5.4.0-SNAPSHOT
export IMPALA_SENTRY_VERSION=1.4.0-cdh5.4.0-SNAPSHOT
export IMPALA_LLAMA_VERSION=1.0.0-cdh5.4.0-SNAPSHOT
export IMPALA_AVRO_VERSION=1.7.4
export IMPALA_PARQUET_VERSION=1.5.0-cdh5.4.0-SNAPSHOT
export IMPALA_THRIFT_VERSION=0.9.0
export IMPALA_LLVM_VERSION=3.3
export IMPALA_MINIKDC_VERSION=1.0.0

export IMPALA_FE_DIR=$IMPALA_HOME/fe
export IMPALA_BE_DIR=$IMPALA_HOME/be
export IMPALA_WORKLOAD_DIR=$IMPALA_HOME/testdata/workloads
export IMPALA_AUX_WORKLOAD_DIR=$IMPALA_AUX_TEST_HOME/testdata/workloads
export IMPALA_DATASET_DIR=$IMPALA_HOME/testdata/datasets
export IMPALA_AUX_DATASET_DIR=$IMPALA_AUX_TEST_HOME/testdata/datasets
export IMPALA_COMMON_DIR=$IMPALA_HOME/common
export PATH=$IMPALA_HOME/bin:$PATH

export HADOOP_HOME=$IMPALA_HOME/thirdparty/hadoop-${IMPALA_HADOOP_VERSION}/
export HADOOP_CONF_DIR=$IMPALA_FE_DIR/src/test/resources
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HADOOP_HOME/share/hadoop/tools/lib/*
export MINI_DFS_BASE_DATA_DIR=$IMPALA_HOME/cdh-${CDH_MAJOR_VERSION}-hdfs-data
export PATH=$HADOOP_HOME/bin:$PATH

export LLAMA_HOME=$IMPALA_HOME/thirdparty/llama-${IMPALA_LLAMA_VERSION}/
export MINIKDC_HOME=$IMPALA_HOME/thirdparty/llama-minikdc-${IMPALA_MINIKDC_VERSION}
export SENTRY_HOME=$IMPALA_HOME/thirdparty/sentry-${IMPALA_SENTRY_VERSION}
export SENTRY_CONF_DIR=$IMPALA_HOME/fe/src/test/resources

export HIVE_HOME=$IMPALA_HOME/thirdparty/hive-${IMPALA_HIVE_VERSION}/
export PATH=$HIVE_HOME/bin:$PATH
export HIVE_CONF_DIR=$IMPALA_FE_DIR/src/test/resources

### Hive looks for jar files in a single directory from HIVE_AUX_JARS_PATH plus
### any jars in AUX_CLASSPATH. (Or a list of jars in HIVE_AUX_JARS_PATH.)
export HIVE_AUX_JARS_PATH=${IMPALA_FE_DIR}/target
export AUX_CLASSPATH=$HADOOP_LZO/build/hadoop-lzo-0.4.15.jar

export HBASE_HOME=$IMPALA_HOME/thirdparty/hbase-${IMPALA_HBASE_VERSION}/
export PATH=$HBASE_HOME/bin:$PATH

# Add the jars so hive can create hbase tables.
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-common-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-client-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-server-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-protocol-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-hadoop-compat-${IMPALA_HBASE_VERSION}.jar

GPERFTOOLS_HOME=${IMPALA_HOME}/thirdparty/gperftools-${IMPALA_GPERFTOOLS_VERSION}/
export PPROF_PATH="${PPROF_PATH:-${GPERFTOOLS_HOME}/src/pprof}"
export HBASE_CONF_DIR=$HIVE_CONF_DIR

export THRIFT_SRC_DIR=${IMPALA_HOME}/thirdparty/thrift-${IMPALA_THRIFT_VERSION}/
export THRIFT_HOME=${THRIFT_SRC_DIR}build/

export CLUSTER_DIR=${IMPALA_HOME}/testdata/cluster

export IMPALA_BUILD_THREADS=`nproc`

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

export JAVA_LIBRARY_PATH=${IMPALA_HOME}/thirdparty/snappy-${IMPALA_SNAPPY_VERSION}/build/lib

# So that the frontend tests and PlanService can pick up libbackend.so
# and other required libraries
LIB_JAVA=`find ${JAVA_HOME}/   -name libjava.so | head -1`
LIB_JSIG=`find ${JAVA_HOME}/   -name libjsig.so | head -1`
LIB_JVM=` find ${JAVA_HOME}/   -name libjvm.so  | head -1`
LIB_HDFS=`find ${HADOOP_HOME}/ -name libhdfs.so | head -1`
LD_LIBRARY_PATH="${LD_LIBRARY_PATH-}"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:`dirname ${LIB_JAVA}`:`dirname ${LIB_JSIG}`"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:`dirname ${LIB_JVM}`:`dirname ${LIB_HDFS}`"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_HOME}/be/build/debug/service"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_HOME}/thirdparty/snappy-${IMPALA_SNAPPY_VERSION}/build/lib"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$IMPALA_LZO/build"
export LD_LIBRARY_PATH
LD_PRELOAD="${LD_PRELOAD-}"
export LD_PRELOAD="${LD_PRELOAD}:${LIB_JSIG}"

CLASSPATH="${CLASSPATH-}"
CLASSPATH=$IMPALA_FE_DIR/target/dependency:$CLASSPATH
CLASSPATH=$IMPALA_FE_DIR/target/classes:$CLASSPATH
CLASSPATH=$IMPALA_FE_DIR/src/test/resources:$CLASSPATH
CLASSPATH=$HADOOP_LZO/build/hadoop-lzo-0.4.15.jar:$CLASSPATH
export CLASSPATH

# Setup aliases
# Helper alias to script that verifies and merges Gerrit changes
alias gerrit-verify-merge="${IMPALA_AUX_TEST_HOME}/jenkins/gerrit-verify-merge.sh"

# A marker in the environment to prove that we really did source this file
export IMPALA_CONFIG_SOURCED=1

echo "IMPALA_HOME            = $IMPALA_HOME"
echo "HADOOP_HOME            = $HADOOP_HOME"
echo "HADOOP_CONF_DIR        = $HADOOP_CONF_DIR"
echo "MINI_DFS_BASE_DATA_DIR = $MINI_DFS_BASE_DATA_DIR"
echo "HIVE_HOME              = $HIVE_HOME"
echo "HIVE_CONF_DIR          = $HIVE_CONF_DIR"
echo "HBASE_HOME             = $HBASE_HOME"
echo "HBASE_CONF_DIR         = $HBASE_CONF_DIR"
echo "MINIKDC_HOME           = $MINIKDC_HOME"
echo "PPROF_PATH             = $PPROF_PATH"
echo "THRIFT_HOME            = $THRIFT_HOME"
echo "HADOOP_LZO             = $HADOOP_LZO"
echo "IMPALA_LZO             = $IMPALA_LZO"
echo "CLASSPATH              = $CLASSPATH"
echo "LIBHDFS_OPTS           = $LIBHDFS_OPTS"
echo "PYTHONPATH             = $PYTHONPATH"
echo "JAVA_HOME              = $JAVA_HOME"
echo "LD_LIBRARY_PATH        = $LD_LIBRARY_PATH"
echo "LD_PRELOAD             = $LD_PRELOAD"

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
