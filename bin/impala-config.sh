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

export JAVA_HOME=${JAVA_HOME-/usr/java/default}
if [ ! -d $JAVA_HOME ] ; then
    echo "Error! JAVA_HOME must be set to the location of your JDK!"
    exit 1
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

# Directory where local cluster logs will go when running tests or loading data
export IMPALA_TEST_CLUSTER_LOG_DIR=${IMPALA_HOME}/cluster_logs

export IMPALA_GFLAGS_VERSION=2.0
export IMPALA_GPERFTOOLS_VERSION=2.0
export IMPALA_GLOG_VERSION=0.3.2
export IMPALA_GTEST_VERSION=1.6.0
export IMPALA_SNAPPY_VERSION=1.0.5
export IMPALA_CYRUS_SASL_VERSION=2.1.23
export IMPALA_OPENLDAP_VERSION=2.4.25
export IMPALA_SQUEASEL_VERSION=3.3

export IMPALA_HADOOP_VERSION=2.2.0-cdh5.0.0-SNAPSHOT
export IMPALA_HBASE_VERSION=0.96.1.1-cdh5.0.0-SNAPSHOT
export IMPALA_HIVE_VERSION=0.12.0-cdh5.0.0-SNAPSHOT
export IMPALA_SENTRY_VERSION=1.2.0-cdh5.0.0-SNAPSHOT
export IMPALA_LLAMA_VERSION=1.0.0-cdh5.0.0-SNAPSHOT

export IMPALA_AVRO_VERSION=1.7.4
export IMPALA_PARQUET_VERSION=1.2.5
export IMPALA_LLAMA_VERSION=1.0.0-cdh5.0.0-SNAPSHOT
export IMPALA_THRIFT_VERSION=0.9.0
export IMPALA_LLVM_VERSION=3.3

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
export MINI_DFS_BASE_DATA_DIR=$IMPALA_HOME/cdh-${CDH_MAJOR_VERSION}-hdfs-data
export PATH=$HADOOP_HOME/bin:$PATH

export LLAMA_HOME=$IMPALA_HOME/thirdparty/llama-${IMPALA_LLAMA_VERSION}/

export HIVE_HOME=$IMPALA_HOME/thirdparty/hive-${IMPALA_HIVE_VERSION}/
export PATH=$HIVE_HOME/bin:$PATH
export HIVE_CONF_DIR=$IMPALA_FE_DIR/src/test/resources

### Hive looks for jar files in a single directory from HIVE_AUX_JARS_PATH plus
### any jars in AUX_CLASSPATH. (Or a list of jars in HIVE_AUX_JARS_PATH.)
export HIVE_AUX_JARS_PATH=$IMPALA_FE_DIR/target
export AUX_CLASSPATH=$HADOOP_LZO/build/hadoop-lzo-0.4.15.jar

# Add the jars so hive can create hbase tables.
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-common-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-client-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-server-${IMPALA_HBASE_VERSION}.jar
export AUX_CLASSPATH=$AUX_CLASSPATH:$HBASE_HOME/lib/hbase-protocol-${IMPALA_HBASE_VERSION}.jar

export HBASE_HOME=$IMPALA_HOME/thirdparty/hbase-${IMPALA_HBASE_VERSION}/
export PATH=$HBASE_HOME/bin:$PATH

GPERFTOOLS_HOME=${IMPALA_HOME}/thirdparty/gperftools-${IMPALA_GPERFTOOLS_VERSION}/
export PPROF_PATH="${PPROF_PATH:-${GPERFTOOLS_HOME}/src/pprof}"
export HBASE_CONF_DIR=$HIVE_CONF_DIR

export THRIFT_SRC_DIR=${IMPALA_HOME}/thirdparty/thrift-${IMPALA_THRIFT_VERSION}/
export THRIFT_HOME=${THRIFT_SRC_DIR}build/

export IMPALA_BUILD_THREADS=`nproc`

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
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:`dirname ${LIB_JAVA}`:`dirname ${LIB_JSIG}`"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:`dirname ${LIB_JVM}`:`dirname ${LIB_HDFS}`"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_HOME}/be/build/debug/service"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${IMPALA_HOME}/thirdparty/snappy-${IMPALA_SNAPPY_VERSION}/build/lib"
LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:$IMPALA_LZO/build"
export LD_LIBRARY_PATH
export LD_PRELOAD="${LD_PRELOAD}:${LIB_JSIG}"

CLASSPATH=$IMPALA_FE_DIR/target/dependency:$CLASSPATH
CLASSPATH=$IMPALA_FE_DIR/target/classes:$CLASSPATH
CLASSPATH=$IMPALA_FE_DIR/src/test/resources:$CLASSPATH
CLASSPATH=$HADOOP_LZO/build/hadoop-lzo-0.4.15.jar:$CLASSPATH
export CLASSPATH

# Setup aliases
# Helper alias to script that verifies and merges Gerrit changes
alias gerrit-verify-merge="${IMPALA_AUX_TEST_HOME}/jenkins/gerrit-verify-merge.sh"

echo "IMPALA_HOME            = $IMPALA_HOME"
echo "HADOOP_HOME            = $HADOOP_HOME"
echo "HADOOP_CONF_DIR        = $HADOOP_CONF_DIR"
echo "MINI_DFS_BASE_DATA_DIR = $MINI_DFS_BASE_DATA_DIR"
echo "HIVE_HOME              = $HIVE_HOME"
echo "HIVE_CONF_DIR          = $HIVE_CONF_DIR"
echo "HBASE_HOME             = $HBASE_HOME"
echo "HBASE_CONF_DIR         = $HBASE_CONF_DIR"
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
