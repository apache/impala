# Welcome to Impala

Lightning-fast, distributed [SQL](http://en.wikipedia.org/wiki/SQL) queries for petabytes
of data stored in Apache Hadoop clusters.

Impala is a modern, massively-distributed, massively-parallel, C++ query engine that lets
you analyze, transform and combine data from a variety of data sources:

* Best of breed performance and scalability.
* Support for data stored in [HDFS](https://hadoop.apache.org/),
  [Apache HBase](http://hbase.apache.org/) and [Amazon S3](http://aws.amazon.com/s3/).
* Wide analytic SQL support, including window functions and subqueries.
* On-the-fly code generation using [LLVM](http://llvm.org/) to generate CPU-efficient
  code tailored specifically to each individual query.
* Support for the most commonly-used Hadoop file formats, including the
  [Apache Parquet](https://parquet.apache.org/) project.
* Apache-licensed, 100% open source.

## More about Impala

To learn more about Impala as a business user, or to try Impala live or in a VM, please
visit the [Impala homepage](https://impala.apache.org).

If you are interested in contributing to Impala as a developer, or learning more about
Impala's internals and architecture, visit the
[Impala wiki](https://cwiki.apache.org/confluence/display/IMPALA/Impala+Home).

## Supported Platforms

Impala only supports Linux at the moment.

## Export Control Notice

This distribution uses cryptographic software and may be subject to export controls.
Please refer to EXPORT\_CONTROL.md for more information.

## Build Instructions

See bin/bootstrap_build.sh.

### Detailed Build Notes

Impala can be built with pre-built components or components downloaded from S3.
The components needed to build Impala are Apache Hadoop, Hive, HBase, and Sentry.
If you need to manually override the locations or versions of these components, you
can do so through the environment variables and scripts listed below.

##### Scripts and directories

| Location                     | Purpose |
|------------------------------|---------|
| bin/impala-config.sh         | This script must be sourced to setup all environment variables properly to allow other scripts to work |
| bin/impala-config-local.sh   | A script can be created in this location to set local overrides for any environment variables |
| bin/impala-config-branch.sh  | A version of the above that can be checked into a branch for convenience. |
| bin/bootstrap_build.sh       | A helper script to bootstrap some of the build requirements. |
| bin/bootstrap_development.sh | A helper script to bootstrap a developer environment.  Please read it before using. |
| be/build/ | Impala build output goes here. |
| be/generated-sources/ | Thrift and other generated source will be found here. |

##### Build Related Variables

| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| IMPALA_HOME          |               | Top level Impala directory |
| IMPALA_TOOLCHAIN     | "${IMPALA_HOME}/toolchain" | Native toolchain directory (for compilers, libraries, etc.) |
| SKIP_TOOLCHAIN_BOOTSTRAP | "false" | Skips downloading the toolchain any python dependencies if "true" |
| CDH_BUILD_NUMBER | | Identifier to indicate the CDH build number
| CDH_COMPONENTS_HOME | "${IMPALA_HOME}/toolchain/cdh_components-${CDH_BUILD_NUMBER}" | Location of the CDH components within the toolchain. |
| CDH_MAJOR_VERSION | "5" | Identifier used to uniqueify paths for potentially incompatible component builds. |
| IMPALA_CONFIG_SOURCED | "1" |  Set by ${IMPALA_HOME}/bin/impala-config.sh (internal use) |
| JAVA_HOME | "/usr/lib/jvm/${JAVA_VERSION}" | Used to locate Java |
| JAVA_VERSION | "java-7-oracle-amd64" | Can override to set a local Java version. |
| JAVA | "${JAVA_HOME}/bin/java" | Java binary location. |
| CLASSPATH | | See bin/set-classpath.sh for details. |
| PYTHONPATH |  Will be changed to include: "${IMPALA_HOME}/shell/gen-py" "${IMPALA_HOME}/testdata" "${THRIFT_HOME}/python/lib/python2.7/site-packages" "${HIVE_HOME}/lib/py" "${IMPALA_HOME}/shell/ext-py/prettytable-0.7.1/dist/prettytable-0.7.1" "${IMPALA_HOME}/shell/ext-py/sasl-0.1.1/dist/sasl-0.1.1-py2.7-linux-x "${IMPALA_HOME}/shell/ext-py/sqlparse-0.1.19/dist/sqlparse-0.1.19-py2 |

##### Source Directories for Impala

| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| IMPALA_BE_DIR        |  "${IMPALA_HOME}/be" | Backend directory.  Build output is also stored here. |
| IMPALA_FE_DIR        |  "${IMPALA_HOME}/fe" | Frontend directory |
| IMPALA_COMMON_DIR    |  "${IMPALA_HOME}/common" | Common code (thrift, function registry) |

##### Various Compilation Settings

| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| IMPALA_BUILD_THREADS | "8" or set to number of processors by default. | Used for make -j and distcc -j settings. |
| IMPALA_MAKE_FLAGS    | "" | Any extra settings to pass to make.  Also used when copying udfs / udas into HDFS. |
| USE_SYSTEM_GCC       | "0" | If set to any other value, directs cmake to not set GCC_ROOT, CMAKE_C_COMPILER, CMAKE_CXX_COMPILER, as well as setting TOOLCHAIN_LINK_FLAGS |
| IMPALA_CXX_COMPILER  | "default" | Used by cmake (cmake_modules/toolchain and clang_toolchain.cmake) to select gcc / clang |
| USE_GOLD_LINKER      | "true" | Directs backend cmake to use gold. |
| IS_OSX               | "false" | (Experimental) currently only used to disable Kudu. |

##### Dependencies
| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| HADOOP_HOME          | "${CDH_COMPONENTS_HOME}/hadoop-${IMPALA_HADOOP_VERSION}/" | Used to locate Hadoop |
| HADOOP_INCLUDE_DIR   | "${HADOOP_HOME}/include" | For 'hdfs.h' |
| HADOOP_LIB_DIR       | "${HADOOP_HOME}/lib" | For 'libhdfs.a' or 'libhdfs.so' |
| HIVE_HOME            | "${CDH_COMPONENTS_HOME}/{hive-${IMPALA_HIVE_VERSION}/" | |
| HBASE_HOME           | "${CDH_COMPONENTS_HOME}/hbase-${IMPALA_HBASE_VERSION}/" | |
| SENTRY_HOME          | "${CDH_COMPONENTS_HOME}/sentry-${IMPALA_SENTRY_VERSION}/" | Used to setup test data |
| THRIFT_HOME          | "${IMPALA_TOOLCHAIN}/thrift-${IMPALA_THRIFT_VERSION}" | |
