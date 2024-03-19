This document introduces the Impala project layout and some key configuration variables.
Beware that it may become stale over time as the project evolves.

# Detailed Build Notes

Impala can be built with pre-built components or components downloaded from S3.
The components needed to build Impala are Apache Hadoop, Hive, and HBase.
If you need to manually override the locations or versions of these components, you
can do so through the environment variables and scripts listed below.

## Scripts and directories

| Location                     | Purpose |
|------------------------------|---------|
| bin/impala-config.sh         | This script must be sourced to setup all environment variables properly to allow other scripts to work |
| bin/impala-config-local.sh   | A script can be created in this location to set local overrides for any environment variables |
| bin/impala-config-branch.sh  | A version of the above that can be checked into a branch for convenience. |
| bin/bootstrap_build.sh       | A helper script to bootstrap some of the build requirements. |
| bin/bootstrap_development.sh | A helper script to bootstrap a developer environment.  Please read it before using. |
| be/build/ | Impala build output goes here. |
| be/generated-sources/ | Thrift and other generated source will be found here. |

## Build Related Variables

| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| IMPALA_HOME          |               | Top level Impala directory |
| IMPALA_TOOLCHAIN     | "${IMPALA_HOME}/toolchain" | Native toolchain directory (for compilers, libraries, etc.) |
| SKIP_TOOLCHAIN_BOOTSTRAP | "false" | Skips downloading the toolchain any python dependencies if "true" |
| CDP_BUILD_NUMBER | | Identifier to indicate the CDP build number
| CDP_COMPONENTS_HOME | "${IMPALA_HOME}/toolchain/cdp_components-${CDP_BUILD_NUMBER}" | Location of the CDP components within the toolchain. |
| CDH_MAJOR_VERSION | "7" | Identifier used to uniqueify paths for potentially incompatible component builds. |
| IMPALA_CONFIG_SOURCED | "1" |  Set by ${IMPALA_HOME}/bin/impala-config.sh (internal use) |
| IMPALA_JAVA_HOME_OVERRIDE | | Specify a non-system Java version. Overrides IMPALA_JDK_VERSION behavior. |
| IMPALA_JDK_VERSION | "system" | Set to 8 or 11 to select a system Java version. Default will set JAVA_HOME based on the javac symlink in PATH. |
| JAVA | "${JAVA_HOME}/bin/java" | Java binary location. |
| CLASSPATH | | See bin/set-classpath.sh for details. |
| PYTHONPATH | | See bin/set-pythonpath.sh for details. |

## Source Directories for Impala

| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| IMPALA_BE_DIR        |  "${IMPALA_HOME}/be" | Backend directory.  Build output is also stored here. |
| IMPALA_FE_DIR        |  "${IMPALA_HOME}/fe" | Frontend directory |
| IMPALA_COMMON_DIR    |  "${IMPALA_HOME}/common" | Common code (thrift, function registry) |

## Various Compilation Settings

| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| IMPALA_BUILD_THREADS | "8" or set to number of processors by default. | Used for make -j and distcc -j settings. |
| IMPALA_MAKE_FLAGS    | "" | Any extra settings to pass to make.  Also used when copying udfs / udas into HDFS. |
| USE_SYSTEM_GCC       | "0" | If set to any other value, directs cmake to not set GCC_ROOT, CMAKE_C_COMPILER, CMAKE_CXX_COMPILER, as well as setting TOOLCHAIN_LINK_FLAGS |
| IMPALA_CXX_COMPILER  | "default" | Used by cmake (cmake_modules/toolchain and clang_toolchain.cmake) to select gcc / clang |
| USE_GOLD_LINKER      | "true" | Directs backend cmake to use gold. |
| IS_OSX               | "false" | (Experimental) currently only used to disable Kudu. |

## Dependencies
| Environment variable | Default value | Description |
|----------------------|---------------|-------------|
| HADOOP_HOME          | "${CDP_COMPONENTS_HOME}/hadoop-${IMPALA_HADOOP_VERSION}/" | Used to locate Hadoop |
| HADOOP_INCLUDE_DIR   | "${HADOOP_HOME}/include" | For 'hdfs.h' |
| HADOOP_LIB_DIR       | "${HADOOP_HOME}/lib" | For 'libhdfs.a' or 'libhdfs.so' |
| HIVE_HOME            | "${CDP_COMPONENTS_HOME}/{hive-${IMPALA_HIVE_VERSION}/" | |
| HBASE_HOME           | "${CDP_COMPONENTS_HOME}/hbase-${IMPALA_HBASE_VERSION}/" | |
| THRIFT_CPP_HOME      | "${IMPALA_TOOLCHAIN}/thrift-${IMPALA_THRIFT_CPP_VERSION}" | |
| THRIFT_JAVA_HOME     | "${IMPALA_TOOLCHAIN}/thrift-${IMPALA_THRIFT_JAVA_VERSION}" | |
| THRIFT_PY_HOME       | "${IMPALA_TOOLCHAIN}/thrift-${IMPALA_THRIFT_PY_VERSION}" | |

## Hive Dependency Overrides
Typically used together to specify a local build of Apache Hive. Care should be taken
while using these variables since they take precedence over the defaults in
impala-config.sh, they may cause confusion when switching between branches or versions of
Apache Impala.

| Environment variable | Description |
|----------------------|-------------|
| HIVE_VERSION_OVERRIDE | Used to specify different Hive version from default |
| HIVE_STORAGE_API_VERSION_OVERRIDE | Used to specify different Hive Storage API version from default |
| HIVE_METASTORE_THRIFT_DIR_OVERRIDE | Used to specify location of metastore thrift files to use during Thrift compilation |
| HIVE_HOME_OVERRIDE | Used to specify location of Hive |

## Ranger Dependency Overrides
Typically used together to specify a local build of Apache Ranger. Care should be taken
while using these variables since they take precedence over the defaults in
impala-config.sh.

| Environment variable | Description |
|----------------------|-------------|
| RANGER_VERSION_OVERRIDE | Used to specify different Ranger version from default |
| RANGER_HOME_OVERRIDE | Used to specify location of Ranger |
