# - Find HDFS (hdfs.h and libhdfs.so)
# This module defines
#  Hadoop_VERSION, version string of ant if found
#  HDFS_INCLUDE_DIR, directory containing hdfs.h
#  HDFS_LIBS, location of libhdfs.so
#  HDFS_FOUND, If false, do not try to use ant

exec_program(hadoop ARGS version OUTPUT_VARIABLE Hadoop_VERSION
             RETURN_VALUE Hadoop_RETURN)

# currently only looking in HADOOP_HOME
find_path(HDFS_INCLUDE_DIR hdfs.h PATHS
  $ENV{HADOOP_HOME}/src/c++/libhdfs
)

set(HDFS_LIB_PATHS
  $ENV{HADOOP_HOME}/c++/Linux-amd64-64/lib
)

find_library(HDFS_LIB NAMES hdfs PATHS ${HDFS_LIB_PATHS})

if (HDFS_LIB)
  set(HDFS_FOUND TRUE)
  set(HDFS_LIBS ${HDFS_LIB})
else ()
  set(HDFS_FOUND FALSE)
endif ()

if (HDFS_FOUND)
  if (NOT HDFS_FIND_QUIETLY)
    message(STATUS "${Hadoop_VERSION}")
  endif ()
else ()
  message(STATUS "HDFS includes and libraries NOT found."
    "Thrift support will be disabled (${Thrift_RETURN}, "
    "${HDFS_INCLUDE_DIR}, ${HDFS_LIB})")
endif ()

mark_as_advanced(
  HDFS_LIBS
  HDFS_INCLUDE_DIR
)
