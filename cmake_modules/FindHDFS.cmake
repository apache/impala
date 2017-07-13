##############################################################################
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
##############################################################################

# - Find HDFS (hdfs.h and libhdfs.so)
# This module defines
#  Hadoop_VERSION, version string of ant if found
#  HDFS_INCLUDE_DIR, directory containing hdfs.h
#  HDFS_LIBS, location of libhdfs.so
#  HDFS_FOUND, If false, do not try to use ant
#  hdfsstatic

exec_program(hadoop ARGS version OUTPUT_VARIABLE Hadoop_VERSION
             RETURN_VALUE Hadoop_RETURN)

# Only look in HADOOP_INCLUDE_DIR
find_path(HDFS_INCLUDE_DIR hdfs.h PATHS
  $ENV{HADOOP_INCLUDE_DIR}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

if ("${CMAKE_SIZEOF_VOID_P}" STREQUAL "8")
  set(arch_hint "x64")
elseif ("$ENV{LIB}" MATCHES "(amd64|ia64)")
  set(arch_hint "x64")
else ()
  set(arch_hint "x86")
endif()

set(HDFS_LIB_PATHS $ENV{HADOOP_LIB_DIR}/native)

if (NOT HDFS_FIND_QUIETLY)
  message(STATUS "Architecture: ${arch_hint}")
  message(STATUS "HDFS_LIB_PATHS: ${HDFS_LIB_PATHS}")
endif()

find_library(HDFS_STATIC_LIB NAMES libhdfs.a PATHS
  ${HDFS_LIB_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
  )
find_library(HDFS_SHARED_LIB NAMES libhdfs.so PATHS
  ${HDFS_LIB_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
  )

if (HDFS_STATIC_LIB AND HDFS_SHARED_LIB)
  set(HDFS_FOUND TRUE)
else ()
  set(HDFS_FOUND FALSE)
endif ()

if (HDFS_FOUND)
  if (NOT HDFS_FIND_QUIETLY)
    message(STATUS "${Hadoop_VERSION}")
    message(STATUS "HDFS_INCLUDE_DIR: ${HDFS_INCLUDE_DIR}")
    message(STATUS "HDFS_STATIC_LIB: ${HDFS_STATIC_LIB}")
    message(STATUS "HDFS_SHARED_LIB: ${HDFS_SHARED_LIB}")
  endif ()
else ()
  message(FATAL_ERROR "HDFS includes and libraries NOT found. "
    "${HDFS_INCLUDE_DIR}, ${HDFS_LIB})")
endif ()

mark_as_advanced(
  HDFS_STATIC_LIB
  HDFS_SHARED_LIB
  HDFS_INCLUDE_DIR
  HDFS_STATIC
)
