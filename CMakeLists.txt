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

cmake_minimum_required(VERSION 3.2.3)

# Explicitly define project() to allow modifying the compiler before the project is
# initialized.
project(Impala)

include(cmake_modules/kudu_cmake_fns.txt)

if (NOT DEFINED BUILD_SHARED_LIBS)
  set(BUILD_SHARED_LIBS OFF)
endif()

# Build compile commands database
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# Determine the build type. If no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE DEBUG)
endif(NOT CMAKE_BUILD_TYPE)

STRING (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

message(STATUS "Build type is ${CMAKE_BUILD_TYPE}")

set(ENABLE_CODE_COVERAGE false)
if ("${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_DEBUG")
  set(CMAKE_BUILD_TYPE DEBUG)
  set(ENABLE_CODE_COVERAGE true)
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_RELEASE")
  set(CMAKE_BUILD_TYPE RELEASE)
  set(ENABLE_CODE_COVERAGE true)
endif()

message(STATUS "ENABLE_CODE_COVERAGE: ${ENABLE_CODE_COVERAGE}")

# Helper function that given a package name constructs the package_ROOT variable based on
# the version number extracted from the environment
function(set_dep_root NAME)
  string(TOLOWER ${NAME} NAME_LOWER)
  string(REPLACE "_" "-" NAME_LOWER ${NAME_LOWER})
  set(VAL_NAME "IMPALA_${NAME}_VERSION")
  set(${NAME}_ROOT $ENV{IMPALA_TOOLCHAIN}/${NAME_LOWER}-$ENV{${VAL_NAME}} PARENT_SCOPE)
endfunction()

# Define root path for all dependencies, this is in the form of
# set_dep_root(PACKAGE) ->
#   PACKAGE_ROOT set to $ENV{IMPALA_TOOLCHAIN}/PACKAGE-$ENV{IMPALA_PACKAGE_VERSION}
set_dep_root(AVRO)
set_dep_root(BOOST)
set_dep_root(BREAKPAD)
set_dep_root(BZIP2)
set_dep_root(CRCUTIL)
set_dep_root(FLATBUFFERS)
set_dep_root(GCC)
set_dep_root(GFLAGS)
set_dep_root(GLOG)
set_dep_root(GPERFTOOLS)
set_dep_root(GTEST)
set_dep_root(LIBEV)
set_dep_root(LLVM)
set(LLVM_DEBUG_ROOT $ENV{IMPALA_TOOLCHAIN}/llvm-$ENV{IMPALA_LLVM_DEBUG_VERSION})
set_dep_root(LZ4)
set_dep_root(OPENLDAP)
set_dep_root(PROTOBUF)
set_dep_root(RE2)
set_dep_root(RAPIDJSON)
set_dep_root(SNAPPY)
set_dep_root(THRIFT)
set_dep_root(ZLIB)
if (APPLE)
  set_dep_root(OPENSSL)
endif()

# The boost-cmake project hasn't been maintained for years. Let's make sure we
# don't accidentally use it if it can be found.
set(Boost_NO_BOOST_CMAKE ON)

set(Boost_USE_STATIC_LIBS NOT ${BUILD_SHARED_LIBS})
set(Boost_USE_STATIC_RUNTIME ON)

# Newer versions of boost (including the version in toolchain) don't build separate
# multithreaded versions (they always are). Make sure to pick those up.
# TODO: understand the consequence of leaving this ON (the default value).
set(Boost_USE_MULTITHREADED OFF)

# The casing and underscoring expected for these properties varies between
# versions of CMake. Multiple inconsistent versions may be present here
# intentionally to provide what a wide range of versions expects.
set(Boost_NO_SYSTEM_PATHS true)
set(BOOST_LIBRARYDIR ${BOOST_ROOT}/lib)
set(BOOST_INCLUDEDIR ${BOOST_ROOT}/include)
set(Boost_INCLUDE_DIR ${BOOST_INCLUDEDIR})

if (CMAKE_DEBUG)
  set(Boost_DEBUG TRUE)
endif()

find_package(Boost REQUIRED COMPONENTS thread regex filesystem system date_time)
include_directories(${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})
message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
set(OPENSSL_LIBS ${OPENSSL_SSL} ${OPENSSL_CRYPTO})
message(STATUS "OpenSSL: ${OPENSSL_LIBS}")

find_package(Bzip2 REQUIRED)
include_directories(${BZIP2_INCLUDE_DIR})

find_package(Zlib REQUIRED)
include_directories(${ZLIB_INCLUDE_DIR})
message(STATUS "zlib: Static -> ${ZLIB_STATIC}, Other -> ${ZLIB_LIBRARIES}")

if(NOT BUILD_SHARED_LIBS)
  # Panic if we cannot find the static libraries as they are supposed to be
  # in the toolchain.
  if (NOT BZIP2_STATIC_FOUND)
    message(FATAL_ERROR "bzip2 static library libbz2.a not found in $ENV{IMPALA_TOOLCHAIN}")
  endif()
  if (NOT ZLIB_STATIC_FOUND)
    message(FATAL_ERROR "zlib static library libz.a not found in $ENV{IMPALA_TOOLCHAIN}")
  endif()

  set(LIBBZ2 BZIP2_STATIC)
  set(LIBZ ZLIB_STATIC)
else()
  set(LIBBZ2 ${BZIP2_LIBRARIES})
  set(LIBZ ${ZLIB_LIBRARIES})
endif ()

# find HDFS headers and libs
find_package(HDFS REQUIRED)
include_directories(${HDFS_INCLUDE_DIR})

if(BUILD_SHARED_LIBS)
  set(LIBS ${LIBS} ${HDFS_LIBRARIES})
  set(HDFS_LIB ${HDFS_LIBRARIES})
else()
  set(LIBS ${LIBS} HDFS_STATIC)
  set(HDFS_LIB HDFS_STATIC)
endif()

# find GLog headers and libs.  Must include glog headers before the other
# google libraries.  They all have a config.h and we want glog's to be picked
# up first.
find_package(GLog REQUIRED)
include_directories(${GLOG_INCLUDE_DIR})
set(LIBS ${LIBS} ${GLOG_LIBS})
message(STATUS "GLog include dir: " ${GLOG_INCLUDE_DIR})

# find GFlags headers and libs (needed for GLog)
find_package(GFlags REQUIRED)
include_directories(${GFLAGS_INCLUDE_DIR})
set(LIBS ${LIBS} ${GFLAGS_LIBS})
message(STATUS "GFlags include dir: " ${GFLAGS_INCLUDE_DIR})
message(STATUS "GFlags lib dir: " ${GFLAGS_LIBS})

# find PProf libs
find_package(PProf REQUIRED)
include_directories(${PPROF_INCLUDE_DIR})
set (LIBS ${LIBS} ${PPROF_LIBRARIES})
message(STATUS "PProf include dir: " ${PPROF_INCLUDE_DIR})
message(STATUS "PProf static lib: " ${PPROF_STATIC_LIB})

# find GTest headers and libs
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
set(LIBS ${LIBS} ${GTEST_LIBRARIES})

message(STATUS "GTest include dir: " ${GTEST_INCLUDE_DIR})
message(STATUS "GTest library: " ${GTEST_LIBRARY})

# Use LLVM release binaries.
set(LLVM_BINARIES_ROOT ${LLVM_ROOT})
find_package(LlvmBinaries REQUIRED)

# Find LLVM libraries to link against.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "TIDY"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
  # Use the LLVM libaries with assertions for debug builds.
  set(LLVM_ROOT ${LLVM_DEBUG_ROOT})
endif()
message(STATUS "LLVM_ROOT: " ${LLVM_ROOT})

find_package(Llvm REQUIRED)
include_directories(${LLVM_INCLUDE_DIR})
set(LIBS ${LIBS} ${LLVM_MODULE_LIBS})

# find Sasl
find_package(Sasl REQUIRED)
include_directories(${SASL_INCLUDE_DIR})
add_library(sasllib SHARED IMPORTED)
set_target_properties(sasllib PROPERTIES IMPORTED_LOCATION ${SASL_SHARED_LIB})
set(SASL_LIBRARY sasllib)
message(STATUS "Sasl include dir: " ${SASL_INCLUDE_DIR})
message(STATUS "Sasl library: " ${SASL_LIBRARY})

# find openldap
find_package(Ldap REQUIRED)
include_directories(${LDAP_INCLUDE_DIR})
set(LDAP_LIBRARY ldapstatic)
set(LBER_LIBRARY lberstatic)
message(STATUS "LDAP: ${LDAP_INCLUDE_DIR}")

# The environment variable $THRIFT_HOME is set in impala-config.sh
# Make sure it's consistent with $THRIFT_ROOT.
if (NOT ($ENV{THRIFT_HOME} STREQUAL ${THRIFT_ROOT}))
  message(FATAL_ERROR "THRIFT_ROOT (${THRIFT_ROOT}) differs from environment "
      "variable THRIFT_HOME ($ENV{THRIFT_HOME}).")
endif()
# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(${THRIFT_INCLUDE_DIR})
set(LIBS ${LIBS} ${THRIFT_LIBS})
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift contrib dir: ${THRIFT_CONTRIB_DIR}")
message(STATUS "Thrift library path: ${THRIFT_LIBS}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")

# find flatbuffers headers, lib and compiler
find_package(FlatBuffers REQUIRED)
include_directories(${FLATBUFFERS_INCLUDE_DIR})
set(LIBS ${LIBS} ${FLATBUFFERS_LIBS})
message(STATUS "FlatBuffers include dir: ${FLATBUFFERS_INCLUDE_DIR}")
message(STATUS "FlatBuffers library path: ${FLATBUFFERS_LIBS}")
message(STATUS "FlatBuffers compiler: ${FLATBUFFERS_COMPILER}")

# find Snappy headers and libs
find_package(Snappy REQUIRED)
include_directories(${SNAPPY_INCLUDE_DIR})
set(LIBS ${LIBS} ${SNAPPY_LIBRARIES})
message(STATUS "Snappy include dir: " ${SNAPPY_INCLUDE_DIR})
message(STATUS "Snappy library: " "${SNAPPY_STATIC_LIB}")

# find lz4 lib
find_package(Lz4 REQUIRED)
include_directories(${LZ4_INCLUDE_DIR})
set(LIBS ${LIBS} ${LZ4_LIBRARIES})
message(STATUS "Lz4 include dir: " ${LZ4_INCLUDE_DIR})
message(STATUS "Lz4 library: " "${LZ4_STATIC_LIB}")

# find re2 headers and libs
find_package(Re2 REQUIRED)
include_directories(${RE2_INCLUDE_DIR})
set(LIBS ${LIBS} ${RE2_LIBRARIES})
message(STATUS "Re2 include dir: " ${RE2_INCLUDE_DIR})
message(STATUS "Re2 library: " ${RE2_STATIC_LIB})

# find rapidjson headers
find_package(RapidJson REQUIRED)
include_directories(${RAPIDJSON_INCLUDE_DIR})
message(STATUS "RapidJson include dir: " ${RAPIDJSON_INCLUDE_DIR})

# find Avro headers and libs
find_package(Avro REQUIRED)
include_directories(${AVRO_INCLUDE_DIR})
set(LIBS ${LIBS} ${AVRO_STATIC_LIB})
message(STATUS "Avro include dir: " ${AVRO_INCLUDE_DIR})
message(STATUS "Avro static library: " ${AVRO_STATIC_LIB})

###################################################################
# These dependencies use Kudu's CMake functions

ADD_THIRDPARTY_LIB(glog
  STATIC_LIB ${GLOG_STATIC_LIB})
ADD_THIRDPARTY_LIB(gflags
  STATIC_LIB ${GFLAGS_STATIC_LIB})
ADD_THIRDPARTY_LIB(zlib
  STATIC_LIB ${ZLIB_STATIC_LIBRARIES})
ADD_THIRDPARTY_LIB(cyrus_sasl
  SHARED_LIB ${SASL_SHARED_LIB})

if (NOT APPLE)
  find_library(RT_LIB_PATH rt)
  if(NOT RT_LIB_PATH)
    message(FATAL_ERROR "Could not find librt on the system path")
  endif()
  ADD_THIRDPARTY_LIB(rt
    SHARED_LIB "${RT_LIB_PATH}")

  find_library(DL_LIB_PATH dl)
  if(NOT DL_LIB_PATH)
    message(FATAL_ERROR "Could not find libdl on the system path")
  endif()
  ADD_THIRDPARTY_LIB(dl
    SHARED_LIB "${DL_LIB_PATH}")
endif()

# find protobuf headers, libs and compiler
find_package(Protobuf REQUIRED)
include_directories(SYSTEM ${PROTOBUF_INCLUDE_DIR})
message(STATUS "Protobuf include dir: " ${PROTOBUF_INCLUDE_DIR})

ADD_THIRDPARTY_LIB(protobuf
  STATIC_LIB "${PROTOBUF_STATIC_LIBRARY}")
ADD_THIRDPARTY_LIB(protoc
  STATIC_LIB "${PROTOBUF_PROTOC_STATIC_LIBRARY}"
  DEPS protobuf)

find_package(LibEv REQUIRED)
include_directories(SYSTEM ${LIBEV_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(libev
  STATIC_LIB "${LIBEV_STATIC_LIB}")

find_package(Crcutil REQUIRED)
include_directories(SYSTEM ${CRCUTIL_INCLUDE_DIR})
ADD_THIRDPARTY_LIB(crcutil
  STATIC_LIB "${CRCUTIL_STATIC_LIB}")

###################################################################

# KuduClient can use GLOG
add_definitions(-DKUDU_HEADERS_USE_GLOG)
if(NOT $ENV{KUDU_CLIENT_DIR} EQUAL "")
  set(kuduClient_DIR "$ENV{KUDU_CLIENT_DIR}/usr/local/share/kuduClient/cmake")
else()
  set(kuduClient_DIR "$ENV{IMPALA_TOOLCHAIN}/kudu-$ENV{IMPALA_KUDU_VERSION}")
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(kuduClient_DIR "${kuduClient_DIR}/debug/share/kuduClient/cmake")
  else()
    set(kuduClient_DIR "${kuduClient_DIR}/release/share/kuduClient/cmake")
  endif()
endif()
# When KUDU_IS_SUPPORTED is false, the Kudu client is expected to be a non-functional
# stub. It's still needed to link though.
find_package(kuduClient REQUIRED NO_DEFAULT_PATH)
include_directories(SYSTEM ${KUDU_CLIENT_INCLUDE_DIR})

# find jni headers and libs
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
set(LIBS ${LIBS} ${JNI_LIBRARIES})
message(STATUS "JNI_INCLUDE_DIRS: ${JNI_INCLUDE_DIRS}")
message(STATUS "JNI_LIBRARIES: ${JNI_LIBRARIES}")

# find breakpad headers and libs
find_package(Breakpad REQUIRED)
include_directories(${BREAKPAD_INCLUDE_DIR})
set(LIBS ${LIBS} ${BREAKPAD_LIBRARIES})
message(STATUS "Breakpad include dir: " ${BREAKPAD_INCLUDE_DIR})
message(STATUS "Breakpad library: " ${BREAKPAD_STATIC_LIB})

# compile these subdirs using their own CMakeLists.txt
add_subdirectory(common/function-registry)
add_subdirectory(common/thrift)
add_subdirectory(be)
add_subdirectory(fe)
add_subdirectory(ext-data-source)

add_custom_target(tarballs ALL DEPENDS shell_tarball)

add_custom_target(shell_tarball DEPENDS thrift-deps
  COMMAND "${CMAKE_SOURCE_DIR}/shell/make_shell_tarball.sh"
)

add_custom_target(cscope ALL
  COMMAND "${CMAKE_SOURCE_DIR}/bin/gen-cscope.sh"
)

if (DEFINED ENV{IMPALA_LZO} AND EXISTS $ENV{IMPALA_LZO})
  add_custom_target(impala-lzo ALL DEPENDS thrift-deps
    COMMAND $ENV{IMPALA_LZO}/build.sh ${CMAKE_BUILD_TYPE} ${CMAKE_SOURCE_DIR}
    $ENV{IMPALA_TOOLCHAIN}
  )
endif()

# Dump include paths to a file
if (DUMP_INCLUDE_PATHS)
  file(REMOVE "${DUMP_INCLUDE_PATHS}")
  get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
  foreach(dir ${dirs})
    file(APPEND "${DUMP_INCLUDE_PATHS}" "${dir}\n")
  endforeach()
endif(DUMP_INCLUDE_PATHS)

SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libstdc++")
