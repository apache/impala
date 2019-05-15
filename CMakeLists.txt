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

# This is a Kudu-specific flag that disables Kudu targets that are test-only.
set(NO_TESTS 1)

# Explicitly define project() to allow modifying the compiler before the project is
# initialized.
project(Impala)

include(cmake_modules/kudu_cmake_fns.txt)

if (NOT DEFINED BUILD_SHARED_LIBS)
  set(BUILD_SHARED_LIBS OFF)
endif()

# Store BUILD_SHARED_LIBS in a variable so it can be read in config.h.in
set(IMPALA_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})

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

# Write build flags to a file so that they can be read by tests
file(WRITE "${CMAKE_SOURCE_DIR}/.cmake_build_type" ${CMAKE_BUILD_TYPE}\n)
file(APPEND "${CMAKE_SOURCE_DIR}/.cmake_build_type" ${BUILD_SHARED_LIBS}\n)

# Store CMAKE_BUILD_TYPE in a variable so it can be read in config.h.in
string(REPLACE "_" "-" ESCAPED_CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE})
set(IMPALA_CMAKE_BUILD_TYPE ${ESCAPED_CMAKE_BUILD_TYPE})

set(ENABLE_CODE_COVERAGE false)
if ("${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_DEBUG")
  set(CMAKE_BUILD_TYPE DEBUG)
  set(ENABLE_CODE_COVERAGE true)
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_RELEASE")
  set(CMAKE_BUILD_TYPE RELEASE)
  set(ENABLE_CODE_COVERAGE true)
endif()

message(STATUS "ENABLE_CODE_COVERAGE: ${ENABLE_CODE_COVERAGE}")

if (ENABLE_CODE_COVERAGE
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
  set (SLOW_BUILD true)
endif()

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
set_dep_root(ORC)
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
set_dep_root(LIBUNWIND)
set_dep_root(LLVM)
set(LLVM_DEBUG_ROOT $ENV{IMPALA_TOOLCHAIN}/llvm-$ENV{IMPALA_LLVM_DEBUG_VERSION})
set_dep_root(LZ4)
set_dep_root(ZSTD)
set_dep_root(OPENLDAP)
set_dep_root(PROTOBUF)
set_dep_root(RE2)
set_dep_root(RAPIDJSON)
set_dep_root(SNAPPY)
set_dep_root(THRIFT)
set(THRIFT11_ROOT $ENV{IMPALA_TOOLCHAIN}/thrift-$ENV{IMPALA_THRIFT11_VERSION})
set_dep_root(ZLIB)
set_dep_root(CCTZ)

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

# Adds a third-party library with name ${NAME}. If BUILD_SHARED_LIBS is true, the new
# library refers to ${SHARED_LIB}; otherwise it refers to ${STATIC_LIB}. If only one
# library (static or shared) is provided, it is used regardless of BUILD_SHARED_LIBS. The
# library's headers are added to the system include path.
function(IMPALA_ADD_THIRDPARTY_LIB NAME HEADER STATIC_LIB SHARED_LIB)
  message(STATUS "----------> Adding thirdparty library ${NAME}. <----------")
  if (HEADER)
    include_directories(SYSTEM ${HEADER})
    message(STATUS "Header files: ${HEADER}")
  endif()
  if (NOT STATIC_LIB AND NOT SHARED_LIB)
    message(FATAL_ERROR "Library '${NAME}' has neither shared nor static library files")
    return ()
  endif()

  if ((BUILD_SHARED_LIBS AND SHARED_LIB) OR NOT STATIC_LIB)
    ADD_THIRDPARTY_LIB(${NAME} SHARED_LIB ${SHARED_LIB})
  else()
    ADD_THIRDPARTY_LIB(${NAME} STATIC_LIB ${STATIC_LIB})
  endif()
endfunction()


find_package(Boost REQUIRED COMPONENTS thread regex filesystem system date_time random)
# Mark Boost as a system header to avoid compile warnings.
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

# If at all possible, try to link with system OpenSSL in an attempt to match what the
# platform that this build might ultimately be deployed on. If no suitable OpenSSL is
# found, use the toolchain version - but beware that Impala will then fail to start if
# deployed on a system with a lower version.
find_package(OpenSSL 1.0.1 QUIET)
if (NOT ${OPENSSL_FOUND})
  set_dep_root(OPENSSL)
  set(OPENSSL_FOUND ON)
  set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT}/include)
  set(OPENSSL_SSL_LIBRARY ${OPENSSL_ROOT}/lib/libssl.so)
  set(OPENSSL_CRYPTO_LIBRARY ${OPENSSL_ROOT}/lib/libcrypto.so)
  set(OPENSSL_VERSION $ENV{IMPALA_OPENSSL_VERSION})
  message(STATUS "System OpenSSL not found. Defaulting to toolchain version in"
    " ${OPENSSL_ROOT_DIR}")
endif()

# OpenSSL, being a security dependency, is always dynamically linked.
IMPALA_ADD_THIRDPARTY_LIB(openssl_ssl ${OPENSSL_INCLUDE_DIR} "" ${OPENSSL_SSL_LIBRARY})
IMPALA_ADD_THIRDPARTY_LIB(openssl_crypto "" "" ${OPENSSL_CRYPTO_LIBRARY})

find_package(Bzip2 REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(bzip2 ${BZIP2_INCLUDE_DIR} ${BZIP2_STATIC_LIBRARIES} "")

find_package(Zlib REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(zlib ${ZLIB_INCLUDE_DIR} ${ZLIB_STATIC_LIBRARIES}
  ${ZLIB_SHARED_LIBRARIES})

# find HDFS headers and libs
set(HDFS_FIND_QUIETLY TRUE)
find_package(HDFS REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(hdfs ${HDFS_INCLUDE_DIR} ${HDFS_STATIC_LIB} ${HDFS_SHARED_LIB})

# find GLog headers and libs.  Must include glog headers before the other
# google libraries.  They all have a config.h and we want glog's to be picked
# up first.
find_package(GLog REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(glog ${GLOG_INCLUDE_DIR} ${GLOG_STATIC_LIB} ${GLOG_SHARED_LIB})

# find GFlags headers and libs
find_package(GFlags REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(gflags ${GFLAGS_INCLUDE_DIR} ${GFLAGS_STATIC_LIB}
  ${GFLAGS_SHARED_LIB})

# find PProf libs
find_package(PProf REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(pprof ${PPROF_INCLUDE_DIR} ${PPROF_STATIC_LIB} "")

# find GTest headers and libs
set (GTEST_FIND_QUIETLY TRUE)
find_package(GTest REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(gtest ${GTEST_INCLUDE_DIR} ${GTEST_STATIC_LIB} "")

# Use LLVM release binaries.
set(LLVM_BINARIES_ROOT ${LLVM_ROOT})
find_package(LlvmBinaries REQUIRED)

# Find LLVM libraries to link against.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "TIDY"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN_FULL"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
  # Use the LLVM libaries with assertions for debug builds.
  set(LLVM_ROOT ${LLVM_DEBUG_ROOT})
endif()
message(STATUS "LLVM_ROOT: " ${LLVM_ROOT})

find_package(Llvm REQUIRED)
include_directories(${LLVM_INCLUDE_DIR})

# find Sasl
set(SASL_FIND_QUIETLY TRUE)
find_package(Sasl REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(cyrus_sasl ${SASL_INCLUDE_DIR} "" ${SASL_SHARED_LIB})

# find openldap
find_package(Ldap REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(ldap ${LDAP_INCLUDE_DIR} ${LDAP_STATIC_LIBRARY} "")
IMPALA_ADD_THIRDPARTY_LIB(lber "" ${LBER_STATIC_LIBRARY} "")

# The environment variable $THRIFT_HOME is set in impala-config.sh
# Make sure it's consistent with $THRIFT_ROOT.
if (NOT ($ENV{THRIFT_HOME} STREQUAL ${THRIFT_ROOT}))
  message(FATAL_ERROR "THRIFT_ROOT (${THRIFT_ROOT}) differs from environment "
      "variable THRIFT_HOME ($ENV{THRIFT_HOME}).")
endif()
# find thrift headers and libs
set(THRIFT_FIND_QUIETLY TRUE)
find_package(Thrift REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(thrift ${THRIFT_INCLUDE_DIR} ${THRIFT_STATIC_LIB} "")
message(STATUS "Thrift version: ${THRIFT_VERSION}")
message(STATUS "Thrift contrib dir: ${THRIFT_CONTRIB_DIR}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")

if (NOT EXISTS ${THRIFT11_ROOT}/bin/thrift)
  message(FATAL_ERROR "Unable to find Thrift 11 compiler")
endif(NOT EXISTS ${THRIFT11_ROOT}/bin/thrift)

set(THRIFT11_COMPILER ${THRIFT11_ROOT}/bin/thrift)

# find flatbuffers headers, lib and compiler
find_package(FlatBuffers REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(flatbuffers ${FLATBUFFERS_INCLUDE_DIR}
  ${FLATBUFFERS_STATIC_LIB} "")
message(STATUS "FlatBuffers compiler: ${FLATBUFFERS_COMPILER}")

# find Snappy headers and libs
find_package(Snappy REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(snappy ${SNAPPY_INCLUDE_DIR} ${SNAPPY_STATIC_LIB} "")

# find lz4 lib
find_package(Lz4 REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(lz4 ${LZ4_INCLUDE_DIR} ${LZ4_STATIC_LIB} "")

# find zstd lib
find_package(Zstd REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(zstd ${ZSTD_INCLUDE_DIR} ${ZSTD_STATIC_LIB} "")

# find re2 headers and libs
find_package(Re2 REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(re2 ${RE2_INCLUDE_DIR} ${RE2_STATIC_LIB} "")

# find rapidjson headers
find_package(RapidJson REQUIRED)
include_directories(${RAPIDJSON_INCLUDE_DIR})
message(STATUS "RapidJson include dir: " ${RAPIDJSON_INCLUDE_DIR})

# find Avro headers and libs
find_package(Avro REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(avro ${AVRO_INCLUDE_DIR} ${AVRO_STATIC_LIB} "")

# find ORC headers and libs
find_package(Orc REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(orc ${ORC_INCLUDE_DIR} ${ORC_STATIC_LIB} "")

# find CCTZ headers and libs
find_package(Cctz REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(cctz ${CCTZ_INCLUDE_DIR} ${CCTZ_STATIC_LIB} "")

# find protobuf headers, libs and compiler
find_package(Protobuf REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(protobuf ${PROTOBUF_INCLUDE_DIR} ${PROTOBUF_STATIC_LIBRARY}
  ${PROTOBUF_SHARED_LIBRARY})
IMPALA_ADD_THIRDPARTY_LIB(protoc ${PROTOBUF_INCLUDE_DIR} ${PROTOBUF_PROTOC_STATIC_LIBRARY}
  ${PROTOBUF_PROTOC_SHARED_LIBRARY})

# find libev headers and libs
find_package(LibEv REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(libev ${LIBEV_INCLUDE_DIR} ${LIBEV_STATIC_LIB}
  ${LIBEV_SHARED_LIB})

# Find crcutil headers and libs
find_package(Crcutil REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(crcutil ${CRCUTIL_INCLUDE_DIR} ${CRCUTIL_STATIC_LIB}
  ${CRCUTIL_SHARED_LIB})

# find jni headers and libs
find_package(JNI REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(java_jvm "${JNI_INCLUDE_DIRS}" ${JAVA_JVM_LIBRARY} "")

# find breakpad headers and libs
find_package(Breakpad REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(breakpad_client ${BREAKPAD_INCLUDE_DIR} ${BREAKPAD_STATIC_LIB}
  "")

# Be careful with Kerberos: we do not statically link against it as it is a security
# dependency.
find_package(Kerberos REQUIRED)
IMPALA_ADD_THIRDPARTY_LIB(krb5 ${KERBEROS_INCLUDE_DIR} "" ${KERBEROS_LIBRARY})

# We require certain binaries from the kerberos project for our automated kerberos
# testing.
find_package(KerberosPrograms REQUIRED)

# Tests that run any security related tests need to link this in to override the
# krb5_realm_override() implementation in krb5.
# See be/src/kudu/security/krb5_realm_override.cc for more information.
set(KRB5_REALM_OVERRIDE -Wl,--undefined=krb5_realm_override_loaded krb5_realm_override)

###################################################################

# System dependencies
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

###################################################################

## libunwind
if (NOT APPLE)
  find_package(LibUnwind REQUIRED)
  include_directories(SYSTEM ${LIBUNWIND_INCLUDE_DIR})
  IMPALA_ADD_THIRDPARTY_LIB(libunwind ${LIBUNWIND_INCLUDE_DIR} ${LIBUNWIND_STATIC_LIB}
    ${LIBUNWIND_SHARED_LIB})
endif()

# Required for KRPC_GENERATE, which converts protobuf to stubs.
find_package(KRPC REQUIRED)

# KuduClient can use GLOG
add_definitions(-DKUDU_HEADERS_USE_GLOG)
if(NOT $ENV{KUDU_CLIENT_DIR} EQUAL "")
  set(kuduClient_DIR "$ENV{KUDU_CLIENT_DIR}/usr/local/share/kuduClient/cmake")
else()
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(kuduClient_DIR "$ENV{IMPALA_KUDU_HOME}/debug/share/kuduClient/cmake")
  else()
    set(kuduClient_DIR "$ENV{IMPALA_KUDU_HOME}/release/share/kuduClient/cmake")
  endif()
endif()
# When KUDU_IS_SUPPORTED is false, the Kudu client is expected to be a non-functional
# stub. It's still needed to link though.
find_package(kuduClient REQUIRED NO_DEFAULT_PATH)
include_directories(SYSTEM ${KUDU_CLIENT_INCLUDE_DIR})

# compile these subdirs using their own CMakeLists.txt
add_subdirectory(common/function-registry)
add_subdirectory(common/thrift)
add_subdirectory(common/fbs)
add_subdirectory(common/yarn-extras)
add_subdirectory(common/protobuf)
add_subdirectory(be)
add_subdirectory(docker)
add_subdirectory(shaded-deps)
add_subdirectory(fe)
add_subdirectory(impala-parent)
add_subdirectory(ext-data-source)

# Build target for all generated files which most backend code depends on
add_custom_target(gen-deps ALL DEPENDS thrift-deps proto-deps fb-deps)

add_custom_target(tarballs ALL DEPENDS shell_tarball)

add_custom_target(shell_tarball DEPENDS gen-deps
  COMMAND "${CMAKE_SOURCE_DIR}/shell/make_shell_tarball.sh"
)

add_custom_target(cscope ALL DEPENDS gen-deps
  COMMAND "${CMAKE_SOURCE_DIR}/bin/gen-cscope.sh"
)

if (DEFINED ENV{IMPALA_LZO} AND EXISTS $ENV{IMPALA_LZO})
  add_custom_target(impala-lzo ALL DEPENDS gen-deps
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
