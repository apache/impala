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

cmake_minimum_required(VERSION 2.6)

if (NOT DEFINED BUILD_SHARED_LIBS)
  set(BUILD_SHARED_LIBS OFF)
endif()

# Build compile commands database
set(CMAKE_EXPORT_COMPILE_COMMNDS ON)

# Enable toolchain variable if the environment is setup
if (NOT IMPALA_TOOLCHAIN)
   message(STATUS "Regular build.")
else()
   message(STATUS "Toolchain build.")
endif()

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# Helper function that given a package name constructs the package_ROOT variable based on
# the version number extracted from the environment
function(set_dep_root NAME)
  string(TOLOWER ${NAME} NAME_LOWER)
  string(REPLACE "_" "-" NAME_LOWER ${NAME_LOWER})
  set(VAL_NAME "IMPALA_${NAME}_VERSION")
  set(${NAME}_ROOT $ENV{IMPALA_TOOLCHAIN}/${NAME_LOWER}-$ENV{${VAL_NAME}} PARENT_SCOPE)
endfunction()

set(Boost_USE_MULTITHREADED ON)

if (IMPALA_TOOLCHAIN)
  message(STATUS "Found Impala Toolchain, enabling toolchain boost")
  set(ENV{BOOST_ROOT} "$ENV{IMPALA_TOOLCHAIN}/boost-$ENV{IMPALA_BOOST_VERSION}")
  set(Boost_USE_STATIC_LIBS NOT ${BUILD_SHARED_LIBS})
  set(Boost_USE_STATIC_RUNTIME ON)

  # Newer versions of boost (including the version in toolchain) don't build separate
  # multithreaded versions (they always are). Make sure to pick those up.
  set(Boost_USE_MULTITHREADED OFF)

  # Define root path for all dependencies, this is in the form of
  # set_dep_root(PACKAGE) ->
  #   PACKAGE_ROOT set to $ENV{IMPALA_TOOLCHAIN}/PACKAGE-$ENV{IMPALA_PACKAGE_VERSION}
  set_dep_root(AVRO)
  set_dep_root(BZIP2)
  set_dep_root(CYRUS_SASL)
  set_dep_root(GFLAGS)
  set_dep_root(GLOG)
  set_dep_root(GPERFTOOLS)
  set_dep_root(GTEST)
  set_dep_root(LLVM)
  set_dep_root(LZ4)
  set_dep_root(OPENLDAP)
  set_dep_root(RE2)
  set_dep_root(RAPIDJSON)
  set_dep_root(SNAPPY)
  set_dep_root(THRIFT)
  set_dep_root(ZLIB)
endif ()

# find libraries that will be statically linked if they were compiled with -fPIC
find_package(PICLibs REQUIRED)

# find boost headers and libs
IF (DEFINED ENV{BOOST_ROOT})
    # The casing and underscoring expected for these properties varies between
    # versions of CMake. Multiple inconsistent versions may be present here
    # intentionally to provide what a wide range of versions expects.
    set(Boost_NO_SYSTEM_PATHS true)
    set(BOOST_ROOT $ENV{BOOST_ROOT})
    set(BOOST_LIBRARYDIR $ENV{BOOST_ROOT}/lib)
    set(BOOST_INCLUDEDIR $ENV{BOOST_ROOT}/include)
    set(Boost_INCLUDE_DIR ${BOOST_INCLUDEDIR})
ENDIF (DEFINED ENV{BOOST_ROOT})

if (CMAKE_DEBUG)
  set(Boost_DEBUG TRUE)
endif()


find_package(Boost $ENV{IMPALA_MIN_BOOST_VERSION} REQUIRED
  COMPONENTS thread regex system filesystem date_time)
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

if(NOT BUILD_SHARED_LIBS)
  # It might be the case that bzip2 and zlib are not available as static libs in the non
  # toolchain case
  # TODO remove this when moving to toolchain only
  if (NOT BZIP2_STATIC_FOUND)
    set(LIBBZ2 ${BZIP2_LIBRARIES})
  else()
    set(LIBBZ2 BZIP2_STATIC)
  endif()

  if (NOT ZLIB_STATIC_FOUND)
    set(LIBZ ${ZLIB_LIBRARIES})
  else()
    set(LIBZ ZLIB_STATIC)
  endif()
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

# find the Squeasel webserver library
find_package(Squeasel REQUIRED)
include_directories(${SQUEASEL_INCLUDE_DIR})

# find the Mustache template library
find_package(Mustache REQUIRED)
include_directories(${MUSTACHE_INCLUDE_DIR})

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

# find LLVM
find_package(Llvm REQUIRED)
include_directories(${LLVM_INCLUDE_DIR})
set(LIBS ${LIBS} ${LLVM_MODULE_LIBS})

# find Sasl
find_package(Sasl REQUIRED)
include_directories(${SASL_INCLUDE_DIR})
set(SASL_LIBRARY saslstatic)
message(STATUS "Sasl include dir: " ${SASL_INCLUDE_DIR})
message(STATUS "Sasl library: " ${SASL_LIBRARY})

# find openldap
find_package(Ldap REQUIRED)
include_directories(${LDAP_INCLUDE_DIR})
set(LDAP_LIBRARY ldapstatic)
set(LBER_LIBRARY lberstatic)
message(STATUS "LDAP: ${LDAP_INCLUDE_DIR}")

# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(${THRIFT_INCLUDE_DIR})
set(LIBS ${LIBS} ${THRIFT_LIBS})
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift contrib dir: ${THRIFT_CONTRIB_DIR}")
message(STATUS "Thrift library path: ${THRIFT_LIBS}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")

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

# find boost multi precision headers
if (Boost_VERSION LESS 105300)
  find_package(BoostMultiPrecision REQUIRED)
  include_directories(${BOOST_MULTI_PRECISION_INCLUDE_DIR})
  message(STATUS "BoostMultiPrecision include dir: " ${BOOST_MULTI_PRECISION_INCLUDE_DIR})
endif ()

# find Avro headers and libs
find_package(Avro REQUIRED)
include_directories(${AVRO_INCLUDE_DIR})
set(LIBS ${LIBS} ${AVRO_STATIC_LIB})
message(STATUS "Avro include dir: " ${AVRO_INCLUDE_DIR})
message(STATUS "Avro static library: " ${AVRO_STATIC_LIB})

# find jni headers and libs
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
set(LIBS ${LIBS} ${JNI_LIBRARIES})
message(STATUS "JNI_INCLUDE_DIRS: ${JNI_INCLUDE_DIRS}")
message(STATUS "JNI_LIBRARIES: ${JNI_LIBRARIES}")

# compile these subdirs using their own CMakeLists.txt
add_subdirectory(common/function-registry)
add_subdirectory(common/thrift)
add_subdirectory(be)

# Run FE and BE tests
add_custom_target(testall
  COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_SOURCE_DIR}/fe mvn test
  COMMAND ${CMAKE_SOURCE_DIR}/bin/runbackendtests.sh
)

# Load test data
add_custom_target(loadtestdata
  COMMAND ${CMAKE_SOURCE_DIR}/bin/load-test-data.sh
)

add_custom_target(benchmark_run
  COMMAND ${CMAKE_SOURCE_DIR}/be/bin/run_hive_benchmark.py
)
