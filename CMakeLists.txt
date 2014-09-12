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

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

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

set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED COMPONENTS thread regex-mt system-mt filesystem-mt)
include_directories(${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})
message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

# find jni headers and libs
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
set(LIBS ${LIBS} ${JNI_LIBRARIES})
message(STATUS "JNI_INCLUDE_DIRS: ${JNI_INCLUDE_DIRS}")
message(STATUS "JNI_LIBRARIES: ${JNI_LIBRARIES}")

# find HDFS headers and libs
find_package(HDFS REQUIRED)
include_directories(${HDFS_INCLUDE_DIR})
set(LIBS ${LIBS} ${HDFS_LIBS})

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
# for static linking with GLOG, GLOG_STATIC_LIB is set in GLOG's find module
add_library(glogstatic STATIC IMPORTED)
# TODO: Is this directive required for all libraries? Seems to make no difference.
set_target_properties(glogstatic PROPERTIES IMPORTED_LOCATION ${GLOG_STATIC_LIB})
message(STATUS "GLog include dir: " ${GLOG_INCLUDE_DIR})

# find GFlags headers and libs (needed for GLog)
find_package(GFlags REQUIRED)
include_directories(${GFLAGS_INCLUDE_DIR})
set(LIBS ${LIBS} ${GFLAGS_LIBS})
# for static linking with GFLAGS, GFLAGS_STATIC_LIB is set in GFLAGS' find module
add_library(gflagsstatic STATIC IMPORTED)
set_target_properties(gflagsstatic PROPERTIES IMPORTED_LOCATION ${GFLAGS_STATIC_LIB})
message(STATUS "GFlags include dir: " ${GFLAGS_INCLUDE_DIR})

# find PProf libs
find_package(PProf REQUIRED)
include_directories(${PPROF_INCLUDE_DIR})
set (LIBS ${LIBS} ${PPROF_LIBRARIES})
add_library(pprofstatic STATIC IMPORTED)
set_target_properties(pprofstatic PROPERTIES IMPORTED_LOCATION "${PPROF_STATIC_LIB}")
add_library(tcmallocstatic STATIC IMPORTED)
set_target_properties(tcmallocstatic PROPERTIES IMPORTED_LOCATION "${HEAPPROF_STATIC_LIB}")
message(STATUS "PProf include dir: " ${PPROF_INCLUDE_DIR})

# find GTest headers and libs
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
set(LIBS ${LIBS} ${GTEST_LIBRARIES})
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION "${GTEST_LIBRARY}")
message(STATUS "GTest include dir: " ${GTEST_INCLUDE_DIR})
message(STATUS "GTest library: " ${GTEST_LIBRARY})

# find LLVM
find_package(Llvm REQUIRED)
include_directories(${LLVM_INCLUDE_DIR})
set(LIBS ${LIBS} ${LLVM_MODULE_LIBS})

# find Sasl
find_package(Sasl REQUIRED)
include_directories(${SASL_INCLUDE_DIR})
add_library(saslstatic STATIC IMPORTED)
set_target_properties(saslstatic PROPERTIES IMPORTED_LOCATION ${SASL_STATIC_LIBRARY})
set(SASL_LIBRARY saslstatic)
message(STATUS "Sasl include dir: " ${SASL_INCLUDE_DIR})
message(STATUS "Sasl library: " ${SASL_LIBRARY})

# find openldap
find_package(Ldap REQUIRED)
include_directories(${LDAP_INCLUDE_DIR})
add_library(ldapstatic STATIC IMPORTED)
set_target_properties(ldapstatic PROPERTIES IMPORTED_LOCATION ${LDAP_STATIC_LIBRARY})
add_library(lberstatic STATIC IMPORTED)
set_target_properties(lberstatic PROPERTIES IMPORTED_LOCATION ${LBER_STATIC_LIBRARY})
set(LDAP_LIBRARY ldapstatic)
set(LBER_LIBRARY lberstatic)

# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(${THRIFT_INCLUDE_DIR})
set(LIBS ${LIBS} ${THRIFT_LIBS})
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift contrib dir: ${THRIFT_CONTRIB_DIR}")
message(STATUS "Thrift library path: ${THRIFT_LIBS}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
# for static linking with Thrift, THRIFT_STATIC_LIB is set in FindThrift.cmake
add_library(thriftstatic STATIC IMPORTED)
set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

# find Snappy headers and libs
find_package(Snappy REQUIRED)
include_directories(${SNAPPY_INCLUDE_DIR})
set(LIBS ${LIBS} ${SNAPPY_LIBRARIES})
add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION "${SNAPPY_STATIC_LIB}")
message(STATUS "Snappy include dir: " ${SNAPPY_INCLUDE_DIR})
message(STATUS "Snappy library: " "${SNAPPY_STATIC_LIB}")

find_package(Lz4 REQUIRED)
include_directories(${LZ4_INCLUDE_DIR})
set(LIBS ${LIBS} ${LZ4_LIBRARIES})
add_library(lz4 STATIC IMPORTED)
set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION "${LZ4_STATIC_LIB}")
message(STATUS "Lz4 include dir: " ${LZ4_INCLUDE_DIR})
message(STATUS "Lz4 library: " "${LZ4_STATIC_LIB}")

# find re2 headers and libs
find_package(Re2 REQUIRED)
include_directories(${RE2_INCLUDE_DIR})
set(LIBS ${LIBS} ${RE2_LIBRARIES})
add_library(re2 STATIC IMPORTED)
set_target_properties(re2 PROPERTIES IMPORTED_LOCATION "${RE2_STATIC_LIB}")
message(STATUS "Re2 include dir: " ${RE2_INCLUDE_DIR})
message(STATUS "Re2 library: " ${RE2_STATIC_LIB})

# find rapidjson headers
find_package(RapidJson REQUIRED)
include_directories(${RAPIDJSON_INCLUDE_DIR})
message(STATUS "RapidJson include dir: " ${RAPIDJSON_INCLUDE_DIR})

# find boost multi precision headers
find_package(BoostMultiPrecision REQUIRED)
include_directories(${BOOST_MULTI_PRECISION_INCLUDE_DIR})
message(STATUS "BoostMultiPrecision include dir: " ${BOOST_MULTI_PRECISION_INCLUDE_DIR})

# find Avro headers and libs
find_package(Avro REQUIRED)
include_directories(${AVRO_INCLUDE_DIR})
set(LIBS ${LIBS} ${AVRO_STATIC_LIB})
add_library(avro STATIC IMPORTED)
set_target_properties(avro PROPERTIES IMPORTED_LOCATION "${AVRO_STATIC_LIB}")
message(STATUS "Avro include dir: " ${AVRO_INCLUDE_DIR})
message(STATUS "Avro static library: " ${AVRO_STATIC_LIB})

# Treat gutil just like an Impala native library (i.e., one built with CMake), so no need
# for find_package()
add_subdirectory($ENV{IMPALA_HOME}/thirdparty/gutil/)

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
