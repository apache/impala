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
set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED COMPONENTS thread regex-mt system-mt filesystem-mt)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})
message(STATUS ${Boost_INCLUDE_DIRS})
message(STATUS ${Boost_LIBRARIES})

# find jni headers and libs
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JNI_INCLUDE_DIRS})
set(LIBS ${LIBS} ${JNI_LIBRARIES})
message(STATUS "JNI_INCLUDE_DIRS: ${JNI_INCLUDE_DIRS}")
message(STATUS "JNI_LIBRARIES: ${JNI_LIBRARIES}")

# find HDFS headers and libs
find_package(HDFS REQUIRED)
include_directories(SYSTEM ${HDFS_INCLUDE_DIR})
set(LIBS ${LIBS} ${HDFS_LIBS})

find_package(Mongoose REQUIRED)
include_directories(SYSTEM ${MONGOOSE_INCLUDE_DIR})

# find GLog headers and libs.  Must include glog headers before the other
# google libraries.  They all have a config.h and we want glog's to be picked
# up first.
find_package(GLog REQUIRED)
include_directories(SYSTEM ${GLOG_INCLUDE_DIR})
set(LIBS ${LIBS} ${GLOG_LIBS})
# for static linking with GLOG, GLOG_STATIC_LIB is set in GLOG's find module
add_library(glogstatic STATIC IMPORTED)
set_target_properties(glogstatic PROPERTIES IMPORTED_LOCATION ${GLOG_STATIC_LIB})

message(STATUS ${GLOG_INCLUDE_DIR})

# find GFlags headers and libs (needed for GLog)
find_package(GFlags REQUIRED)
include_directories(SYSTEM ${GFLAGS_INCLUDE_DIR})
set(LIBS ${LIBS} ${GFLAGS_LIBS})
# for static linking with GFLAGS, GFLAGS_STATIC_LIB is set in GFLAGS' find module
add_library(gflagsstatic STATIC IMPORTED)
set_target_properties(gflagsstatic PROPERTIES IMPORTED_LOCATION ${GFLAGS_STATIC_LIB})

message(STATUS ${GFLAGS_INCLUDE_DIR})

# find PProf libs
find_package(PProf REQUIRED)
include_directories(SYSTEM ${PPROF_INCLUDE_DIR})
set (LIBS ${LIBS} ${PPROF_LIBRARIES})
add_library(pprofstatic STATIC IMPORTED)
set_target_properties(pprofstatic PROPERTIES IMPORTED_LOCATION "${PPROF_STATIC_LIB}")
add_library(tcmallocstatic STATIC IMPORTED)
set_target_properties(tcmallocstatic PROPERTIES IMPORTED_LOCATION "${HEAPPROF_STATIC_LIB}")

message(STATUS ${PPROF_INCLUDE_DIR})

# find GTest headers and libs
find_package(GTest REQUIRED)
include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
set(LIBS ${LIBS} ${GTEST_LIBRARIES})
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION "${GTEST_LIBRARY}")

message(STATUS ${GTEST_INCLUDE_DIR})
message(STATUS ${GTEST_LIBRARY})

# find LLVM
find_package(Llvm REQUIRED)
include_directories(SYSTEM ${LLVM_INCLUDE_DIR})
set(LIBS ${LIBS} ${LLVM_MODULE_LIBS})

# find Sasl
find_package(Sasl REQUIRED)
include_directories(SYSTEM ${SASL_INCLUDE_DIR})
add_library(saslstatic STATIC IMPORTED)
set_target_properties(saslstatic PROPERTIES IMPORTED_LOCATION ${SASL_STATIC_LIBRARY})
set(SASL_LIBRARY saslstatic)

# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
set(LIBS ${LIBS} ${THRIFT_LIBS})
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift contrib dir: ${THRIFT_CONTRIB_DIR}")
message(STATUS "Thrift library path: ${THRIFT_LIBS}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift static non-blocking library: ${THRIFT_NB_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
# for static linking with Thrift, THRIFT_STATIC_LIB is set in FindThrift.cmake
add_library(thriftstatic STATIC IMPORTED)
set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})
add_library(thriftnbstatic STATIC IMPORTED)
set_target_properties(thriftnbstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_NB_STATIC_LIB})

# find Snappy headers and libs
find_package(Snappy REQUIRED)
include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
set(LIBS ${LIBS} ${SNAPPY_LIBRARIES})
add_library(snappy STATIC IMPORTED)
set_target_properties(snappy PROPERTIES IMPORTED_LOCATION "${SNAPPY_LIBRARY}")

message(STATUS ${SNAPPY_INCLUDE_DIR})
message(STATUS ${SNAPPY_LIBRARY})

# find re2 headers and libs
find_package(Re2 REQUIRED)
include_directories(SYSTEM ${RE2_INCLUDE_DIR})
set(LIBS ${LIBS} ${RE2_LIBRARIES})
add_library(re2 STATIC IMPORTED)
set_target_properties(re2 PROPERTIES IMPORTED_LOCATION "${RE2_LIBRARY}")

message(STATUS ${RE2_INCLUDE_DIR})
message(STATUS ${RE2_LIBRARY})

# find rapidjson headers
find_package(RapidJson REQUIRED)
include_directories(SYSTEM ${RAPIDJSON_INCLUDE_DIR})
message(STATUS ${RAPIDJSON_INCLUDE_DIR})

# find Avro headers and libs
find_package(Avro REQUIRED)
include_directories(SYSTEM ${AVRO_INCLUDE_DIR})
set(LIBS ${LIBS} ${AVRO_STATIC_LIB})
add_library(avro STATIC IMPORTED)
set_target_properties(avro PROPERTIES IMPORTED_LOCATION "${AVRO_STATIC_LIB}")

message(STATUS ${AVRO_INCLUDE_DIR})
message(STATUS ${AVRO_STATIC_LIB})

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
