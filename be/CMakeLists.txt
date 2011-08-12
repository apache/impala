# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# generate CTest input files
enable_testing()

# set flags for CMAKE_BUILD_TYPE=Debug;
# suppress warnings for comparison of signed and unsigned integers
SET(CMAKE_CXX_FLAGS_DEBUG "-g -Wall -Wno-sign-compare")

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(${Thrift_INCLUDE_DIR})
set(LIBS ${LIBS} ${Thrift_LIBS})

# find boost headers and libs
set(Boost_DEBUG ON)
set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost REQUIRED COMPONENTS thread)
include_directories(${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})

# find jni headers and libs
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})
set(LIBS ${LIBS} ${JNI_LIBRARIES})

# find HDFS headers and libs
find_package(HDFS REQUIRED)
include_directories(${HDFS_INCLUDE_DIR})
set(LIBS ${LIBS} ${HDFS_LIBS})

# find GFlags headers and libs (needed for GLog)
find_package(GFlags REQUIRED)
include_directories(${GFLAGS_INCLUDE_DIR})
set(LIBS ${LIBS} ${GFLAGS_LIBS})
# for static linking with GFLAGS, GFLAGS_STATIC_LIB is set in GFLAGS' find module
add_library(gflagsstatic STATIC IMPORTED)
set_target_properties(gflagsstatic PROPERTIES IMPORTED_LOCATION ${GFLAGS_STATIC_LIB})

message(STATUS ${GFLAGS_INCLUDE_DIR})

# find GLog headers and libs
find_package(GLog REQUIRED)
include_directories(${GLOG_INCLUDE_DIR})
set(LIBS ${LIBS} ${GLOG_LIBS})
# for static linking with GLOG, GLOG_STATIC_LIB is set in GLOG's find module
add_library(glogstatic STATIC IMPORTED)
set_target_properties(glogstatic PROPERTIES IMPORTED_LOCATION ${GLOG_STATIC_LIB})

message(STATUS ${GLOG_INCLUDE_DIR})

# find GTest headers and libs
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIR})
set(LIBS ${LIBS} ${GTEST_LIBRARIES})
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION "${GTEST_LIBRARY}")

message(STATUS ${GTEST_INCLUDE_DIR})
message(STATUS ${GTEST_LIBRARY})

# resolve "#include "<subdir>/<name>.h"
include_directories(${CMAKE_SOURCE_DIR}/src)

# resolve includes of generated code
include_directories(${CMAKE_SOURCE_DIR}/generated-sources)

# allow linking of static libs into dynamic lib
add_definitions(-fPIC)

# compile these subdirs using their own CMakeLists.txt
add_subdirectory(src/common)
add_subdirectory(src/exec)
add_subdirectory(src/exprs)
add_subdirectory(src/runtime)
add_subdirectory(src/service)
add_subdirectory(src/testutil)
add_subdirectory(src/util)

add_subdirectory(generated-sources/gen-cpp)

link_directories(
  ${CMAKE_SOURCE_DIR}/build/common
  ${CMAKE_SOURCE_DIR}/build/exec
  ${CMAKE_SOURCE_DIR}/build/exprs
  ${CMAKE_SOURCE_DIR}/build/runtime
  ${CMAKE_SOURCE_DIR}/build/service
  ${CMAKE_SOURCE_DIR}/build/testutil
  ${CMAKE_SOURCE_DIR}/build/thrift
  ${CMAKE_SOURCE_DIR}/build/util
)

# only generate statically linked libs and executables
set(BUILD_SHARED_LIBS OFF)

# where to put generated libraries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/build/")
set(ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/build/")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/")
