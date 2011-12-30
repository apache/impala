# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# generate CTest input files
enable_testing()

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
find_package(Boost REQUIRED COMPONENTS thread regex-mt)
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

# find PProf libs
find_package(PProf REQUIRED)
include_directories(${PPROF_INCLUDE_DIR})
set (LIBS ${LIBS} ${PPROF_LIBRARIES})
add_library(pprofstatic STATIC IMPORTED)
set_target_properties(pprofstatic PROPERTIES IMPORTED_LOCATION "${PPROF_STATIC_LIB}")
add_library(tcmallocstatic STATIC IMPORTED)
set_target_properties(tcmallocstatic PROPERTIES IMPORTED_LOCATION "${HEAPPROF_STATIC_LIB}")

message(STATUS ${PPROF_INCLUDE_DIR})

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

# compile these subdirs using their own CMakeLists.txt
add_subdirectory(common/function-registry)
add_subdirectory(common/thrift)
add_subdirectory(be)

add_custom_target(testall 
  COMMAND ${CMAKE_COMMAND} -E chdir ${CMAKE_SOURCE_DIR}/fe mvn test
  COMMAND ${CMAKE_SOURCE_DIR}/bin/runbackendtests.sh
)
