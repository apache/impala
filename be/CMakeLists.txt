# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

cmake_minimum_required(VERSION 2.6)

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(${Thrift_INCLUDE_DIR})
set(LIBS ${LIBS} ${Thrift_LIBS})

# find boost headers and libs
find_package(Boost REQUIRED)
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

add_subdirectory(generated-sources/gen-cpp)

link_directories(
  ${CMAKE_SOURCE_DIR}/build/common
  ${CMAKE_SOURCE_DIR}/build/exec
  ${CMAKE_SOURCE_DIR}/build/exprs
  ${CMAKE_SOURCE_DIR}/build/runtime
  ${CMAKE_SOURCE_DIR}/build/service
  ${CMAKE_SOURCE_DIR}/build/testutil
  ${CMAKE_SOURCE_DIR}/build/thrift
)

# only generate statically linked libs and executables
set(BUILD_SHARED_LIBS OFF)

# where to put generated libraries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/build/")
set(ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/build/")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/")
