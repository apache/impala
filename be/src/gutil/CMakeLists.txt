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

set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/gutil")
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/gutil")

# Ignore the code in proto/ for now until needed; otherwise this adds an extra dependency
# on protoc to the build for no gain.

# PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS
#   SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../
#   BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../
#   )
# SET(GUTIL_SRCS ${PROTO_SRCS})

# PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS
#   SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/../
#   BINARY_ROOT ${CMAKE_CURRENT_BINARY_DIR}/../
#   PROTO_FILES proto/types.proto
#   )
# SET(GUTIL_SRCS ${GUTIL_SRCS} ${PROTO_SRCS})

INCLUDE_DIRECTORIES($ENV{IMPALA_HOME}/thirdparty/)
INCLUDE_DIRECTORIES(${GLOG_INCLUDE_DIR})

ADD_LIBRARY(gutil
#  ${GUTIL_SRCS}
./bits.cc
./stringprintf.cc
./strings/memutil.cc
./strings/charset.cc
./strings/util.cc
./strings/human_readable.cc
./strings/join.cc
./strings/numbers.cc
./strings/split.cc
./strings/strip.cc
./strings/ascii_ctype.cc
./strings/escaping.cc
./strings/stringpiece.cc
./strings/substitute.cc
./strings/strcat.cc
./strings/serialize.cc
./mathlimits.cc
./int128.cc
./strtoint.cc
./once.cc
./atomicops-internals-x86.cc
./spinlock_wait.cc
./walltime.cc
./hash/hash.cc
./hash/jenkins.cc
./hash/city.cc
./utf/rune.c
)



# Disable warnings which trigger a lot in the Google code:
SET_TARGET_PROPERTIES(gutil PROPERTIES
  COMPILE_FLAGS "${CXX_COMMON_FLAGS} -funsigned-char -Wno-deprecated -Wno-char-subscripts -fPIC")

TARGET_LINK_LIBRARIES(gutil glogstatic gflagsstatic)
SET(GUTIL_LIBS gutil)
