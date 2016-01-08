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

# Setting this enables compiling for assembly output.  To compile to assembly:
#  1. cd into the directory containing the source file
#  2. 'make help' will list the assembly file targets (i.e. <srcfile.s>
#  3. 'make <srcfile>.s' to build the assembly for that file.  The file is built
#      to CMakeFiles/<currentdir>.dir/<srcfile>.s
PROJECT(ASSEMBLER)

# compiler flags that are common across debug/release builds
#  -Wall: Enable all warnings.
#  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#   -fno-strict-aliasing: disable optimizations that assume strict aliasing. This
#       is unsafe to do if the code uses casts (which we obviously do).
#  -Wno-unknown-pragmas: suppress warnings for unknown (compiler specific) pragmas
#  -Wno-deprecated: gutil contains deprecated headers
#  -pthread: enable multithreaded malloc
#  -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -Wno-unknown-pragmas -pthread")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-strict-aliasing")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")

# On Apple we build with clang and need libstdc++ instead of libc++
if (APPLE)
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libstdc++")
endif()

SET(CXX_COVERAGE_FLAGS "-fprofile-arcs -ftest-coverage")

# For any clang builds (currently only ASAN):
#   -Qunused-arguments: quiet warnings about unused arguments to clang because ccache
#        makes extra calls to clang which may have extra includes (-I) that are unused.
#   -fcolor-diagnostics: ensure clang generates colorized output, which is necessary
#        when using ccache as clang thinks it is not called from a terminal.
SET(CXX_CLANG_FLAGS "-Qunused-arguments -fcolor-diagnostics -Wno-unused-local-typedef")
# For any gcc builds:
#   -g: Enable symbols for profiler tools
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
SET(CXX_GCC_FLAGS "-g -Wno-unused-local-typedefs")

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
#
# Debug information is stored as dwarf2 to be as compatible as possible
SET(CXX_FLAGS_DEBUG "${CXX_GCC_FLAGS} -ggdb -gdwarf-2")
SET(CXX_FLAGS_RELEASE "${CXX_GCC_FLAGS} -O3 -DNDEBUG -gdwarf-2")
SET(CXX_FLAGS_ADDRESS_SANITIZER
  "${CXX_CLANG_FLAGS} -O1 -g -fsanitize=address -fno-omit-frame-pointer -DADDRESS_SANITIZER")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

STRING (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_DEBUG")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_DEBUG} ${CXX_COVERAGE_FLAGS}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_RELEASE")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_RELEASE} ${CXX_COVERAGE_FLAGS}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  if (NOT IMPALA_TOOLCHAIN)
    SET(CMAKE_CXX_COMPILER ${LLVM_CLANG_EXECUTABLE})
  endif()
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_ADDRESS_SANITIZER}")
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

# Add flags that are common across build types
#  - fverboase-asm creates better annotated assembly.  This doesn't seem to affect
#    when building the binary.
# LLMV_CFLAGS - Adding llvm compile flags
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fverbose-asm")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LLVM_CFLAGS}")

# Use ccache when found and not explicitly disabled by setting the DISABLE_CCACHE envvar.
find_program(CCACHE ccache)
if (CCACHE AND NOT DEFINED ENV{DISABLE_CCACHE})
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
    # Need to set CCACHE_CPP so that ccache calls clang with the original source file for
    # both preprocessing and compilation. Otherwise, ccache will use clang to preprocess
    # the file and then call clang with the preprocessed output if not cached. However,
    # the preprocessed output from clang may contain code (e.g. from macro expansions)
    # that generates compilation warnings that would not be reported if compiling the
    # original source directly with clang.
    SET(ENV{CCACHE_CPP} YES)
  endif()
endif()

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DHAVE_NETDB_H)

# Set clang flags for cross-compiling to IR.
# IR_COMPILE is #defined for the cross compile to remove code that bloats the IR.
# Note that we don't enable any optimization. We want unoptimized IR since we will be
# modifying it at runtime, then re-compiling (and optimizing) the modified code. The final
# optimizations will be less effective if the initial code is also optimized.
#  -Wno-deprecated: gutil contains deprecated headers
#  -Wno-c++11-extensions: c++11 extensions are ok as long as clang supports them
set(CLANG_IR_CXX_FLAGS "-emit-llvm" "-c" "-DIR_COMPILE" "-DNDEBUG"
  "-DHAVE_INTTYPES_H" "-DHAVE_NETINET_IN_H" "-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG"
  "-Wno-deprecated" "-Wno-c++11-extensions")

# setup doc generation with Doxygen
find_package(Doxygen)
if (DOXYGEN_FOUND)
  set(DOXYGEN_OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build/docs)
  # Possible to not input the subdirs one by one?
  set(CMAKE_DOXYGEN_INPUT
    ${CMAKE_SOURCE_DIR}/be/src
    ${CMAKE_SOURCE_DIR}/be/src/catalog/
    ${CMAKE_SOURCE_DIR}/be/src/common/
    ${CMAKE_SOURCE_DIR}/be/src/exec/
    ${CMAKE_SOURCE_DIR}/be/src/exprs/
    ${CMAKE_SOURCE_DIR}/be/src/resourcebroker/
    ${CMAKE_SOURCE_DIR}/be/src/runtime/
    ${CMAKE_SOURCE_DIR}/be/src/scheduling/
    ${CMAKE_SOURCE_DIR}/be/src/service/
    ${CMAKE_SOURCE_DIR}/be/src/statestore/
    ${CMAKE_SOURCE_DIR}/be/src/testutil/
    ${CMAKE_SOURCE_DIR}/be/src/thrift/
    ${CMAKE_SOURCE_DIR}/be/src/util/
    ${CMAKE_SOURCE_DIR}/be/src/transport/
    )
  # CMake appends using ';'. doxygen wants spaces
  string(REPLACE ";" " " DOXYGEN_INPUT "${CMAKE_DOXYGEN_INPUT}")
  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/.impala.doxy
                 ${CMAKE_CURRENT_SOURCE_DIR}/build/config/.impala.doxy)
  file(MAKE_DIRECTORY ${DOXYGEN_OUTPUT_DIR})
  add_custom_target(docs
    COMMAND ${CMAKE_COMMAND} -E echo_append "Building Docs..."
    COMMAND ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/build/config/.impala.doxy
    )
else (DOXYGEN_FOUND)
  MESSAGE(STATUS "WARNING: Doxygen not found - Docs will not be created")
endif(DOXYGEN_FOUND)

# resolve "#include "<subdir>/<name>.h"
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# resolve includes of generated code
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/generated-sources)

set(CLANG_INCLUDE_FLAGS)

if (IMPALA_TOOLCHAIN)
  # When the Toolchain is used we use LLVM 3.3 that was built in a different path that it
  # is invoked from, and a GCC that resides in a different location. LVVM 3.3 relies on
  # hard-coded path information about where to find the system headers and does not support
  # specifying the -gcc-toolchain flag to dynamically provide this information. Because of
  # these reasons we need to manually add the system c++ headers to the path when we
  # compile the IR code with clang.
  set(CLANG_BASE_FLAGS
    "-I${GCC_ROOT}/include/c++/$ENV{IMPALA_GCC_VERSION}"
    "-I${GCC_ROOT}/include/c++/$ENV{IMPALA_GCC_VERSION}/backward"
    "-I${GCC_ROOT}/include/c++/$ENV{IMPALA_GCC_VERSION}/x86_64-unknown-linux-gnu")
  set(CLANG_INCLUDE_FLAGS ${CLANG_BASE_FLAGS})
endif()

set(CLANG_INCLUDE_FLAGS
  ${CLANG_INCLUDE_FLAGS}
  "-I${CMAKE_CURRENT_SOURCE_DIR}/src"
  "-I${CMAKE_CURRENT_SOURCE_DIR}/generated-sources"
  "-I${THRIFT_INCLUDE_DIR}"
  "-I${SQUEASEL_INCLUDE_DIR}"
  "-I${GLOG_INCLUDE_DIR}"
  "-I${GFLAGS_INCLUDE_DIR}"
  "-I${RAPIDJSON_INCLUDE_DIR}"
  "-I${AVRO_INCLUDE_DIR}"
  "-I${BOOST_INCLUDEDIR}"
  # Required so that jni.h can be found during Clang compilation
  "-I${JAVA_INCLUDE_PATH}"
  "-I${JAVA_INCLUDE_PATH2}"
  "-I${RE2_INCLUDE_DIR}"
  "-I${SASL_INCLUDE_DIR}"
  "-I${BZIP2_INCLUDE_DIR}"
  "-I${ZLIB_INCLUDE_DIR}"
  "-I${OPENSSL_INCLUDE_DIR}"
  "-I${LDAP_INCLUDE_DIR}"
)

# allow linking of static libs into dynamic lib
add_definitions(-fPIC)

# set compile output directory
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE_DEBUG" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/debug/")
else()
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/release/")
endif()

# This is a list of impala library dependencies. Individual libraries
# must not specify library dependencies in their own CMakeLists.txt file.
# Enclose the impala libraries in -Wl,--start-group and -Wl,--end-group
# to resolve cyclic dependencies. As long as those flags are given,
# the order in which impala libraries are listed below does not matter.
# Note: The ld documentation discourages auto-resolving cyclic dependencies
# for performance reasons.
if (NOT APPLE)
  # When compiling on Mac with clang using these linker flags are undefined and Clang on
  # Mac will abort on unknown compiler or linker flags. In the long-term we should
  # move away from using these flags to have a coherent build on OS X and Linux.
  set(WL_START_GROUP "-Wl,--start-group")
  set(WL_END_GROUP "-Wl,--end-group")
endif()
set (IMPALA_LINK_LIBS
  ${WL_START_GROUP}
  Catalog
  CodeGen
  Common
  Exec
  Exprs
  GlobalFlags
  ImpalaThrift
  ResourceBroker
  Rpc
  Runtime
  Scheduling
  Service
  Statestore
  TestUtil
  ThriftSaslTransport
  Udf
  Util
  Webserver
  ${WL_END_GROUP}
)

# If using dynamic linking, -Wl does not have any effect (it's only for .a files). So we
# need to add these redundant dependencies to resolve the circular references in our
# libraries when dynamic linking is enabled.
if (BUILD_SHARED_LIBS)
  set (IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS}
    Runtime
    Exec
    CodeGen
    Exprs
    Rpc
    Service
    Statestore
    Scheduling
    Catalog
    ResourceBroker
    Webserver
    ImpalaThrift
    GlobalFlags
    Common
    Udf
    )
endif ()

# Add all external dependencies. They should come after the impala libs.
set (IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS}
  ${SNAPPY_STATIC_LIB}
  ${LZ4_STATIC_LIB}
  ${RE2_STATIC_LIB}
  ${Boost_LIBRARIES}
  ${LLVM_MODULE_LIBS}
  thriftstatic
  ${SASL_LIBRARY}
  ${LDAP_LIBRARY}
  ${LBER_LIBRARY}
  ThriftSaslTransport
  gutil
  glogstatic
  gflagsstatic
  pprofstatic
  ${HDFS_LIB}
  ${LIBZ}
  ${LIBBZ2}
  ${AVRO_STATIC_LIB}
  ${JAVA_JVM_LIBRARY}
  -lrt
)


if ("${CMAKE_BUILD_TYPE}" STREQUAL "CODE_COVERAGE")
  set (IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS} -lgcov)
endif ()

# The above link list does not include tcmalloc. This is because the Impala JVM support
# libraries (libfesupport, libloggingsupport) cannot use tcmalloc in all cases. When they
# are started up by the FE (for tests) the jvm has already made allocations before
# tcmalloc can be loaded. In all other binaries, we can use tcmalloc except the address
# sanitizer build. Address sanitizer is incompatible with tcmalloc (they both intercept
# malloc/free)
set (IMPALA_LINK_LIBS_NO_TCMALLOC ${IMPALA_LINK_LIBS})
if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  set (IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS} tcmallocstatic)
endif()

# When we link statically, we need to replace the static libhdfs.a with the dynamic
# version otherwise the dynamic support libraries will pickup the static libhdfs.a
# library. The result will not compile as libhdfs.a is not compiled with -fpic. The same
# is true for other system dependencies that we don't have control over.
set(IMPALA_LINK_LIBS_DYNAMIC_TARGETS ${IMPALA_LINK_LIBS_NO_TCMALLOC})
list(REMOVE_ITEM IMPALA_LINK_LIBS_DYNAMIC_TARGETS HDFS_STATIC)
set(IMPALA_LINK_LIBS_DYNAMIC_TARGETS ${IMPALA_LINK_LIBS_DYNAMIC_TARGETS} ${HDFS_LIBRARIES})
# Replace static libz bzib with dynamic
if (NOT IMPALA_TOOLCHAIN AND NOT BUILD_SHARED_LIBS)
  list(REMOVE_ITEM IMPALA_LINK_LIBS_DYNAMIC_TARGETS BZIP2_STATIC)
  list(REMOVE_ITEM IMPALA_LINK_LIBS_DYNAMIC_TARGETS ZLIB_STATIC)
  set(IMPALA_LINK_LIBS_DYNAMIC_TARGETS
    ${IMPALA_LINK_LIBS_DYNAMIC_TARGETS} ${ZLIB_LIBRARIES} ${BZIP2_LIBRARIES})
endif()

# Replace Udf with ImpalaUdf for $UDF_TEST_LINK_LIBS
set (UDF_TEST_LINK_LIBS ${IMPALA_LINK_LIBS} ImpalaUdf)
list(REMOVE_ITEM UDF_TEST_LINK_LIBS Udf)

# Link libs for test executables.  Although not all tests need all libs,
# the build time for the tests is rather small and not worth the trouble.
set (IMPALA_TEST_LINK_LIBS ${IMPALA_LINK_LIBS} gtest)
set (UDF_TEST_LINK_LIBS ${UDF_TEST_LINK_LIBS} gtest)

MESSAGE(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")

if (CMAKE_DEBUG)
  MESSAGE(STATUS "Linker Libs: ${IMPALA_LINK_LIBS}")
endif()

set(LLVM_IR_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/llvm-ir")
file(MAKE_DIRECTORY ${LLVM_IR_OUTPUT_DIRECTORY})

# Add custom target to only build the backend tests
add_custom_target(be-test)

# Utility CMake function to make specifying tests and benchmarks less verbose
FUNCTION(ADD_BE_TEST TEST_NAME)
  # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
  get_filename_component(DIR_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)
  ADD_EXECUTABLE(${TEST_NAME} ${TEST_NAME}.cc)
  TARGET_LINK_LIBRARIES(${TEST_NAME} ${IMPALA_TEST_LINK_LIBS})
  set(CMAKE_EXE_LINKER_FLAGS "--start-group")
  ADD_TEST(${TEST_NAME} "${BUILD_OUTPUT_ROOT_DIRECTORY}/${DIR_NAME}/${TEST_NAME}")
  ADD_DEPENDENCIES(be-test ${TEST_NAME})
ENDFUNCTION()

FUNCTION(ADD_BE_BENCHMARK BENCHMARK_NAME)
  ADD_EXECUTABLE(${BENCHMARK_NAME} ${BENCHMARK_NAME}.cc)
  TARGET_LINK_LIBRARIES(${BENCHMARK_NAME} ${IMPALA_LINK_LIBS})
ENDFUNCTION()

# Function to generate rule to cross compile a source file to an IR module.
# This should be called with the .cc src file and it will generate a
# src-file-ir target that can be built.
# e.g. COMPILE_TO_IR(test.cc) generates the "test-ir" make target.
# Note: this is duplicated in udf_samples/CMakeLists.txt
function(COMPILE_TO_IR SRC_FILE)
  get_filename_component(BASE_NAME ${SRC_FILE} NAME_WE)
  set(OUTPUT_FILE "${LIBRARY_OUTPUT_PATH}/${BASE_NAME}.ll")
  add_custom_command(
    OUTPUT ${OUTPUT_FILE}
    COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} ${CLANG_INCLUDE_FLAGS} ${SRC_FILE} -o ${OUTPUT_FILE}
    DEPENDS ${SRC_FILE})
  add_custom_target(${BASE_NAME}-ir ALL DEPENDS ${OUTPUT_FILE})
endfunction(COMPILE_TO_IR)

# Gutil is a little bit special
add_subdirectory(src/gutil)

# compile these subdirs using their own CMakeLists.txt
add_subdirectory(src/catalog)
add_subdirectory(src/codegen)
add_subdirectory(src/common)
add_subdirectory(src/exec)
add_subdirectory(src/resourcebroker)
add_subdirectory(src/exprs)
add_subdirectory(src/runtime)
add_subdirectory(src/scheduling)
add_subdirectory(src/statestore)
add_subdirectory(src/service)
add_subdirectory(src/testutil)
add_subdirectory(src/rpc)
add_subdirectory(src/udf)
add_subdirectory(src/udf_samples)
add_subdirectory(src/util)
add_subdirectory(src/transport)

add_subdirectory(src/benchmarks)
add_subdirectory(src/experiments)

# Thrift generated files have unused variables.  Ignore those compiler
# warnings by adding this flag.  Note: impala subdirectories should be
# added *before* this so we can fix our issues.
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-variable")
add_subdirectory(generated-sources/gen-cpp)

link_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/build/catalog
  ${CMAKE_CURRENT_SOURCE_DIR}/build/common
  ${CMAKE_CURRENT_SOURCE_DIR}/build/exec
  ${CMAKE_CURRENT_SOURCE_DIR}/build/exprs
  ${CMAKE_CURRENT_SOURCE_DIR}/build/rpc
  ${CMAKE_CURRENT_SOURCE_DIR}/build/runtime
  ${CMAKE_CURRENT_SOURCE_DIR}/build/statestore
  ${CMAKE_CURRENT_SOURCE_DIR}/build/service
  ${CMAKE_CURRENT_SOURCE_DIR}/build/testutil
  ${CMAKE_CURRENT_SOURCE_DIR}/build/util
  ${CMAKE_CURRENT_SOURCE_DIR}/build/transport
)

# only generate statically linked libs and executables
set(BUILD_SHARED_LIBS OFF)

# where to put generated libraries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")
