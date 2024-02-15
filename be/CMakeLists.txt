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

# generate CTest input files
enable_testing()

# Setting this enables compiling for assembly output.  To compile to assembly:
#  1. cd into the directory containing the source file
#  2. 'make help' will list the assembly file targets (i.e. <srcfile.s>
#  3. 'make <srcfile>.s' to build the assembly for that file.  The file is built
#      to CMakeFiles/<currentdir>.dir/<srcfile>.s
PROJECT(ASSEMBLER)

option(BUILD_WITH_NO_TESTS "Do not generate test and benchmark targets" OFF)

# Validate the IMPALA_LINKER environment variable
if (NOT "$ENV{IMPALA_LINKER}" STREQUAL "ld" AND
    NOT "$ENV{IMPALA_LINKER}" STREQUAL "gold" AND
    NOT "$ENV{IMPALA_LINKER}" STREQUAL "mold")
  message(FATAL_ERROR "Invalid IMPALA_LINKER: $ENV{IMPALA_LINKER} (expected: ld, gold, or mold)")
endif()

# compiler flags that are common across debug/release builds
#  -Wall: Enable all warnings.
#  -Wno-sign-compare: suppress warnings for comparison between signed and unsigned
#    integers
#   -fno-strict-aliasing: disable optimizations that assume strict aliasing. This
#       is unsafe to do if the code uses casts (which we obviously do).
#  -Wno-unknown-pragmas: suppress warnings for unknown (compiler specific) pragmas
#  -fsigned-char: on aarch64 platform, type of char default is unsigned char, here
#        set it to signed-char to be compatible with x86-64
#  -Wno-vla: we use C99-style variable-length arrays
#  -pthread: enable multithreaded malloc
#  -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG: enable nanosecond precision for boost
#  -fno-omit-frame-pointers: Keep frame pointer for functions in register
if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -march=armv8-a+crc")
endif()
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wall -Wno-sign-compare -Wno-unknown-pragmas -pthread")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fno-strict-aliasing -fno-omit-frame-pointer")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fsigned-char")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -std=c++17")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-vla")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_SYSTEM_NO_DEPRECATED")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_BIND_GLOBAL_PLACEHOLDERS")
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_ALLOW_DEPRECATED_HEADERS")
#  -DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX
#      For higher portability of the built binaries, switch to /dev/[u]random
#      even if getrandom(2) is available. This is to allow for running binaries
#      built at OS where getrandom(2) is available at OSes where getrandom(2)
#      isn't supported (e.g., that might happen in containerized deployments).
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DBOOST_UUID_RANDOM_PROVIDER_FORCE_POSIX")
IF($ENV{IMPALA_LINKER} STREQUAL "mold")
  # Only very recent GCC 12+ has support for -fuse-ld=mold, so we override "ld" by
  # putting Mold's libexec/mold directory (which has a "ld" symlink) on the path.
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -B $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/mold-$ENV{IMPALA_MOLD_VERSION}/libexec/mold")
ENDIF()
# Note: apart from gold linker, binutils provides an up-to-date "as" utility. Older
# distributions will have an "as" utility too old to process the output from
# modern GCC.
SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -B $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/binutils-$ENV{IMPALA_BINUTILS_VERSION}/bin/")
#  -Wno-deprecated-declarations: OpenSSL3 deprecated various APIs currently used by
#      Impala, so this disables those warnings when using OpenSSL3 until they can be
#      addressed. See IMPALA-12226.
if (OPENSSL_VERSION VERSION_GREATER_EQUAL 3)
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -Wno-deprecated-declarations")
endif()
IF($ENV{IMPALA_LINKER} STREQUAL "gold")
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -fuse-ld=gold")
ENDIF()

if(BUILD_SHARED_LIBS)
  # There is some logic in be/src/kudu/util/debug/unwind_safeness.cc that needs to adapt
  # when using shared libraries. See IMPALA-11640.
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -DIMPALA_SHARED_LIBRARY")
endif()

# On Apple we build with clang and need libstdc++ instead of libc++
if (APPLE)
  SET(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libstdc++")
endif()

SET(CXX_COVERAGE_FLAGS "-fprofile-arcs -ftest-coverage -DCODE_COVERAGE_ENABLED")

# For any clang builds (currently only ASAN):
#   -Qunused-arguments: quiet warnings about unused arguments to clang because ccache
#        makes extra calls to clang which may have extra includes (-I) that are unused.
#   -fcolor-diagnostics: ensure clang generates colorized output, which is necessary
#        when using ccache as clang thinks it is not called from a terminal.
#   -Wno-zero-as-null-pointer-constant: We are slowly moving towards the use of nullptr,
#        but till we switch to it completely, we will ignore the warnings due to use of
#        NULL as a null pointer constant.
#   -Wno-c++17-extensions: ignore warnings caused due to the use of [[nodiscard]]
#        attribute which our current compiler does not support but is used in conjunction
#        with WARN_UNUSED_RESULT with our current toolchain to be effective.
#   -Wno-inconsistent-missing-destructor-override: ignore warnings to mark virtual
#        destructors with 'override' which is enforced by clang by not recommended by c++
#        core guidelines (read C.128).
SET(CXX_CLANG_FLAGS "-Qunused-arguments -fcolor-diagnostics -Wno-unused-local-typedef")
SET(CXX_CLANG_FLAGS "${CXX_CLANG_FLAGS} -fsigned-char")
if (CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  SET(CXX_CLANG_FLAGS "${CXX_CLANG_FLAGS} -march=armv8-a+crc")
endif()
SET(CXX_CLANG_FLAGS "${CXX_CLANG_FLAGS} -Wno-zero-as-null-pointer-constant")
SET(CXX_CLANG_FLAGS "${CXX_CLANG_FLAGS} -Wno-c++17-extensions")
SET(CXX_CLANG_FLAGS "${CXX_CLANG_FLAGS} -Wno-inconsistent-missing-destructor-override")
SET(CXX_CLANG_FLAGS "${CXX_CLANG_FLAGS} -Wno-return-type-c-linkage")
SET(CXX_CLANG_FLAGS "${CXX_CLANG_FLAGS} -DCALLONCEHACK")
# For any gcc builds:
#   -g: Enable symbols for profiler tools
#   -Wno-unused-local-typedefs: Do not warn for local typedefs that are unused.
#   -gdwarf-4: Set the appropriate DWARF version. Later versions of DWARF have better
#    support for newer C++ language features and better compression, but require newer
#    versions of GDB. DWARF 4 requires GDB 7.0 or above.
#   -Wno-maybe-unitialized: Do not warn for variables that might be uninitialized
SET(CXX_GCC_FLAGS "-g -Wno-unused-local-typedefs -gdwarf-4 -Wno-maybe-uninitialized")
# There are some GCC warnings added in recent versions that current code hits.
# These can be addressed over time, because they also appear in the headers of
# some of our dependencies:
#   -Wno-class-memaccess: This warning was added in GCC 8. This impacts a lot of
#    locations (e.g. Tuple and TimestampValue) as well as rapidjson. This warning
#    doesn't seem particularly useful for us.
#   -Wno-init-list-lifetime: This warning was added in GCC 9, and several code pieces
#    are not clean yet (including some LLVM code).
# TODO: These should be cleaned up and reenabled.
SET(CXX_GCC_FLAGS "${CXX_GCC_FLAGS} -Wno-class-memaccess -Wno-init-list-lifetime")

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For CMAKE_BUILD_TYPE=DEBUG_NOOPT
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=Debug
#   (Same as CMAKE_BUILD_TYPE=DEBUG_NOOPT) +
#   -Og: Enable basic optimizations
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -DNDEBUG: Turn off dchecks/asserts/debug only code.
SET(CXX_FLAGS_DEBUG_NOOPT "${CXX_GCC_FLAGS} -ggdb")
# -Werror: compile warnings should be errors when using the toolchain compiler.
#   Enabled for DEBUG, ASAN, TSAN and UBSAN builds which are built pre-commit.
SET(CXX_FLAGS_DEBUG_NOOPT "${CXX_FLAGS_DEBUG_NOOPT} -Werror")
# The legacy debug mode built without optimizations, as optimizations can interfere with
# debuggability. The DEBUG_NOOPT mode maintains this old behavior, while the default
# Debug mode now applies basic optimizations (-Og) to speed up test runs.
SET(CXX_FLAGS_DEBUG "${CXX_FLAGS_DEBUG_NOOPT} -Og")

SET(CXX_FLAGS_RELEASE "${CXX_GCC_FLAGS} -O3 -DNDEBUG")
SET(CXX_FLAGS_ADDRESS_SANITIZER
  "${CXX_CLANG_FLAGS} -Werror -O1 -g -fsanitize=address -fno-omit-frame-pointer -DADDRESS_SANITIZER")

# Set the flags to the undefined behavior sanitizer, also known as "ubsan"
# Turn on sanitizer and debug symbols to get stack traces:
SET(CXX_FLAGS_UBSAN "${CXX_CLANG_FLAGS} -Werror -ggdb3 -fno-omit-frame-pointer -fsanitize=undefined")
# Set preprocessor macros to facilitate initialization the relevant configuration.
SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_UBSAN} -DUNDEFINED_SANITIZER")
# Calling getenv() in __ubsan_default_options doesn't work, likely because of
# initialization ordering. We need to double-quote to create a macro that expands
# to a string-literal.
SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_UBSAN} -DUNDEFINED_SANITIZER_SUPPRESSIONS=\\\"$ENV{IMPALA_HOME}/bin/ubsan-suppressions.txt\\\"")
# Add flags to enable symbol resolution in the stack traces:
SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_UBSAN} -rtlib=compiler-rt -lgcc_s")
# Ignore a number of noisy errors with too many false positives:
SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_UBSAN} -fno-sanitize=alignment,function,vptr,float-divide-by-zero,float-cast-overflow")
# Don't enforce wrapped signed integer arithmetic so that the sanitizer actually sees
# undefined wrapping:
SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_UBSAN} -fno-wrapv")
# To ease debugging, turn off all optimizations:
SET(CXX_FLAGS_UBSAN "${CXX_FLAGS_UBSAN} -O0")

# Set the flags to the thread sanitizer, also known as "tsan"
# Turn on sanitizer and debug symbols to get stack traces:
SET(CXX_FLAGS_TSAN "${CXX_CLANG_FLAGS} -Werror -O1 -ggdb3 -fno-omit-frame-pointer")
SET(CXX_FLAGS_TSAN "${CXX_FLAGS_TSAN} -fsanitize=thread -DTHREAD_SANITIZER -DDYNAMIC_ANNOTATIONS_ENABLED")
SET(CXX_FLAGS_TSAN "${CXX_FLAGS_TSAN} -DTHREAD_SANITIZER_SUPPRESSIONS=\\\"$ENV{IMPALA_HOME}/bin/tsan-suppressions.txt\\\"")

SET(CXX_FLAGS_TIDY "${CXX_CLANG_FLAGS}")
# Catching unused variables requires an optimization level greater than 0
SET(CXX_FLAGS_TIDY "${CXX_FLAGS_TIDY} -O1")
# Ignore assert() and DCHECK() to avoid dead code errors on "DCHECK(false); return
# nullptr" in impossible default switch/case statements.
SET(CXX_FLAGS_TIDY "${CXX_FLAGS_TIDY} -DNDEBUG")
# Clang-tidy's clang-diagnostic issues are sourced from Clang warnings, so there can
# only be clang-diagnostic issues for warnings that are enabled. Warnings change across
# Clang releases and most are disabled via the .clang-tidy's "Checks" value. To avoid
# enormous output, this only enables -Wall and -Wextra.
SET(CXX_FLAGS_TIDY "${CXX_FLAGS_TIDY} -Wall -Wextra")
# The Tidy build output can be verbose and it is unlikely to be viewed in a terminal.
# It usually is redirected to less, a log file, or /dev/null. In those places color
# codes just make the output harder to read.
SET(CXX_FLAGS_TIDY "${CXX_FLAGS_TIDY} -fno-color-diagnostics")

# Set compile flags based on the build type.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG_NOOPT")
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_DEBUG_NOOPT})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  SET(CMAKE_CXX_FLAGS ${CXX_FLAGS_RELEASE})
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_ADDRESS_SANITIZER}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TIDY")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_TIDY}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_UBSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN_FULL")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_UBSAN}")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUNDEFINED_SANITIZER_FULL")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_TSAN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "TSAN_FULL")
  SET(CMAKE_CXX_FLAGS "${CXX_FLAGS_TSAN}")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DTHREAD_SANITIZER_FULL")
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif()

if (ENABLE_CODE_COVERAGE)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COVERAGE_FLAGS}")
endif()

# Add flags that are common across build types
#  - fverbose-asm creates better annotated assembly.  This doesn't seem to affect
#    when building the binary.
# LLMV_CFLAGS - Adding llvm compile flags
SET(CMAKE_CXX_FLAGS "${CXX_COMMON_FLAGS} ${CMAKE_CXX_FLAGS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fverbose-asm")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${LLVM_CFLAGS}")

# The IMPALA_MINIMAL_DEBUG_INFO option saves diskspace by reducing the debug info
# in binaries to the minimal level that can do backtraces. The "-g1" option
# keeps line number tables, but it does not keep variable information. This
# can reduce the size of binaries by >%60. This is appended to the end of arguments
# so that it overrides other "-g" arguments.
if ($ENV{IMPALA_MINIMAL_DEBUG_INFO} STREQUAL "true")
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g1")
  # The choice of CMAKE_BUILD_TYPE specifies a set of flags that are added
  # after the flags in CMAKE_CXX_FLAGS. CMAKE_BUILD_TYPE=Debug adds "-g", which
  # overrides our "-g1" because it is later in the argument list. To fix this,
  # this overrides CMake's flags for CMAKE_BUILD_TYPE=Debug to use "-g1" rather
  # than "-g". CMAKE_BUILD_TYPE=Release and other CMAKE_BUILD_TYPEs that we use
  # don't include a "-g" flag, so they don't need similar treatment.
  SET(CMAKE_CXX_FLAGS_DEBUG "-g1")
endif()

# The IMPALA_COMPRESSED_DEBUG_INFO option saves diskspace by compressing the
# debug info in the executable. This can reduce the size of binaries by >50%
# without changing the amount of debug information. gdb is known to work
# with compressed debug info, but other tools may not know how to use it.
if ($ENV{IMPALA_COMPRESSED_DEBUG_INFO} STREQUAL "true")
  # Clang doesn't handle -gz properly until version 12, so there is no reason to keep it.
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
      AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 12.0)
    message(STATUS "Detected Clang < 12: -gz is ineffective on this version, skipping.")
  else()
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -gz")
  endif()
endif()

# Use ccache when found and not explicitly disabled by setting the DISABLE_CCACHE envvar.
find_program(CCACHE ccache)
set(RULE_LAUNCH_PREFIX)
if (CCACHE AND NOT DEFINED ENV{DISABLE_CCACHE})
  set(RULE_LAUNCH_PREFIX ccache)
  if ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER"
      OR "${CMAKE_BUILD_TYPE}" STREQUAL "TIDY"
      OR "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN"
      OR "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN_FULL"
      OR "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN"
      OR "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN_FULL")
    # Need to set CCACHE_CPP so that ccache calls clang with the original source file for
    # both preprocessing and compilation. Otherwise, ccache will use clang to preprocess
    # the file and then call clang with the preprocessed output if not cached. However,
    # the preprocessed output from clang may contain code (e.g. from macro expansions)
    # that generates compilation warnings that would not be reported if compiling the
    # original source directly with clang.
    SET(ENV{CCACHE_CPP} YES)
  endif()
endif()

# There can be RULE_LAUNCH_COMPILE / RULE_LAUNCH_LINK settings already at the parent
# level. The parent layer should wrap any launcher used here.
get_property(PARENT_RULE_LAUNCH_COMPILE GLOBAL PROPERTY RULE_LAUNCH_COMPILE)
get_property(PARENT_RULE_LAUNCH_LINK GLOBAL PROPERTY RULE_LAUNCH_LINK)

set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE
  "${PARENT_RULE_LAUNCH_COMPILE} ${RULE_LAUNCH_PREFIX}")
set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK
  "${PARENT_RULE_LAUNCH_LINK} ${RULE_LAUNCH_PREFIX}")

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DHAVE_NETDB_H)

# Kudu flags. 1. Enable full support for all backing types of kudu::Slices.
# 2. Don't include stubs.h
add_definitions(-DKUDU_HEADERS_USE_RICH_SLICE -DKUDU_HEADERS_NO_STUBS)

# Set clang flags for cross-compiling to IR.
# IR_COMPILE is #defined for the cross compile to remove code that bloats the IR.
# Optimization is omitted and left up to individual uses.
#  -Wno-return-type-c-linkage: UDFs return C++ classes but use C linkage to prevent
#       mangling
#  -DBOOST_NO_EXCEPTIONS: call a custom error handler for exceptions in codegen'd code.
set(CLANG_IR_CXX_FLAGS "-emit-llvm" "-c" "-std=c++17" "-DIR_COMPILE" "-DHAVE_INTTYPES_H"
  "-DHAVE_NETINET_IN_H" "-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG" "-DBOOST_NO_EXCEPTIONS"
  "-DBOOST_BIND_GLOBAL_PLACEHOLDERS" "-DBOOST_ALLOW_DEPRECATED_HEADERS"
  "-DKUDU_HEADERS_NO_STUBS" "-fcolor-diagnostics"
  "-Wno-return-type-c-linkage" "-fsigned-char")
#  -Wno-deprecated-declarations: OpenSSL3 deprecated various APIs currently used by
#      Impala, so this disables those warnings when using OpenSSL3 until they can be
#      addressed. See IMPALA-12226.
if (OPENSSL_VERSION VERSION_GREATER_EQUAL 3)
  SET(CLANG_IR_CXX_FLAGS "${CLANG_IR_CXX_FLAGS}" "-Wno-deprecated-declarations")
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
  set(CLANG_IR_CXX_FLAGS "${CLANG_IR_CXX_FLAGS}" "-march=armv8-a+crc"
    "-DCACHELINESIZE_AARCH64=${CACHELINESIZE_AARCH64}")
endif()

# -Werror: compile warnings should be errors when using the toolchain compiler.
set(CLANG_IR_CXX_FLAGS "${CLANG_IR_CXX_FLAGS}" "-Werror")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER")
  SET(CLANG_IR_CXX_FLAGS "${CLANG_IR_CXX_FLAGS}" "-DADDRESS_SANITIZER")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  SET(CLANG_IR_CXX_FLAGS "${CLANG_IR_CXX_FLAGS}" "-DNDEBUG")
endif()

if ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN_FULL")
  set(CLANG_IR_CXX_FLAGS "${CLANG_IR_CXX_FLAGS}" "-DUNDEFINED_SANITIZER"
    "-fno-omit-frame-pointer" "-fsanitize=undefined" "-fno-wrapv" "-ggdb3"
    "-fno-sanitize=alignment,function,vptr,float-divide-by-zero,float-cast-overflow"
    "-DUNDEFINED_SANITIZER_SUPPRESSIONS=\\\"$ENV{IMPALA_HOME}/bin/ubsan-suppressions.txt\\\"")
endif()

IF($ENV{ENABLE_IMPALA_IR_DEBUG_INFO} STREQUAL "true")
  # -g: emit debug symbols in IR. These increase IR size and memory overhead of LLVM, but
  #     are useful for debugging codegened code and interpreting codegen disassembly
  #     dumps.
  SET(CLANG_IR_CXX_FLAGS "${CLANG_IR_CXX_FLAGS}" "-g")
endif()

# Flags to pass to LLVM's opt to further optimize cross-compiled IR.
#  -inline: inline with low threshold to get rid of trivial accessor functions.
set(LLVM_OPT_IR_FLAGS "-inline" "-inlinehint-threshold=10" "-inline-threshold=10")

# Additional compile flags that will hide symbols by default, e.g. for building
# UDFs. We have both a concatenated string version and a list version for convenience,
# depending on what is needed in the context.
set(HIDE_SYMBOLS "-fvisibility=hidden -fvisibility-inlines-hidden")
set(HIDE_SYMBOLS_ARGS "${HIDE_SYMBOLS_STRING}")
separate_arguments(HIDE_SYMBOLS_ARGS)

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

# Ensure that clang uses the gcc toolchain headers.
set(CLANG_BASE_FLAGS --gcc-toolchain=${GCC_ROOT})
set(CLANG_INCLUDE_FLAGS ${CLANG_BASE_FLAGS})

set(CLANG_INCLUDE_FLAGS
  ${CLANG_INCLUDE_FLAGS}
  "-I${CMAKE_CURRENT_SOURCE_DIR}/src"
  "-I${CMAKE_CURRENT_SOURCE_DIR}/generated-sources"
  "-I${THRIFT_CPP_INCLUDE_DIR}"
  "-I${SQUEASEL_INCLUDE_DIR}"
  "-I${GLOG_INCLUDE_DIR}"
  "-I${GFLAGS_INCLUDE_DIR}"
  "-I${GTEST_INCLUDE_DIR}"
  "-I${JWT_CPP_INCLUDE_DIR}"
  "-I${RAPIDJSON_INCLUDE_DIR}"
  "-I${AVRO_INCLUDE_DIR}"
  "-I${ORC_INCLUDE_DIR}"
  # Include Boost as a system directory to suppress warnings from headers.
  "-isystem${BOOST_INCLUDEDIR}"
  "-I${KUDU_CLIENT_INCLUDE_DIR}"
  # Required so that jni.h can be found during Clang compilation
  "-I${JAVA_INCLUDE_PATH}"
  "-I${JAVA_INCLUDE_PATH2}"
  "-I${RE2_INCLUDE_DIR}"
  "-I${SASL_INCLUDE_DIR}"
  "-I${BZIP2_INCLUDE_DIR}"
  "-I${ZLIB_INCLUDE_DIR}"
  "-I${OPENSSL_INCLUDE_DIR}"
  "-I${LDAP_INCLUDE_DIR}"
  "-I${PROTOBUF_INCLUDE_DIR}"
  "-I${CCTZ_INCLUDE_DIR}"
  "-I${CURL_INCLUDE_DIR}"
)

# allow linking of static libs into dynamic lib
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# set compile output directory
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG_NOOPT" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN_FULL" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN" OR
    "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN_FULL")
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/debug/")
else()
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/build/release/")
endif()

# Create a latest link so that scripts can pick up the correct build automatically
FILE(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
if (NOT APPLE)
  set(MORE_ARGS "-T")
endif()
EXECUTE_PROCESS(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
  ${CMAKE_CURRENT_SOURCE_DIR}/build/latest)

# Determine what functions are available on the current platform.
INCLUDE(CheckFunctionExists)
CHECK_FUNCTION_EXISTS(sched_getcpu HAVE_SCHED_GETCPU)
CHECK_FUNCTION_EXISTS(pipe2 HAVE_PIPE2)
CHECK_FUNCTION_EXISTS(sync_file_range HAVE_SYNC_FILE_RANGE)

# linux/fs.h defines HAVE_FALLOCATE whether or not the function is available,
# which is why we use IMPALA_HAVE_FALLOCATE here.
CHECK_FUNCTION_EXISTS(fallocate IMPALA_HAVE_FALLOCATE)
CHECK_FUNCTION_EXISTS(preadv HAVE_PREADV)
INCLUDE(CheckIncludeFiles)
CHECK_INCLUDE_FILES(linux/magic.h HAVE_MAGIC_H)

# Used to check if we're using krb-1.6 or lower.
CHECK_LIBRARY_EXISTS("krb5" krb5_get_init_creds_opt_set_fast_ccache_name
  ${KERBEROS_LIBRARY} HAVE_KRB5_GET_INIT_CREDS_OPT_SET_FAST_CCACHE_NAME)

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
set (IMPALA_LIBS
  BufferPool
  Catalog
  CodeGen
  Common
  Exec
  ExecIr
  ExecAvro
  ExecAvroIr
  ExecHBase
  ExecJson
  ExecKudu
  ExecKuduIr
  ExecOrc
  ExecParquet
  ExecRcfile
  ExecSequence
  ExecText
  ExecIcebergMetadata
  Exprs
  ExprsIr
  GlobalFlags
  histogram_proto
  ImpalaThrift
  Io
  kudu_curl_util
  kudu_util
  krpc
  Rpc
  rpc_header_proto
  rpc_introspection_proto
  pb_util_proto
  Runtime
  RuntimeIr
  Scheduling
  security
  Service
  Statestore
  ThriftSaslTransport
  token_proto
  Udf
  UdfIr
  Util
  UtilIr
  UtilCache
)

if (NOT BUILD_WITH_NO_TESTS)
  set(IMPALA_LIBS ${IMPALA_LIBS} TestUtil)
endif()

set (IMPALA_LINK_LIBS
  ${WL_START_GROUP}
  ${IMPALA_LIBS}
  ${WL_END_GROUP}
)

# Backend tests originally produced a single executable for each backend c++ test file.
# Since these executables linked in all of the libraries, each test is very large
# (100s of MB) and requires considerable link time. To address this, tests can now
# be linked into a unified test executable that contains tests from many backend
# c++ test files. See the ADD_UNIFIED_BE_TEST and ADD_UNIFIED_LSAN_BE_TEST
# macros below. The original mode of producing a standalone executable is still
# available via the ADD_BE_TEST and ADD_LSAN_BE_TEST macros.
#
# To make a unified test executable, the backend tests need to be in their own libraries.
# The main function is provided by the unified main c++ file. None of the test c++ files
# has a main function. Normal dependency resolution would not include any of the tests
# in any executable, as no function references them. Force the unified test executable
# to include the tests by using "--whole-archive".
set(WL_WHOLE_ARCHIVE "-Wl,--whole-archive")
set(WL_NO_WHOLE_ARCHIVE "-Wl,--no-whole-archive")
set (UNIFIED_TEST_LIBS
  BufferPoolTests
  CatalogTests
  CodeGenTests
  CommonTests
  ExecTests
  ExecAvroTests
  ExecJsonTests
  ExecParquetTests
  ExprsTests
  GUtilTests
  IoTests
  RpcTests
  RuntimeTests
  SchedulingTests
  ServiceTests
  UtilTests
  UtilCacheTests
)
set (UNIFIED_TEST_LINK_LIBS
  ${WL_START_GROUP}
  ${WL_WHOLE_ARCHIVE}
  ${UNIFIED_TEST_LIBS}
  ${WL_NO_WHOLE_ARCHIVE}
  ${IMPALA_LIBS}
  ${WL_END_GROUP}
)

# If using dynamic linking, -Wl does not have any effect (it's only for .a files). So we
# need to add these redundant dependencies to resolve the circular references in our
# libraries when dynamic linking is enabled.
if (BUILD_SHARED_LIBS)
  set (IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS}
    BufferPool
    Io
    Runtime
    Exec
    ExecAvro
    ExecHBase
    ExecJson
    ExecKudu
    ExecOrc
    ExecParquet
    ExecRcfile
    ExecSequence
    ExecText
    ExecIcebergMetadata
    CodeGen
    Exprs
    Rpc
    Service
    security
    Statestore
    Scheduling
    Catalog
    ImpalaThrift
    GlobalFlags
    Common
    Udf
    )
endif ()

set (IMPALA_DEPENDENCIES
  snappy
  lz4
  zstd
  re2
  ${Boost_LIBRARIES}
  ${LLVM_MODULE_LIBS}
  thrift
  cyrus_sasl
  ldap
  lber
  ThriftSaslTransport
  openssl_ssl
  openssl_crypto
  crcutil
  gutil
  glog
  gflags
  krb5
  libev
  libunwind
  pprof
  breakpad_client
  hdfs
  zlib
  bzip2
  avro
  orc
  java_jvm
  kudu_client
  cctz
  curl)

# When building with Clang, linking fails because it is trying to
# use a symbol in kudu_client, but that symbol is discarded. To
# hack around this error, the calloncehack shared library defines the
# same symbol publicly. Placing calloncehack ahead of kudu_client
# causes the linker to use its definition rather than kudu_client's.
# The underlying issue is some incompatibility when building with
# Clang while having libraries built with GCC, so this only applies
# to Clang compilation.
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  message(STATUS "C++ compiler is Clang, enabling calloncehack")
  # Put calloncehack at the start of the dependencies. The important thing
  # is that it is ahead of kudu_client.
  set(IMPALA_DEPENDENCIES calloncehack ${IMPALA_DEPENDENCIES})
else()
  message(STATUS "C++ compiler is not Clang, skipping calloncehack")
endif()

# Add all external dependencies. They should come after the impala libs.
set (IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS}
  ${IMPALA_DEPENDENCIES}
  -lrt
  -ldl # Needed for LLVM
)

# Add external dependencies for backend tests
set (UNIFIED_TEST_LINK_LIBS ${UNIFIED_TEST_LINK_LIBS}
  ${IMPALA_DEPENDENCIES}
  -lrt
  -ldl # Needed for LLVM
)

if (ENABLE_CODE_COVERAGE)
  set(IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS} -lgcov)
  set(UNIFIED_TEST_LINK_LIBS ${UNIFIED_TEST_LINK_LIBS} -lgcov)
endif()

# The above link list does not include tcmalloc. This is because the Impala JVM support
# libraries (libfesupport, libloggingsupport) cannot use tcmalloc in all cases. When they
# are started up by the FE (for tests) the jvm has already made allocations before
# tcmalloc can be loaded. In all other binaries, we can use tcmalloc except the address
# sanitizer build. Address sanitizer is incompatible with tcmalloc (they both intercept
# malloc/free)
set (IMPALA_LINK_LIBS_NO_TCMALLOC ${IMPALA_LINK_LIBS})
if (NOT "${CMAKE_BUILD_TYPE}" STREQUAL "ADDRESS_SANITIZER" AND
    NOT "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN" AND
    NOT "${CMAKE_BUILD_TYPE}" STREQUAL "TSAN_FULL")
  set (IMPALA_LINK_LIBS ${IMPALA_LINK_LIBS} tcmallocstatic)
  set (UNIFIED_TEST_LINK_LIBS ${UNIFIED_TEST_LINK_LIBS} tcmallocstatic)
endif()

# When we link statically, we need to replace the static libhdfs.a with the dynamic
# version otherwise the dynamic support libraries will pickup the static libhdfs.a
# library. The result will not compile as libhdfs.a is not compiled with -fpic. The same
# is true for other system dependencies that we don't have control over.
set(IMPALA_LINK_LIBS_DYNAMIC_TARGETS ${IMPALA_LINK_LIBS_NO_TCMALLOC})
list(REMOVE_ITEM IMPALA_LINK_LIBS_DYNAMIC_TARGETS hdfs)
set(IMPALA_LINK_LIBS_DYNAMIC_TARGETS ${IMPALA_LINK_LIBS_DYNAMIC_TARGETS}
  ${HDFS_SHARED_LIB})

# Link libs for test executables.  Although not all tests need all libs,
# the build time for the tests is rather small and not worth the trouble.
# TODO: build time for tests is no longer small, but our dependencies are now very
# complicated and hard to isolate
set (IMPALA_TEST_LINK_LIBS ${IMPALA_LINK_LIBS} gtest)
set (UNIFIED_TEST_LINK_LIBS ${UNIFIED_TEST_LINK_LIBS} gtest)

MESSAGE(STATUS "Compiler Flags: ${CMAKE_CXX_FLAGS}")

if (CMAKE_DEBUG)
  MESSAGE(STATUS "Linker Libs: ${IMPALA_LINK_LIBS}")
endif()

set(LLVM_IR_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/llvm-ir")
file(MAKE_DIRECTORY ${LLVM_IR_OUTPUT_DIRECTORY})

if (NOT BUILD_WITH_NO_TESTS)
  # Add custom target to only build the backend tests
  # Note: this specifies "ALL" so it builds if running "make" with no arguments. This is
  # necessary due to the non-executable targets (i.e. generating backend test scripts)
  # that run for the unified backend tests.
  add_custom_target(be-test ALL)

  # Add custom target to build unified backend tests
  add_custom_target(unified-be-test)

  # Add custom target to build the unified backend test executable
  add_custom_target(unified-be-test-executable)
endif()

# Variable to use to aggregate all of the filter patterns, joined by ":"
set_property(GLOBAL PROPERTY AGG_UNIFIED_FILTER_PATTERN)

# Utility CMake functions for specifying tests and benchmarks

# ADD_BE_TEST: This function adds a backend test with its own executable. The associated
# c++ file must have its own main() function.
FUNCTION(ADD_BE_TEST TEST_NAME)
  # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
  file(RELATIVE_PATH DIR_NAME "${CMAKE_SOURCE_DIR}/be/src/" ${CMAKE_CURRENT_SOURCE_DIR})
  ADD_EXECUTABLE(${TEST_NAME} ${TEST_NAME}.cc)
  TARGET_LINK_LIBRARIES(${TEST_NAME} ${IMPALA_TEST_LINK_LIBS})
  set(CMAKE_EXE_LINKER_FLAGS "--start-group")
  ADD_TEST(NAME ${TEST_NAME}
    COMMAND "${CMAKE_SOURCE_DIR}/bin/run-jvm-binary.sh"
            "${BUILD_OUTPUT_ROOT_DIRECTORY}/${DIR_NAME}/${TEST_NAME}"
            -log_dir=$ENV{IMPALA_BE_TEST_LOGS_DIR})
  ADD_DEPENDENCIES(be-test ${TEST_NAME})
ENDFUNCTION()

# ADD_UNIFIED_BE_TEST: This function adds a backend test that is part of the unified
# backend executable. This creates an executable script that runs the unified executable
# with appropriate args to run the subset of tests identified by "TEST_FILTER_PATTERN".
# See the documentation for --gtest_filter for examples of filter patterns.
FUNCTION(ADD_UNIFIED_BE_TEST TEST_NAME TEST_FILTER_PATTERN)
  # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
  file(RELATIVE_PATH DIR_NAME "${CMAKE_SOURCE_DIR}/be/src/" ${CMAKE_CURRENT_SOURCE_DIR})
  add_custom_target(${TEST_NAME} "${CMAKE_SOURCE_DIR}/bin/gen-backend-test-script.py"
    "--test_script_output" "${BUILD_OUTPUT_ROOT_DIRECTORY}/${DIR_NAME}/${TEST_NAME}"
    "--gtest_filter" ${TEST_FILTER_PATTERN})
  # Incorporate this TEST_FILTER_PATTERN into the aggregate list of filter patterns
  get_property(tmp GLOBAL PROPERTY AGG_UNIFIED_FILTER_PATTERN)
  set(tmp "${TEST_FILTER_PATTERN}:${tmp}")
  set_property(GLOBAL PROPERTY AGG_UNIFIED_FILTER_PATTERN "${tmp}")
  ADD_TEST(NAME ${TEST_NAME}
    COMMAND  "${CMAKE_SOURCE_DIR}/bin/run-jvm-binary.sh"
             "${BUILD_OUTPUT_ROOT_DIRECTORY}/${DIR_NAME}/${TEST_NAME}"
             -log_dir=$ENV{IMPALA_BE_TEST_LOGS_DIR})
  ADD_DEPENDENCIES(unified-be-test ${TEST_NAME})
  ADD_DEPENDENCIES(${TEST_NAME} unified-be-test-validated-executable)
ENDFUNCTION()

FUNCTION(ENABLE_LSAN_FOR_TEST TEST_NAME)
  SET_TESTS_PROPERTIES(${TEST_NAME} PROPERTIES ENVIRONMENT
      "ASAN_OPTIONS=handle_segv=0 detect_leaks=1 allocator_may_return_null=1")
  SET_TESTS_PROPERTIES(${TEST_NAME} PROPERTIES ENVIRONMENT
      "LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/bin/lsan-suppressions.txt")
ENDFUNCTION()

# ADD_BE_LSAN_TEST: Same as ADD_BE_TEST, but also enable LeakSanitizer.
# TODO: IMPALA-2746: we should make this the default.
FUNCTION(ADD_BE_LSAN_TEST TEST_NAME)
  ADD_BE_TEST(${TEST_NAME})
  ENABLE_LSAN_FOR_TEST(${TEST_NAME})
ENDFUNCTION()

# ADD_UNIFIED_BE_LSAN_TEST: Same as ADD_UNIFIED_BE_TEST, but also enable LeakSanitizer.
# TODO: IMPALA_2746: we should make this the default.
FUNCTION(ADD_UNIFIED_BE_LSAN_TEST TEST_NAME TEST_FILTER_PATTERN)
  ADD_UNIFIED_BE_TEST(${TEST_NAME} ${TEST_FILTER_PATTERN})
  ENABLE_LSAN_FOR_TEST(${TEST_NAME})
ENDFUNCTION()

# Similar utility function for tests that use the UDF SDK
FUNCTION(ADD_UDF_TEST TEST_NAME)
  # This gets the directory where the test is from (e.g. 'exprs' or 'runtime')
  get_filename_component(DIR_NAME ${CMAKE_CURRENT_SOURCE_DIR} NAME)
  ADD_EXECUTABLE(${TEST_NAME} ${TEST_NAME}.cc)
  # Set ImpalaUdf as the first link library for UDF tests. This will cause its test
  # definitions to be linked instead of subsequent non-test definitions. Otherwise the
  # test definitions of MemTracker, etc. will be used in the udf.cc compilation unit, but
  # the Runtime method implementations will be linked. See IMPALA-3132.
  TARGET_LINK_LIBRARIES(${TEST_NAME} ImpalaUdf ${IMPALA_TEST_LINK_LIBS})
  set(CMAKE_EXE_LINKER_FLAGS "--start-group")
  ADD_TEST(NAME ${TEST_NAME}
    COMMAND "${CMAKE_SOURCE_DIR}/bin/run-jvm-binary.sh"
            "${BUILD_OUTPUT_ROOT_DIRECTORY}/${DIR_NAME}/${TEST_NAME}"
            -log_dir=$ENV{IMPALA_BE_TEST_LOGS_DIR})
  ADD_DEPENDENCIES(be-test ${TEST_NAME})
  ENABLE_LSAN_FOR_TEST(${TEST_NAME})
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
    COMMAND ${LLVM_CLANG_EXECUTABLE} ${CLANG_IR_CXX_FLAGS} -O2 ${HIDE_SYMBOLS_ARGS}
            ${CLANG_INCLUDE_FLAGS} ${SRC_FILE} -o ${OUTPUT_FILE}
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
add_subdirectory(src/exprs)
add_subdirectory(src/kudu/security)
add_subdirectory(src/kudu/rpc)
add_subdirectory(src/kudu/util)
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

if (NOT BUILD_WITH_NO_TESTS)
  # Add custom target to validate the unified backend test executable and test match
  # patterns. At this point, all filter patterns have been aggregated from the individual
  # ADD_UNIFIED_BE_TEST calls into AGG_UNIFIED_FILTER_PATTERN.
  get_property(TOTAL_UNIFIED_FILTER_PATTERN GLOBAL PROPERTY AGG_UNIFIED_FILTER_PATTERN)
  add_custom_target(unified-be-test-validated-executable
    "${CMAKE_CURRENT_SOURCE_DIR}/../bin/validate-unified-backend-test-filters.py"
    "-f" "${TOTAL_UNIFIED_FILTER_PATTERN}"
    "-b" "${BUILD_OUTPUT_ROOT_DIRECTORY}/service/unifiedbetests")

  ADD_DEPENDENCIES(be-test unified-be-test)
  ADD_DEPENDENCIES(unified-be-test unified-be-test-validated-executable)
  ADD_DEPENDENCIES(unified-be-test-validated-executable unified-be-test-executable)
endif()

# only generate statically linked libs and executables
set(BUILD_SHARED_LIBS OFF)

# where to put generated libraries
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")
