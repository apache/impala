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

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/paimon")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/paimon")

add_library(ExecPaimon
 paimon-jni-row-reader.cc
 paimon-jni-scan-node.cc
 paimon-jni-scanner.cc
 paimon-scan-plan-node.cc
)

# Below is a work-around to fix clang compile issue. For the normal build option,
# the compile will be successful. The issue will occur if CMAKE_BUILD_TYPE has
# following types: ADDRESS_SANITIZER, TIDY, UBSAN,UBSAN_FULL, TSAN, TSAN_FULL.
# For the normal build option, will invoke g++, however for the above build type,
# build script will use clang++ instead of g++ to compile, in this case, following
# error will occur:
# The detailed errors are:
# error: cannot cast private base class 'std::__detail::__variant::_Variant_storage
#     <false, arrow::FieldPath, std::__cxx11::basic_string<char>,
#     std::vector<arrow::FieldRef, std::allocator<arrow::FieldRef> > >' to
#     'std::variant<arrow::FieldPath, std::__cxx11::basic_string<char>,
#     std::vector<arrow::FieldRef, std::allocator<arrow::FieldRef> > >'
#             return static_cast<variant<_Types...>&>(__rhs);
#
#
# looks like it hit bug: https://bugs.llvm.org/show_bug.cgi?id=31852
# and the following patch is already applied:
#     https://gcc.gnu.org/cgit/gcc/commit/?id=aafaa3254ec6fc3d5e3a15a40185950d3af04432
# in the libc++ header of our toolchain.
# but looks like the patch above is only fixes the method get
# it doesn't fix the method static_cast.
# here we use the same work-ground for method static_cast.
# NOTE: the fix below only applies to current version of toolchain.
# need to adapt if toolchain is upgraded.
set(PAIMON_PATCH_GCC_HEADER_DIR
  gcc-$ENV{IMPALA_GCC_VERSION}/include/c++/$ENV{IMPALA_GCC_VERSION})

set(PAIMON_PATCH_WORK_DIR
  $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/${PAIMON_PATCH_GCC_HEADER_DIR})

set(PAIMON_PATCH_TARGET_FILE
  ${PAIMON_PATCH_WORK_DIR}/variant)

# change this if upgraded toolchain still haven't fix the issue.
set(PAIMON_PATCH_TARGET_FILE_CHECKSUM_ORIG
  4daf8153a09ee07bab2ac339e21b9725e17a40854a42284c85b3d2ba3c0862e3)

set(PAIMON_PATCH_SENTINEL_FILE
  ${PAIMON_PATCH_WORK_DIR}/variant_patched)
file(SHA256 ${PAIMON_PATCH_TARGET_FILE}
  PAIMON_PATCH_TARGET_FILE_CHECKSUM_CURR)
message("Current Hash Is: "
  ${PAIMON_PATCH_TARGET_FILE_CHECKSUM_CURR})
message("Original Hash Is: "
  ${PAIMON_PATCH_TARGET_FILE_CHECKSUM_ORIG})

if(PAIMON_PATCH_TARGET_FILE_CHECKSUM_ORIG STREQUAL PAIMON_PATCH_TARGET_FILE_CHECKSUM_CURR
  )

  message(STATUS "apply variant patch to fix clang++ static_cast issue.")
  # To fix the compile error, the following tiny patch should be applied to
  # toolchain-packages-gcc10.4.0/gcc-10.4.0/include/c++/10.4.0/variant
  file(WRITE /tmp/variant.patch
    "1296a1297,1299
    > #if defined(__clang__) && __clang_major__ <= 7
    >     : public __detail::__variant::_Variant_base<_Types...>,
    > #else
    1297a1301
    > #endif
  ")
  add_custom_command(
    OUTPUT ${PAIMON_PATCH_SENTINEL_FILE}
    COMMAND cd ${PAIMON_PATCH_WORK_DIR} &&
            patch -b -i /tmp/variant.patch ${PAIMON_PATCH_TARGET_FILE} &&
            touch ${PAIMON_PATCH_SENTINEL_FILE}
    DEPENDS ${PAIMON_PATCH_TARGET_FILE}
    COMMENT "Patching variant file in GCC include directory to fix static_cast issue."
    VERBATIM
  )
  add_custom_target(patch_variant
      DEPENDS ${PAIMON_PATCH_SENTINEL_FILE}
      COMMENT "Variant patch applied."
  )
  add_dependencies(ExecPaimon patch_variant gen-deps)
else()
  message(STATUS "skip apply patch since hash is changed.")
  add_dependencies(ExecPaimon gen-deps)
endif()
