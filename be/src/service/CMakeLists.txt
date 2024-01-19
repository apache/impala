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

# where to put generated libraries.
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# where to put generated binaries.
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# Mark the protobuf file as generated
set_source_files_properties(${CONTROL_SERVICE_PROTO_SRCS} PROPERTIES GENERATED TRUE)
set_source_files_properties(${DATA_STREAM_SERVICE_PROTO_SRCS} PROPERTIES GENERATED TRUE)

add_library(Service
  child-query.cc
  client-request-state.cc
  ${CONTROL_SERVICE_PROTO_SRCS}
  control-service.cc
  ${DATA_STREAM_SERVICE_PROTO_SRCS}
  data-stream-service.cc
  frontend.cc
  fe-support.cc
  hs2-util.cc
  impala-beeswax-server.cc
  impala-hs2-server.cc
  impala-http-handler.cc
  impalad-main.cc
  impala-server.cc
  internal-server.cc
  query-options.cc
  query-result-set.cc
  query-state-record.cc
  workload-management.cc
  workload-management-fields.cc
  workload-management-flags.cc
)
add_dependencies(Service gen-deps)

# this shared library provides Impala executor functionality to FE test.
add_library(fesupport SHARED
  fe-support.cc
)

# Hide all symbols from compression libraries so that users of this libfesupport.so
# don't have conflicts with libhadoop or other libraries.
# TODO: This should also hide most other symbols as a precaution.
# Note: Using the full library name with ".a" is needed for lld/mold linkers.
# It has no impact on gold, which supports with or without the .a.
set(LINK_FLAGS "-Wl,--exclude-libs=libbz2.a")
set(LINK_FLAGS "${LINK_FLAGS} -Wl,--exclude-libs=liblz4.a")
set(LINK_FLAGS "${LINK_FLAGS} -Wl,--exclude-libs=libsnappy.a")
set(LINK_FLAGS "${LINK_FLAGS} -Wl,--exclude-libs=libz.a")
set(LINK_FLAGS "${LINK_FLAGS} -Wl,--exclude-libs=libzstd.a")
set_target_properties(fesupport
  PROPERTIES
  LINK_FLAGS "${LINK_FLAGS}")
add_dependencies(fesupport gen-deps)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN"
    OR "${CMAKE_BUILD_TYPE}" STREQUAL "UBSAN_FULL")
  target_link_libraries(fesupport "-Wl,--whole-archive \
    $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/llvm-$ENV{IMPALA_LLVM_DEBUG_VERSION}/lib/clang/$ENV{IMPALA_LLVM_UBSAN_BASE_VERSION}/lib/linux/libclang_rt.ubsan_standalone_cxx-${CMAKE_SYSTEM_PROCESSOR}.a \
    $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/llvm-$ENV{IMPALA_LLVM_DEBUG_VERSION}/lib/clang/$ENV{IMPALA_LLVM_UBSAN_BASE_VERSION}/lib/linux/libclang_rt.ubsan_standalone-${CMAKE_SYSTEM_PROCESSOR}.a \
    -Wl,--no-whole-archive")
endif()

target_link_libraries(fesupport ${IMPALA_LINK_LIBS_DYNAMIC_TARGETS})

add_executable(impalad
  daemon-main.cc
)

# All Impala daemons run from the same binary. The code that is run is determined by the
# name (i.e. argv[0]) of the command that executes the binary, so we create symlinks for
# statestored and catalogd. The symlinks are relative so they can be installed along with
# the impalad binary.
set (STATESTORED_SYMLINK ${EXECUTABLE_OUTPUT_PATH}/statestored)
set (CATALOGD_SYMLINK ${EXECUTABLE_OUTPUT_PATH}/catalogd)
set (ADMISSIOND_SYMLINK ${EXECUTABLE_OUTPUT_PATH}/admissiond)

ADD_CUSTOM_COMMAND(
  OUTPUT ${STATESTORED_SYMLINK}
  COMMAND ${CMAKE_COMMAND} -E create_symlink impalad ${STATESTORED_SYMLINK}
  DEPENDS impalad)
ADD_CUSTOM_COMMAND(
  OUTPUT ${CATALOGD_SYMLINK}
  COMMAND ${CMAKE_COMMAND} -E create_symlink impalad ${CATALOGD_SYMLINK}
  DEPENDS impalad)
ADD_CUSTOM_COMMAND(
  OUTPUT ${ADMISSIOND_SYMLINK}
  COMMAND ${CMAKE_COMMAND} -E create_symlink impalad ${ADMISSIOND_SYMLINK}
  DEPENDS impalad)
ADD_CUSTOM_TARGET(statestored ALL DEPENDS ${STATESTORED_SYMLINK})
ADD_CUSTOM_TARGET(catalogd ALL DEPENDS ${CATALOGD_SYMLINK})
ADD_CUSTOM_TARGET(admissiond ALL DEPENDS ${ADMISSIOND_SYMLINK})

ADD_CUSTOM_TARGET(daemons DEPENDS impalad statestored catalogd admissiond fesupport)

# This permits libraries loaded by dlopen to link to the symbols in the program.
set_target_properties(impalad PROPERTIES LINK_FLAGS -rdynamic)

target_link_libraries(impalad
  ${JAVA_JSIG_LIBRARY}
  ${IMPALA_LINK_LIBS}
)

install(FILES ${STATESTORED_SYMLINK} DESTINATION ${IMPALA_INSTALLDIR}/sbin)
install(FILES ${CATALOGD_SYMLINK} DESTINATION ${IMPALA_INSTALLDIR}/sbin)
install(FILES ${ADMISSIOND_SYMLINK} DESTINATION ${IMPALA_INSTALLDIR}/sbin)
install(TARGETS impalad DESTINATION ${IMPALA_INSTALLDIR}/sbin)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_executable(unifiedbetests
  unified-betest-main.cc
)

target_link_libraries(unifiedbetests
  ${JAVA_JSIG_LIBRARY} ${UNIFIED_TEST_LINK_LIBS})
ADD_DEPENDENCIES(unified-be-test-executable unifiedbetests)

add_library(ServiceTests STATIC
  hs2-util-test.cc
  impala-server-test.cc
  query-options-test.cc
  query-state-record-test.cc
)
add_dependencies(ServiceTests gen-deps)

# Exception to unified be tests: Custom main() due to leak
ADD_BE_TEST(session-expiry-test session-expiry-test.cc) # TODO: this leaks thrift server
ADD_UNIFIED_BE_LSAN_TEST(hs2-util-test "StitchNullsTest.*:PrintTColumnValueTest.*")
ADD_UNIFIED_BE_LSAN_TEST(query-options-test QueryOptions.*)
ADD_UNIFIED_BE_LSAN_TEST(impala-server-test ImpalaServerTest.*)
ADD_UNIFIED_BE_LSAN_TEST(query-state-record-test QueryStateRecordTest.*)
ADD_BE_LSAN_TEST(internal-server-test)
