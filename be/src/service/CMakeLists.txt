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

# where to put generated libraries.
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# where to put generated binaries.
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

add_library(Service
  frontend.cc
  fe-support.cc
  fragment-exec-state.cc
  fragment-mgr.cc
  hs2-util.cc
  impala-server.cc
  impala-server-callbacks.cc
  impala-hs2-server.cc
  impala-beeswax-server.cc
  query-exec-state.cc
  query-options.cc
  child-query.cc
  impalad-main.cc
)
add_dependencies(Service thrift-deps)

# this shared library provides Impala executor functionality to FE test.
add_library(fesupport SHARED
  fe-support.cc
)
add_dependencies(fesupport thrift-deps)

target_link_libraries(fesupport ${IMPALA_LINK_LIBS_DYNAMIC_TARGETS})

add_executable(impalad
  daemon-main.cc
)

# All Impala daemons run from the same binary. The code that is run is determined by the
# name (i.e. argv[0]) of the command that executes the binary, so we create symlinks for
# statestored and catalogd.
ADD_CUSTOM_TARGET(statestored
  ${CMAKE_COMMAND} -E create_symlink ${EXECUTABLE_OUTPUT_PATH}/impalad
  ${BUILD_OUTPUT_ROOT_DIRECTORY}/statestore/statestored DEPENDS impalad)

ADD_CUSTOM_TARGET(catalogd
  ${CMAKE_COMMAND} -E create_symlink ${EXECUTABLE_OUTPUT_PATH}/impalad
  ${BUILD_OUTPUT_ROOT_DIRECTORY}/catalog/catalogd DEPENDS impalad)

# This permits libraries loaded by dlopen to link to the symbols in the program.
set_target_properties(impalad PROPERTIES LINK_FLAGS -rdynamic)

target_link_libraries(impalad
  ${JAVA_JSIG_LIBRARY}
  ${IMPALA_LINK_LIBS}
)

ADD_BE_TEST(session-expiry-test session-expiry-test.cc)
ADD_BE_TEST(hs2-util-test hs2-util-test.cc)
