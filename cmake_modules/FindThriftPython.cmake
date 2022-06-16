##############################################################################
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
##############################################################################

# - Find Thrift (a cross platform RPC lib/tool)
# THRIFT_PY_ROOT hints the location
#
# This module defines
#  THRIFT_PY_CONTRIB_DIR, where contrib thrift files (e.g. fb303.thrift) are installed
#  THRIFT_PY_COMPILER, where to find thrift compiler

# prefer the thrift version supplied in THRIFT_PY_HOME
if (NOT THRIFT_PY_FIND_QUIETLY)
  message(STATUS "THRIFT_PY_HOME: $ENV{THRIFT_PY_HOME}")
endif()

find_path(THRIFT_PY_CONTRIB_DIR share/fb303/if/fb303.thrift HINTS
  ${THRIFT_PY_ROOT}/include
  $ENV{THRIFT_PY_HOME}
  /usr/local/
)

find_program(THRIFT_PY_COMPILER thrift
  ${THRIFT_PY_ROOT}/bin
  $ENV{THRIFT_PY_HOME}/bin
  /usr/local/bin
  /usr/bin
  NO_DEFAULT_PATH
)


mark_as_advanced(
  THRIFT_PY_COMPILER
)
