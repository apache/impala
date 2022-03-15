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
#
# - Find libunwind (libunwind.h, libunwind.so)
#
# This module defines
#  LIBUNWIND_INCLUDE_DIR, directory containing headers
#  LIBUNWIND_SHARED_LIB, path to libunwind's shared library
#  LIBUNWIND_STATIC_LIB, path to libunwind's static library

find_path(LIBUNWIND_INCLUDE_DIR libunwind.h
  ${LIBUNWIND_ROOT}/include
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(LIBUNWIND_SHARED_LIB unwind
  ${LIBUNWIND_ROOT}/lib
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)
find_library(LIBUNWIND_STATIC_LIB libunwind.a
  ${LIBUNWIND_ROOT}/lib
  NO_CMAKE_SYSTEM_PATH
  NO_SYSTEM_ENVIRONMENT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibUnwind REQUIRED_VARS
  LIBUNWIND_SHARED_LIB LIBUNWIND_STATIC_LIB LIBUNWIND_INCLUDE_DIR)
