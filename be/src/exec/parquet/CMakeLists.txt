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

cmake_minimum_required(VERSION 2.6)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/parquet")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/parquet")

add_library(Parquet
  hdfs-parquet-scanner.cc
  hdfs-parquet-scanner-ir.cc
  hdfs-parquet-table-writer.cc
  parquet-bool-decoder.cc
  parquet-collection-column-reader.cc
  parquet-column-readers.cc
  parquet-column-stats.cc
  parquet-level-decoder.cc
  parquet-metadata-utils.cc
  parquet-common.cc
  parquet-page-index.cc
)

add_dependencies(Parquet gen-deps)

ADD_BE_LSAN_TEST(parquet-bool-decoder-test)
ADD_BE_LSAN_TEST(parquet-common-test)
ADD_BE_LSAN_TEST(parquet-page-index-test)
ADD_BE_LSAN_TEST(parquet-plain-test)
ADD_BE_LSAN_TEST(parquet-version-test)
ADD_BE_LSAN_TEST(hdfs-parquet-scanner-test)
