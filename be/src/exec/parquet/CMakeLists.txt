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
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/parquet")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exec/parquet")

add_library(ExecParquet
  hdfs-parquet-scanner.cc
  hdfs-parquet-table-writer.cc
  parquet-bloom-filter-util.cc
  parquet-bool-decoder.cc
  parquet-collection-column-reader.cc
  parquet-column-readers.cc
  parquet-column-stats.cc
  parquet-complex-column-reader.cc
  parquet-delta-decoder.cc
  parquet-delta-encoder.cc
  parquet-level-decoder.cc
  parquet-metadata-utils.cc
  parquet-column-chunk-reader.cc
  parquet-page-reader.cc
  parquet-common.cc
  parquet-page-index.cc
  parquet-struct-column-reader.cc
)

add_dependencies(ExecParquet gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_library(ExecParquetTests STATIC
  hdfs-parquet-scanner-test.cc
  parquet-bool-decoder-test.cc
  parquet-common-test.cc
  parquet-delta-coder-test.cc
  parquet-page-index-test.cc
  parquet-plain-test.cc
  parquet-version-test.cc
  serialize-single-value-test.cc
)
add_dependencies(ExecParquetTests gen-deps)

ADD_UNIFIED_BE_LSAN_TEST(parquet-bool-decoder-test ParquetBoolDecoder.*)
ADD_UNIFIED_BE_LSAN_TEST(parquet-common-test ParquetCommon.*)
ADD_UNIFIED_BE_LSAN_TEST(parquet-delta-coder-test "ParquetDeltaEncoder.*:ParquetDeltaDecoder.*:ParquetDeltaCoder.*")
ADD_UNIFIED_BE_LSAN_TEST(parquet-page-index-test ParquetPageIndex.*)
ADD_UNIFIED_BE_LSAN_TEST(parquet-plain-test PlainEncoding.*)
ADD_UNIFIED_BE_LSAN_TEST(parquet-version-test ParquetVersionTest.*)
ADD_UNIFIED_BE_LSAN_TEST(hdfs-parquet-scanner-test HdfsParquetScannerTest.*)
ADD_UNIFIED_BE_LSAN_TEST(serialize-single-value-test SerializeSingleValueTest.*)
