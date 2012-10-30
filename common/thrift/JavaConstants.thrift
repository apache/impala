// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Centralised store of constants that are shared between C++ and Java
// - Thrift compiles all constants into Constants.java, which means it
// gets overwritten if two separate files have constants.
// This is fixed in Thrift 0.9 - see THRIFT-1090

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Descriptors.thrift"
include "ImpalaService.thrift" // For TImpalaQueryOptions

// constants for TQueryOptions.num_nodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// constants for TPlanNodeId
const i32 INVALID_PLAN_NODE_ID = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;

// Mapping from names defined by Trevni to the enum.
// We permit gzip and bzip2 in addtion.
const map<string, Descriptors.THdfsCompression> COMPRESSION_MAP = {
  "": Descriptors.THdfsCompression.NONE,
  "none": Descriptors.THdfsCompression.NONE,
  "deflate": Descriptors.THdfsCompression.DEFAULT,
  "gzip": Descriptors.THdfsCompression.GZIP,
  "bzip2": Descriptors.THdfsCompression.BZIP2,
  "snappy": Descriptors.THdfsCompression.SNAPPY
}

// Default values for each query option in ImpalaService.TImpalaQueryOptions
const map<ImpalaService.TImpalaQueryOptions, string> DEFAULT_QUERY_OPTIONS = {
  ImpalaService.TImpalaQueryOptions.ABORT_ON_ERROR : "false",
  ImpalaService.TImpalaQueryOptions.MAX_ERRORS : "0",
  ImpalaService.TImpalaQueryOptions.DISABLE_CODEGEN : "false",
  ImpalaService.TImpalaQueryOptions.BATCH_SIZE : "0",
  ImpalaService.TImpalaQueryOptions.NUM_NODES : "0",
  ImpalaService.TImpalaQueryOptions.MAX_SCAN_RANGE_LENGTH : "0",
  ImpalaService.TImpalaQueryOptions.MAX_IO_BUFFERS : "0"
  ImpalaService.TImpalaQueryOptions.NUM_SCANNER_THREADS : "0"
  ImpalaService.TImpalaQueryOptions.PARTITION_AGG : "false"
  ImpalaService.TImpalaQueryOptions.ALLOW_UNSUPPORTED_FORMATS : "false"
}
