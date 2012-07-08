// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

// Centralised store of constants that are shared between C++ and Java
// - Thrift compiles all constants into Constants.java, which means it
// gets overwritten if two separate files have constants.
// This is fixed in Thrift 0.9 - see THRIFT-1090

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Descriptors.thrift"

// constants for TQueryRequest.numNodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

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

