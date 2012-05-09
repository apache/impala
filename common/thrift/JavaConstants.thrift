// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

// Centralised store of constants that are shared between C++ and Java
// - Thrift compiles all constants into Constants.java, which means it
// gets overwritten if two separate files have constants.
// This is fixed in Thrift 0.9 - see THRIFT-1090

namespace cpp impala
namespace java com.cloudera.impala.thrift

// constants for TQueryRequest.numNodes
const i32 NUM_NODES_ALL = 0
const i32 NUM_NODES_ALL_RACKS = -1

// Constant default partition ID, must be < 0 to avoid collisions
const i64 DEFAULT_PARTITION_ID = -1;
