// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

// Counter data types.
enum TCounterType {
  UNIT,
  UNIT_PER_SECOND,
  TIME_MS,
  CPU_TICKS,
  BYTES
  BYTES_PER_SECOND,
}

// Counter data
struct TCounter {
  1: required string name
  2: required TCounterType type
  3: required i64 value 
}

// A single runtime profile
struct TRuntimeProfileNode {
  1: required string name
  2: required i32 num_children 
  3: required list<TCounter> counters
  // TODO: should we make metadata a serializable struct?  We only use it to
  // store the node id right now so this is sufficient.
  4: required i64 metadata

  // indicates whether the child will be printed with extra indentation;
  // corresponds to indent param of RuntimeProfile::AddChild()
  5: required bool indent
}

// A flattened tree of runtime profiles, obtained by an
// in-order traversal
struct TRuntimeProfileTree {
  1: required list<TRuntimeProfileNode> nodes
}
