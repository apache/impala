// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

typedef i64 TTimestamp
typedef i32 TPlanNodeId
typedef i32 TTupleId
typedef i32 TSlotId
typedef i32 TTableId

enum TPrimitiveType {
  INVALID_TYPE,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  DATE,
  DATETIME,
  TIMESTAMP,
  STRING
}

// Wire format for UniqueId
struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}

enum TStatusCode {
  OK,
  ANALYSIS_ERROR,
  NOT_IMPLEMENTED_ERROR,
  RUNTIME_ERROR,
  INTERNAL_ERROR
}

struct TStatus {
  1: required TStatusCode status_code
  2: list<string> error_msgs
}

