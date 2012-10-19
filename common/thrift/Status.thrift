// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

enum TStatusCode {
  OK,
  CANCELLED,
  ANALYSIS_ERROR,
  NOT_IMPLEMENTED_ERROR,
  RUNTIME_ERROR,
  INTERNAL_ERROR
}

struct TStatus {
  1: required TStatusCode status_code
  2: list<string> error_msgs
}
