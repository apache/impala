// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

typedef i64 TTimestamp

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
