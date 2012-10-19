// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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

enum TStmtType {
  QUERY,
  DDL, // Data definition, e.g. CREATE TABLE (includes read-only functions e.g. SHOW)
  DML // Data modification e.g. INSERT
}

// level of verboseness for "explain" output
// TODO: should this go somewhere else?
enum TExplainLevel {
  NORMAL,
  VERBOSE
}

// A THostPort represents a general network address. It includes both
// hostname and IP address fields in order to support use cases that may
// require either one or both these fields set. An example is those
// network addresses stored by the state-store which require IP addresses
// in order for the scheduler to correctly assign data locations to
// Impala backends but also fully qualified hostnames to be able to
// establish secure connections with subscribers. Which of the fields are
// set in general is usage specific.
struct THostPort {
  1: required string hostname
  2: required string ipaddress
  3: required i32 port
}

// Wire format for UniqueId
struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}
