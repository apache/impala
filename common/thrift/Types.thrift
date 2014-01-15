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

namespace cpp impala
namespace java com.cloudera.impala.thrift

typedef i64 TTimestamp
typedef i32 TPlanNodeId
typedef i32 TTupleId
typedef i32 TSlotId
typedef i32 TTableId

enum TPrimitiveType {
  INVALID_TYPE,
  NULL_TYPE,
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
  STRING,
  // Unsupported types
  BINARY,
  DECIMAL,
  // CHAR(n). Currently only supported in UDAs
  CHAR,
}

struct TColumnType {
  1: required TPrimitiveType type

  // Only set if type == CHAR_ARRAY
  2: optional i32 len
}

enum TStmtType {
  QUERY,
  DDL, // Data definition, e.g. CREATE TABLE (includes read-only functions e.g. SHOW)
  DML, // Data modification e.g. INSERT
  EXPLAIN,
  LOAD // Statement type for LOAD commands
}

// Level of verboseness for "explain" output.
enum TExplainLevel {
  MINIMAL,
  STANDARD,
  EXTENDED,
  VERBOSE
}

// A TNetworkAddress is the standard host, port representation of a
// network address. The hostname field must be resolvable to an IPv4
// address.
struct TNetworkAddress {
  1: required string hostname
  2: required i32 port
}

// Wire format for UniqueId
struct TUniqueId {
  1: required i64 hi
  2: required i64 lo
}

enum TFunctionType {
  SCALAR,
  AGGREGATE,
}

enum TFunctionBinaryType {
  // Impala builtin. We can either run this interpreted or via codegen
  // depending on the query option.
  BUILTIN,

  // Hive UDFs, loaded from *.jar
  HIVE,

  // Native-interface, precompiled UDFs loaded from *.so
  NATIVE,

  // Native-interface, precompiled to IR; loaded from *.ll
  IR,
}

// Represents a fully qualified function name.
struct TFunctionName {
  // Name of the function's parent database. Not set if in global
  // namespace (e.g. builtins)
  1: optional string db_name

  // Name of the function
  2: required string function_name
}

struct TScalarFunction {
  // Symbol for the function
  1: optional string symbol;
}

struct TAggregateFunction {
  1: required TColumnType intermediate_type
  2: optional string update_fn_symbol
  3: optional string init_fn_symbol
  4: optional string serialize_fn_symbol
  5: optional string merge_fn_symbol
  6: optional string finalize_fn_symbol
}

// Represents a function in the Catalog.
struct TFunction {
  // Id that is unique across all functions.
  1: required i64 id

  // Fully qualified function name.
  2: required TFunctionName name

  // Type of the udf. e.g. hive, native, ir
  3: required TFunctionBinaryType binary_type

  // The types of the arguments to the function
  4: required list<TColumnType> arg_types

  // Return type for the function.
  5: required TColumnType ret_type

  // If true, this function takes var args.
  6: required bool has_var_args

  // Optional comment to attach to the function
  7: optional string comment

  8: optional string signature

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  9: optional string hdfs_location

  // One of these should be set.
  10: optional TScalarFunction scalar_fn
  11: optional TAggregateFunction aggregate_fn
}

