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

include "Exprs.thrift"
include "Status.thrift"
include "Types.thrift"
include "hive_metastore.thrift"

// Type of Catalog object.
enum TCatalogObjectType {
  // UNKNOWN is used to indicate an error condition when converting
  // strings to their matching TCatalogObjectType.
  UNKNOWN,
  CATALOG,
  DATABASE,
  TABLE,
  VIEW,
  FUNCTION,
}

enum TTableType {
  HDFS_TABLE,
  HBASE_TABLE
}

// Valid table file formats
// TODO: Combine this an THdfsFileFormat once we are able to create LZO_TEXT files
// in Impala.
enum TFileFormat {
  PARQUETFILE,
  RCFILE,
  SEQUENCEFILE,
  TEXTFILE,
  AVROFILE,
}

enum THdfsFileFormat {
  TEXT,
  LZO_TEXT,
  RC_FILE,
  SEQUENCE_FILE,
  AVRO,
  PARQUET
}

enum THdfsCompression {
  NONE,
  DEFAULT,
  GZIP,
  DEFLATE,
  BZIP2,
  SNAPPY,
  SNAPPY_BLOCKED, // Used by sequence and rc files but not stored in the metadata.
  LZO
}

// The table property type.
enum TTablePropertyType {
  TBL_PROPERTY,
  SERDE_PROPERTY
}

// The access level that is available to Impala on the Catalog object.
enum TAccessLevel {
  NONE,
  READ_WRITE,
  READ_ONLY,
  WRITE_ONLY,
}

// Mapping from names defined by Avro to values in the THdfsCompression enum.
const map<string, THdfsCompression> COMPRESSION_MAP = {
  "": THdfsCompression.NONE,
  "none": THdfsCompression.NONE,
  "deflate": THdfsCompression.DEFAULT,
  "gzip": THdfsCompression.GZIP,
  "bzip2": THdfsCompression.BZIP2,
  "snappy": THdfsCompression.SNAPPY
}

// Represents a single item in a partition spec (column name + value)
struct TPartitionKeyValue {
  // Partition column name
  1: required string name,

  // Partition value
  2: required string value
}

// Represents a fully qualified function name.
struct TFunctionName {
  // Name of the function's parent database.
  1: required string db_name

  // Name of the function
  2: required string function_name
}

// Represents a fully qualified table name.
struct TTableName {
  // Name of the table's parent database.
  1: required string db_name

  // Name of the table
  2: required string table_name
}

struct TColumnDesc {
  1: required string columnName
  2: required Types.TPrimitiveType columnType
}

// A column definition; used by CREATE TABLE and DESCRIBE <table> statements. A column
// definition has a different meaning (and additional fields) from a column descriptor,
// so this is a separate struct from TColumnDesc.
struct TColumnDef {
  1: required TColumnDesc columnDesc
  2: optional string comment
}

struct TTableStatsData {
  // Estimated number of rows in the table or -1 if unknown
  1: required i64 num_rows;
}

// Column stats data that Impala uses.
struct TColumnStatsData {
  // Average serialized size and max size, in bytes. Includes serialization overhead.
  // For fixed-length types (those which don't need additional storage besides the slot
  // they occupy), sets avg_serialized_size and max_size to their slot size.
  1: required double avg_serialized_size
  2: required i64 max_size

  // Estimated number of distinct values.
  3: required i64 num_distinct_values

  // Estimated number of null values.
  4: required i64 num_nulls
}

// Represents a block in an HDFS file
struct THdfsFileBlock {
  // Name of the file
  1: required string file_name

  // Size of the file
  2: required i64 file_size

  // Offset of this block within the file
  3: required i64 offset

  // Total length of the block
  4: required i64 length

  // List of datanodes that contain this block
  5: required list<string> host_ports

  // The list of disk ids for the file block. May not be set if disk ids are not supported
  6: optional list<i32> disk_ids
}

// Represents an HDFS file
struct THdfsFileDesc {
  1: required string path
  2: required i64 length
  3: required THdfsCompression compression
  4: required i64 last_modification_time
  5: required list<THdfsFileBlock> file_blocks
}

// Represents an HDFS partition
struct THdfsPartition {
  1: required byte lineDelim
  2: required byte fieldDelim
  3: required byte collectionDelim
  4: required byte mapKeyDelim
  5: required byte escapeChar
  6: required THdfsFileFormat fileFormat
  7: list<Exprs.TExpr> partitionKeyExprs
  8: required i32 blockSize
  9: required THdfsCompression compression
  10: optional list<THdfsFileDesc> file_desc
  11: optional string location

  // The access level Impala has on this partition (READ_WRITE, READ_ONLY, etc).
  12: optional TAccessLevel access_level
}

struct THdfsTable {
  1: required string hdfsBaseDir

  // Names of the columns, including clustering columns.  As in other
  // places, the clustering columns come before the non-clustering
  // columns.  This includes non-materialized columns.
  2: required list<string> colNames;

  // Partition keys are the same as clustering columns in
  // TTableDescriptor, so there should be an equal number of each.
  3: required string nullPartitionKeyValue

  // String to indicate a NULL column value in text files
  5: required string nullColumnValue

  // Set to the table's Avro schema if this is an Avro table
  6: optional string avroSchema

  // map from partition id to partition metadata
  4: required map<i64, THdfsPartition> partitions
}

struct THBaseTable {
  1: required string tableName
  2: required list<string> families
  3: required list<string> qualifiers

  // Column i is binary encoded if binary_encoded[i] is true. Otherwise, column i is
  // text encoded.
  4: optional list<bool> binary_encoded
}

// Represents a table, and the metadata assiciated with it, in the Catalog
struct TTable {
  // Name of the parent database
  1: required string db_name

  // Unqualified table name
  2: required string tbl_name

  // The following fields may not be set if there were problems loading the table
  // metadata.
  3: optional Types.TTableId id

  // The access level Impala has on this table (READ_WRITE, READ_ONLY, etc).
  4: optional TAccessLevel access_level

  // List of columns (excludes partition columns)
  5: optional list<TColumnDef> columns

  // List of partition columns (empty list if table is not partitioned)
  6: optional list<TColumnDef> partition_columns

  // Table stats data for the table.
  7: optional TTableStatsData table_stats

  // Column stats for the table. May not be set if there were errors loading the
  // table metadata or if the table did not contain any column stats data.
  8: optional map<string, TColumnStatsData> column_stats

  // Set if there were any errors loading the Table metadata.
  9: optional Status.TStatus load_status

  // Determines whether this is an HDFS or HBASE table.
  10: optional TTableType table_type

  // Set iff this is an HDFS table
  11: optional THdfsTable hdfs_table

  // Set iff this is an Hbase table
  12: optional THBaseTable hbase_table

  // The Hive Metastore representation of this table. May not be set if there were
  // errors loading the table metadata
  13: optional hive_metastore.Table metastore_table
}

// Represents a database, and the metadata associated with it, in the Catalog
struct TDatabase {
  // Name of the database
  1: required string db_name

  // The HDFS location new tables will default their base directory to
  2: optional string location
}

struct TUdf {
  // Name of function in the binary
  1: required string symbol_name;
}

struct TUda {
  1: required string update_fn_name
  2: required string init_fn_name
  // This function does not need to be specified by the UDA.
  3: optional string serialize_fn_name
  4: required string merge_fn_name
  5: optional string finalize_fn_name
  6: required Types.TColumnType intermediate_type
}

// Represents a function in the Catalog.
struct TFunction {
  // Fully qualified function name of the function to create
  1: required TFunctionName fn_name

  // Type of the udf. e.g. hive, native, ir
  2: required Types.TFunctionBinaryType fn_binary_type

  // HDFS path for the function binary. This binary must exist at the time the
  // function is created.
  3: required string location

  // The types of the arguments to the function
  4: required list<Types.TPrimitiveType> arg_types

  // Return type for the function.
  5: required Types.TPrimitiveType ret_type

  // If true, this function takes var args.
  6: required bool has_var_args

  // Optional comment to attach to the function
  7: optional string comment

  8: optional string signature

  // Only one of the below is set.
  9: optional TUdf udf
  10: optional TUda uda
}

struct TCatalog {
  // The CatalogService service ID.
  1: required Types.TUniqueId catalog_service_id
}

// Union of all Thrift Catalog objects
struct TCatalogObject {
  // The object type (Database, Table, View, or Function)
  1: required TCatalogObjectType type

  // The Catalog version this object is from
  2: required i64 catalog_version

  // Set iff object type is CATALOG
  3: optional TCatalog catalog

  // Set iff object type is DATABASE
  4: optional TDatabase db

  // Set iff object type is TABLE or VIEW
  5: optional TTable table

  // Set iff object type is FUNCTION
  6: optional TFunction fn
}
