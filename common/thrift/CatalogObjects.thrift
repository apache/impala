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

// Types used to represent catalog objects.

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
  DATA_SOURCE,
  ROLE,
  PRIVILEGE,
  HDFS_CACHE_POOL,
}

enum TTableType {
  HDFS_TABLE,
  HBASE_TABLE,
  VIEW,
  DATA_SOURCE_TABLE
}

enum THdfsFileFormat {
  TEXT,
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
  SNAPPY_BLOCKED,
  LZO,
  LZ4
}

enum THdfsSeqCompressionMode {
  RECORD,
  BLOCK
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

// Represents a fully qualified table name.
struct TTableName {
  // Name of the table's parent database.
  1: required string db_name

  // Name of the table
  2: required string table_name
}

struct TTableStats {
  // Estimated number of rows in the table or -1 if unknown
  1: required i64 num_rows;
}

// Column stats data that Impala uses.
struct TColumnStats {
  // Average size and max size, in bytes. Excludes serialization overhead.
  // For fixed-length types (those which don't need additional storage besides the slot
  // they occupy), sets avg_size and max_size to their slot size.
  1: required double avg_size
  2: required i64 max_size

  // Estimated number of distinct values.
  3: required i64 num_distinct_values

  // Estimated number of null values.
  4: required i64 num_nulls
}

// Intermediate state for the computation of per-column stats. Impala can aggregate these
// structures together to produce final stats for a column.
struct TIntermediateColumnStats {
  // One byte for each bucket of the NDV HLL computation
  1: optional binary intermediate_ndv

  // If true, intermediate_ndv is RLE-compressed
  2: optional bool is_ndv_encoded

  // Number of nulls seen so far (or -1 if nulls are not counted)
  3: optional i64 num_nulls

  // The maximum width, in bytes, of the column
  4: optional i32 max_width

  // The average width (in bytes) of the column
  5: optional double avg_width

  // The number of rows counted, needed to compute NDVs from intermediate_ndv
  6: optional i64 num_rows
}

// Per-partition statistics
struct TPartitionStats {
  // Number of rows gathered per-partition by non-incremental stats.
  // TODO: This can probably be removed in favour of the intermediate_col_stats, but doing
  // so would interfere with the non-incremental stats path
  1: required TTableStats stats

  // Intermediate state for incremental statistics, one entry per column name.
  2: optional map<string, TIntermediateColumnStats> intermediate_col_stats
}

struct TColumn {
  1: required string columnName
  2: required Types.TColumnType columnType
  3: optional string comment
  // Stats for this table, if any are available.
  4: optional TColumnStats col_stats
  // Ordinal position in the source table
  5: optional i32 position

  // Indicates whether this is an HBase column. If true, implies
  // all following HBase-specific fields are set.
  6: optional bool is_hbase_column
  7: optional string column_family
  8: optional string column_qualifier
  9: optional bool is_binary
}

// Represents a block in an HDFS file
struct THdfsFileBlock {
  // Offset of this block within the file
  1: required i64 offset

  // Total length of the block
  2: required i64 length

  // Hosts that contain replicas of this block. Each value in the list is an index in to
  // the network_addresses list of THdfsTable.
  3: required list<i32> replica_host_idxs

  // The list of disk ids for the file block. May not be set if disk ids are not supported
  4: optional list<i32> disk_ids

  // For each replica, specifies if the block is cached in memory.
  5: optional list<bool> is_replica_cached
}

// Represents an HDFS file in a partition.
struct THdfsFileDesc {
  // The name of the file (not the full path). The parent path is assumed to be the
  // 'location' of the THdfsPartition this file resides within.
  1: required string file_name

  // The total length of the file, in bytes.
  2: required i64 length

  // The type of compression used for this file.
  3: required THdfsCompression compression

  // The last modified time of the file.
  4: required i64 last_modification_time

  // List of THdfsFileBlocks that make up this file.
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
  9: optional list<THdfsFileDesc> file_desc
  10: optional string location

  // The access level Impala has on this partition (READ_WRITE, READ_ONLY, etc).
  11: optional TAccessLevel access_level

  // Statistics on this partition, e.g., number of rows in this partition.
  12: optional TTableStats stats

  // True if this partition has been marked as cached (does not necessarily mean the
  // underlying data is cached).
  13: optional bool is_marked_cached

  // Unique (in this table) id of this partition. If -1, the partition does not currently
  // exist.
  14: optional i64 id

  // (key,value) pairs stored in the Hive Metastore.
  15: optional map<string, string> hms_parameters
}

struct THdfsTable {
  1: required string hdfsBaseDir

  // Deprecated. Use TTableDescriptor.colNames.
  2: required list<string> colNames;

  // The string used to represent NULL partition keys.
  3: required string nullPartitionKeyValue

  // String to indicate a NULL column value in text files
  5: required string nullColumnValue

  // Set to the table's Avro schema if this is an Avro table
  6: optional string avroSchema

  // map from partition id to partition metadata
  4: required map<i64, THdfsPartition> partitions

  // Each TNetworkAddress is a datanode which contains blocks of a file in the table.
  // Used so that each THdfsFileBlock can just reference an index in this list rather
  // than duplicate the list of network address, which helps reduce memory usage.
  7: optional list<Types.TNetworkAddress> network_addresses

  // Indicates that this table's partitions reside on more than one filesystem.
  // TODO: remove once INSERT across filesystems is supported.
  8: optional bool multiple_filesystems
}

struct THBaseTable {
  1: required string tableName
  2: required list<string> families
  3: required list<string> qualifiers

  // Column i is binary encoded if binary_encoded[i] is true. Otherwise, column i is
  // text encoded.
  4: optional list<bool> binary_encoded
}

// Represents an external data source
struct TDataSource {
  // Name of the data source
  1: required string name

  // HDFS URI of the library
  2: required string hdfs_location

  // Class name of the data source implementing the ExternalDataSource interface.
  3: required string class_name

  // Version of the ExternalDataSource interface. Currently only 'V1' exists.
  4: required string api_version
}

// Represents a table scanned by an external data source.
struct TDataSourceTable {
  // The data source that will scan this table.
  1: required TDataSource data_source

  // Init string for the table passed to the data source. May be an empty string.
  2: required string init_string
}

// Represents a table or view.
struct TTable {
  // Name of the parent database. Case insensitive, expected to be stored as lowercase.
  1: required string db_name

  // Unqualified table name. Case insensitive, expected to be stored as lowercase.
  2: required string tbl_name

  // Set if there were any errors loading the Table metadata. The remaining fields in
  // the struct may not be set if there were problems loading the table metadata.
  // By convention, the final error message in the Status should contain the call stack
  // string pointing to where the metadata loading error occurred.
  3: optional Status.TStatus load_status

  // Table identifier.
  4: optional Types.TTableId id

  // The access level Impala has on this table (READ_WRITE, READ_ONLY, etc).
  5: optional TAccessLevel access_level

  // List of columns (excludes clustering columns)
  6: optional list<TColumn> columns

  // List of clustering columns (empty list if table has no clustering columns)
  7: optional list<TColumn> clustering_columns

  // Table stats data for the table.
  8: optional TTableStats table_stats

  // Determines the table type - either HDFS, HBASE, or VIEW.
  9: optional TTableType table_type

  // Set iff this is an HDFS table
  10: optional THdfsTable hdfs_table

  // Set iff this is an Hbase table
  11: optional THBaseTable hbase_table

  // The Hive Metastore representation of this table. May not be set if there were
  // errors loading the table metadata
  12: optional hive_metastore.Table metastore_table

  // Set iff this is a table from an external data source
  13: optional TDataSourceTable data_source_table
}

// Represents a database.
struct TDatabase {
  // Name of the database. Case insensitive, expected to be stored as lowercase.
  1: required string db_name

  // The HDFS location new tables will default their base directory to
  2: optional string location
}

// Represents a role in an authorization policy.
struct TRole {
  // Case-insensitive role name
  1: required string role_name

  // Unique ID of this role, generated by the Catalog Server.
  2: required i32 role_id

  // List of groups this role has been granted to (group names are case sensitive).
  // TODO: Keep a list of grant groups globally (in TCatalog?) and reference by ID since
  // the same groups will likely be shared across multiple roles.
  3: required list<string> grant_groups
}

// The scope a TPrivilege applies to.
enum TPrivilegeScope {
  SERVER,
  URI,
  DATABASE,
  TABLE,
  COLUMN,
}

// The privilege level allowed.
enum TPrivilegeLevel {
  ALL,
  INSERT,
  SELECT
}

// Represents a privilege in an authorization policy. Privileges contain the level
// of access, the scope and role the privilege applies to, and details on what
// catalog object the privilege is securing. Objects are hierarchical, so a privilege
// corresponding to a table must also specify all the parent objects (database name
// and server name).
struct TPrivilege {
  // A human readable name for this privilege. The combination of role_id +
  // privilege_name is guaranteed to be unique. Stored in a form that can be passed
  // to Sentry: [ServerName]->[DbName]->[TableName]->[ColumnName]->[Action Granted].
  1: required string privilege_name

  // The level of access this privilege provides.
  2: required TPrivilegeLevel privilege_level

  // The scope of the privilege: SERVER, DATABASE, URI, TABLE or COLUMN
  3: required TPrivilegeScope scope

  // If true, GRANT OPTION was specified. For a GRANT privilege statement, everyone
  // granted this role should be able to issue GRANT/REVOKE privilege statements even if
  // they are not an admin. For REVOKE privilege statements, the privilege should be
  // retainined and the existing GRANT OPTION (if it was set) on the privilege should be
  // removed.
  4: required bool has_grant_opt

  // The ID of the role this privilege belongs to.
  5: optional i32 role_id

  // Set if scope is SERVER, URI, DATABASE, or TABLE
  6: optional string server_name

  // Set if scope is DATABASE or TABLE
  7: optional string db_name

  // Unqualified table name. Set if scope is TABLE.
  8: optional string table_name

  // Set if scope is URI
  9: optional string uri

  // Time this privilege was created (in milliseconds since epoch).
  10: optional i64 create_time_ms

  // Set if scope is COLUMN
  11: optional string column_name
}

// Thrift representation of an HdfsCachePool.
struct THdfsCachePool {
  // Name of the cache pool
  1: required string pool_name

  // In the future we may want to include additional info on the pool such as
  // the pool limits, pool owner, etc.
}

// Represents state associated with the overall catalog.
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
  6: optional Types.TFunction fn

  // Set iff object type is DATA SOURCE
  7: optional TDataSource data_source

  // Set iff object type is ROLE
  8: optional TRole role

  // Set iff object type is PRIVILEGE
  9: optional TPrivilege privilege

  // Set iff object type is HDFS_CACHE_POOL
  10: optional THdfsCachePool cache_pool
}
