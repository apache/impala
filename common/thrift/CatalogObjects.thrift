// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

namespace cpp impala
namespace java org.apache.impala.thrift

include "Exprs.thrift"
include "Status.thrift"
include "Types.thrift"
include "hive_metastore.thrift"

// Types used to represent catalog objects.

// Type of Catalog object.
enum TCatalogObjectType {
  // UNKNOWN is used to indicate an error condition when converting
  // strings to their matching TCatalogObjectType.
  UNKNOWN = 0
  CATALOG = 1
  DATABASE = 2
  TABLE = 3
  VIEW = 4
  FUNCTION = 5
  DATA_SOURCE = 6
  PRINCIPAL = 7
  PRIVILEGE = 8
  HDFS_CACHE_POOL = 9
  // A catalog object type as a marker for authorization cache invalidation.
  AUTHZ_CACHE_INVALIDATION = 10
}

enum TTableType {
  HDFS_TABLE = 0
  HBASE_TABLE = 1
  VIEW = 2
  DATA_SOURCE_TABLE = 3
  KUDU_TABLE = 4
}

// TODO: Separate the storage engines (e.g. Kudu) from the file formats.
// TODO: Make the names consistent with the file format keywords specified in
// the parser.
enum THdfsFileFormat {
  TEXT = 0
  RC_FILE = 1
  SEQUENCE_FILE = 2
  AVRO = 3
  PARQUET = 4
  KUDU = 5
  ORC = 6
}

// TODO: Since compression is also enabled for Kudu columns, we should
// rename this enum to not be Hdfs specific.
enum THdfsCompression {
  NONE = 0
  DEFAULT = 1
  GZIP = 2
  DEFLATE = 3
  BZIP2 = 4
  SNAPPY = 5
  SNAPPY_BLOCKED = 6
  LZO = 7
  LZ4 = 8
  ZLIB = 9
  ZSTD = 10
}

enum TColumnEncoding {
  AUTO = 0
  PLAIN = 1
  PREFIX = 2
  GROUP_VARINT = 3
  RLE = 4
  DICTIONARY = 5
  BIT_SHUFFLE = 6
}

enum THdfsSeqCompressionMode {
  RECORD = 0
  BLOCK = 1
}

// The table property type.
enum TTablePropertyType {
  TBL_PROPERTY = 0
  SERDE_PROPERTY = 1
}

// The access level that is available to Impala on the Catalog object.
enum TAccessLevel {
  NONE = 0
  READ_WRITE = 1
  READ_ONLY = 2
  WRITE_ONLY = 3
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
  1: required i64 num_rows

  // Sum of file sizes in the table. Only set for tables of type HDFS_TABLE.
  2: optional i64 total_file_bytes
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
  // The column name, in lower case.
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

  // All the following are Kudu-specific column properties
  10: optional bool is_kudu_column
  11: optional bool is_key
  12: optional bool is_nullable
  13: optional TColumnEncoding encoding
  14: optional THdfsCompression compression
  15: optional Exprs.TExpr default_value
  16: optional i32 block_size
  // The column name, in the case that it appears in Kudu.
  17: optional string kudu_column_name
}

// Represents an HDFS file in a partition.
struct THdfsFileDesc {
  // File descriptor metadata serialized into a FlatBuffer
  // (defined in common/fbs/CatalogObjects.fbs).
  // TODO: Put this in a KRPC sidecar to avoid serialization cost.
  1: required binary file_desc_data
}

// Represents an HDFS partition's location in a compressed format. 'prefix_index'
// represents the portion of the partition's location that comes before the last N
// directories, where N is the number of partitioning columns. 'prefix_index' is an index
// into THdfsTable.partition_prefixes, or -1 if this location has not been compressed.
// 'suffix' is the rest of the partition location.
struct THdfsPartitionLocation {
  1: required i32 prefix_index = -1
  2: required string suffix
}

// Represents an HDFS partition
// TODO(vercegovac): rename to TFsPartition
struct THdfsPartition {

  // ============================================================
  // Fields included in the "Descriptor" format sent to the backend
  // as part of query plans and fragments.
  // ============================================================

  1: required byte lineDelim
  2: required byte fieldDelim
  3: required byte collectionDelim
  4: required byte mapKeyDelim
  5: required byte escapeChar
  6: required THdfsFileFormat fileFormat

  // These are Literal expressions
  7: list<Exprs.TExpr> partitionKeyExprs
  8: required i32 blockSize

  10: optional THdfsPartitionLocation location

  // Unique (in this table) id of this partition. May be set to
  // PROTOTYPE_PARTITION_ID when this object is used to describe
  // a partition which will be created as part of a query.
  14: optional i64 id


  // ============================================================
  // Fields only included when the catalogd serializes a table to be
  // sent to the impalad as part of a catalog update.
  // ============================================================

  9: optional list<THdfsFileDesc> file_desc

  // The access level Impala has on this partition (READ_WRITE, READ_ONLY, etc).
  11: optional TAccessLevel access_level

  // Statistics on this partition, e.g., number of rows in this partition.
  12: optional TTableStats stats

  // True if this partition has been marked as cached (does not necessarily mean the
  // underlying data is cached).
  13: optional bool is_marked_cached

  // (key,value) pairs stored in the Hive Metastore.
  15: optional map<string, string> hms_parameters

  // The following fields store stats about this partition
  // which are collected when toThrift() is called.
  // Total number of blocks in this partition.
  16: optional i64 num_blocks

  // Total file size in bytes of this partition.
  17: optional i64 total_file_size_bytes

  // byte[] representation of TPartitionStats for this partition that is compressed using
  // 'deflate-compression'.
  18: optional binary partition_stats

  // Set to true if partition_stats contain intermediate column stats computed via
  // incremental statistics, false otherwise.
  19: optional bool has_incremental_stats

  // For acid table, store last committed write id.
  20: optional i64 write_id
}

// Constant partition ID used for THdfsPartition.prototype_partition below.
// Must be < 0 to avoid collisions
const i64 PROTOTYPE_PARTITION_ID = -1;


struct THdfsTable {
  // ============================================================
  // Fields included in the "Descriptor" format sent to the backend
  // as part of query plans and fragments.
  // ============================================================

  1: required string hdfsBaseDir

  // Deprecated. Use TTableDescriptor.colNames.
  2: required list<string> colNames;

  // The string used to represent NULL partition keys.
  3: required string nullPartitionKeyValue

  // String to indicate a NULL column value in text files
  5: required string nullColumnValue

  // Set to the table's Avro schema if this is an Avro table
  6: optional string avroSchema

  // Map from partition id to partition metadata.
  // Does not include the special prototype partition with id=PROTOTYPE_PARTITION_ID --
  // that partition is separately included below.
  4: required map<i64, THdfsPartition> partitions

  // Prototype partition, used when creating new partitions during insert.
  10: required THdfsPartition prototype_partition

  // REMOVED: 8: optional bool multiple_filesystems

  // The prefixes of locations of partitions in this table. See THdfsPartitionLocation for
  // the description of how a prefix is computed.
  9: optional list<string> partition_prefixes

  // ============================================================
  // Fields only included when the catalogd serializes a table to be
  // sent to the impalad as part of a catalog update.
  // ============================================================

  // Each TNetworkAddress is a datanode which contains blocks of a file in the table.
  // Used so that each THdfsFileBlock can just reference an index in this list rather
  // than duplicate the list of network address, which helps reduce memory usage.
  7: optional list<Types.TNetworkAddress> network_addresses
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

// Parameters needed for hash partitioning
struct TKuduPartitionByHashParam {
  1: required list<string> columns
  2: required i32 num_partitions
}

struct TRangePartition {
  1: optional list<Exprs.TExpr> lower_bound_values
  2: optional bool is_lower_bound_inclusive
  3: optional list<Exprs.TExpr> upper_bound_values
  4: optional bool is_upper_bound_inclusive
}

// A range partitioning is identified by a list of columns and a list of range partitions.
struct TKuduPartitionByRangeParam {
  1: required list<string> columns
  2: optional list<TRangePartition> range_partitions
}

// Parameters for the PARTITION BY clause.
struct TKuduPartitionParam {
  1: optional TKuduPartitionByHashParam by_hash_param;
  2: optional TKuduPartitionByRangeParam by_range_param;
}

// Represents a Kudu table
struct TKuduTable {
  1: required string table_name

  // Network address of a master host in the form of 0.0.0.0:port
  2: required list<string> master_addresses

  // Name of the key columns
  3: required list<string> key_columns

  // Partitioning
  4: required list<TKuduPartitionParam> partition_by
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

  // The access level Impala has on this table (READ_WRITE, READ_ONLY, etc).
  4: optional TAccessLevel access_level

  // List of columns (excludes clustering columns)
  5: optional list<TColumn> columns

  // List of clustering columns (empty list if table has no clustering columns)
  6: optional list<TColumn> clustering_columns

  // Table stats data for the table.
  7: optional TTableStats table_stats

  // Determines the table type - either HDFS, HBASE, or VIEW.
  8: optional TTableType table_type

  // Set iff this is an HDFS table
  9: optional THdfsTable hdfs_table

  // Set iff this is an Hbase table
  10: optional THBaseTable hbase_table

  // The Hive Metastore representation of this table. May not be set if there were
  // errors loading the table metadata
  11: optional hive_metastore.Table metastore_table

  // Set iff this is a table from an external data source
  12: optional TDataSourceTable data_source_table

  // Set iff this a kudu table
  13: optional TKuduTable kudu_table

  // Set iff this is an acid table. The valid write ids list.
  // The string is assumed to be created by ValidWriteIdList.writeToString
  // For example ValidReaderWriteIdList object's format is:
  // <table_name>:<highwatermark>:<minOpenWriteId>:<open_writeids>:<abort_writeids>
  14: optional string valid_write_ids
}

// Represents a database.
struct TDatabase {
  // Name of the database. Case insensitive, expected to be stored as lowercase.
  1: required string db_name

  // The Hive Metastore representation of this database. May not be set if there were
  // errors loading the database metadata
  2: optional hive_metastore.Database metastore_db
}

// Represents a type of principal.
enum TPrincipalType {
  ROLE = 0
  USER = 1
  GROUP = 2
}

// Represents a principal in an authorization policy.
struct TPrincipal {
  // Case-insensitive principal name
  1: required string principal_name

  // Unique ID of this principal, generated by the Catalog Server.
  2: required i32 principal_id

  // Type of this principal.
  3: required TPrincipalType principal_type

  // List of groups this principal has been granted to (group names are case sensitive).
  // TODO: Keep a list of grant groups globally (in TCatalog?) and reference by ID since
  // the same groups will likely be shared across multiple principals.
  4: required list<string> grant_groups
}

// The scope a TPrivilege applies to.
enum TPrivilegeScope {
  SERVER = 0
  URI = 1
  DATABASE = 2
  TABLE = 3
  COLUMN = 4
}

// The privilege level allowed.
enum TPrivilegeLevel {
  ALL = 0
  INSERT = 1
  SELECT = 2
  REFRESH = 3
  CREATE = 4
  ALTER = 5
  DROP = 6
  OWNER = 7
}

// Represents a privilege in an authorization policy. Privileges contain the level
// of access, the scope and principal the privilege applies to, and details on what
// catalog object the privilege is securing. Objects are hierarchical, so a privilege
// corresponding to a table must also specify all the parent objects (database name
// and server name).
struct TPrivilege {
  // NOTE: This field is no longer needed. Keeping it here to keep the field numbers.
  // A human readable name for this privilege. The combination of principal_id +
  // privilege_name is guaranteed to be unique. Stored in a form that can be passed
  // to Sentry: [ServerName]->[DbName]->[TableName]->[ColumnName]->[Action Granted].
  // 1: required string privilege_name

  // The level of access this privilege provides.
  2: required TPrivilegeLevel privilege_level

  // The scope of the privilege: SERVER, DATABASE, URI, TABLE or COLUMN
  3: required TPrivilegeScope scope

  // If true, GRANT OPTION was specified. For a GRANT privilege statement, everyone
  // granted this principal should be able to issue GRANT/REVOKE privilege statements even
  // if they are not an admin. For REVOKE privilege statements, the privilege should be
  // retainined and the existing GRANT OPTION (if it was set) on the privilege should be
  // removed.
  4: required bool has_grant_opt

  // The ID of the principal this privilege belongs to.
  5: optional i32 principal_id

  // The type of the principal this privilege belongs to.
  6: optional TPrincipalType principal_type

  // Set if scope is SERVER, URI, DATABASE, or TABLE
  7: optional string server_name

  // Set if scope is DATABASE or TABLE
  8: optional string db_name

  // Unqualified table name. Set if scope is TABLE.
  9: optional string table_name

  // Set if scope is URI
  10: optional string uri

  // Time this privilege was created (in milliseconds since epoch).
  11: optional i64 create_time_ms

  // Set if scope is COLUMN
  12: optional string column_name
}

// Thrift representation of an HdfsCachePool.
struct THdfsCachePool {
  // Name of the cache pool
  1: required string pool_name

  // In the future we may want to include additional info on the pool such as
  // the pool limits, pool owner, etc.
}

// Thrift representation of an TAuthzCacheInvalidation. This catalog object does not
// contain any authorization data and it's used as marker to perform an authorization
// cache invalidation.
struct TAuthzCacheInvalidation {
  // Name of the authorization cache marker.
  1: required string marker_name
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

  // Set iff object type is PRINCIPAL
  8: optional TPrincipal principal

  // Set iff object type is PRIVILEGE
  9: optional TPrivilege privilege

  // Set iff object type is HDFS_CACHE_POOL
  10: optional THdfsCachePool cache_pool

  // Set iff object type is AUTHZ_CACHE_INVALIDATION
  11: optional TAuthzCacheInvalidation authz_cache_invalidation
}
