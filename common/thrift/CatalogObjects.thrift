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

include "Data.thrift"
include "Exprs.thrift"
include "Status.thrift"
include "Types.thrift"
include "hive_metastore.thrift"
include "SqlConstraints.thrift"

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
  HDFS_PARTITION = 11
}

enum TTableType {
  HDFS_TABLE = 0
  HBASE_TABLE = 1
  VIEW = 2
  DATA_SOURCE_TABLE = 3
  KUDU_TABLE = 4
  ICEBERG_TABLE = 5
  // Type for tables that we haven't loaded its full metadata so we don't know whether
  // it's a HDFS or Kudu table, etc. We just know it's not a view.
  UNLOADED_TABLE = 6
  // We added MATERIALIZED_VIEW as a table type to TImpalaTableType in IMPALA-3268.
  // To properly set the table type of a materialized view when calling
  // JniFrontend#updateCatalogCache(), we need to also introduce this table type here
  // so that a materialized view will not be classified as a table. Refer to
  // IncompleteTable#toThrift() for further details.
  MATERIALIZED_VIEW = 7
  // Represents a system table reflecting backend internal state.
  SYSTEM_TABLE = 8
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
  HUDI_PARQUET = 7
  ICEBERG = 8
  JSON = 9
  JDBC = 10
}

enum TVirtualColumnType {
  NONE,
  INPUT_FILE_NAME,
  FILE_POSITION,
  PARTITION_SPEC_ID,
  ICEBERG_PARTITION_SERIALIZED,
  ICEBERG_DATA_SEQUENCE_NUMBER
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
  BROTLI = 11
  LZ4_BLOCKED = 12
}

// Iceberg table file format identified by table property 'write.format.default'
enum TIcebergFileFormat {
  PARQUET = 0
  ORC = 1
  AVRO = 2
}

// Iceberg table catalog type identified by table property 'iceberg.catalog'
enum TIcebergCatalog {
  HADOOP_TABLES = 0
  HADOOP_CATALOG = 1
  HIVE_CATALOG = 2
  CATALOGS = 3
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

// Table properties used by Impala
const string TBL_PROP_SYSTEM_TABLE = "__IMPALA_SYSTEM_TABLE"

// The access level that is available to Impala on the Catalog object.
enum TAccessLevel {
  NONE = 0
  READ_WRITE = 1
  READ_ONLY = 2
  WRITE_ONLY = 3
}

enum TIcebergPartitionTransformType {
  IDENTITY = 0
  HOUR = 1
  DAY = 2
  MONTH = 3
  YEAR = 4
  BUCKET = 5
  TRUNCATE = 6
  VOID = 7
}

// Data distribution method of bucketed table.
// (Easy to add more types later.)
enum TBucketType {
  // Non-Bucketed
  NONE = 0
  // For hive compatibility, the hash function used in Hive's bucketed tables
  HASH = 1
}

struct TCompressionCodec {
  // Compression codec
  1: required THdfsCompression codec
  // Compression level
  2: optional i32 compression_level
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

// Represents the bucket spec of a table.
struct TBucketInfo {
  1: required TBucketType bucket_type
  2: optional list<string> bucket_columns
  3: required i32 num_bucket
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

  // Estimated number of true and false value for boolean type
  5: required i64 num_trues
  6: required i64 num_falses

  // The low and the high value
  7: optional Data.TColumnValue low_value
  8: optional Data.TColumnValue high_value
}

// Intermediate state for the computation of per-column stats. Impala can aggregate these
// structures together to produce final stats for a column.
// Fields should be optional for backward compatibility since this is stored in HMS
// partition properties.
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

  // The number of true and false value, of the column
  7: optional i64 num_trues
  8: optional i64 num_falses

  // The low and the high value
  9: optional Data.TColumnValue low_value
  10: optional Data.TColumnValue high_value
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
  6: optional TVirtualColumnType virtual_column_type = TVirtualColumnType.NONE
  // True for hidden columns
  7: optional bool is_hidden

  // Indicates whether this is an HBase column. If true, implies
  // all following HBase-specific fields are set.
  8: optional bool is_hbase_column
  9: optional string column_family
  10: optional string column_qualifier
  11: optional bool is_binary

  // The followings are Kudu-specific column properties
  12: optional bool is_kudu_column
  13: optional bool is_key
  14: optional bool is_nullable
  15: optional TColumnEncoding encoding
  16: optional THdfsCompression compression
  17: optional Exprs.TExpr default_value
  18: optional i32 block_size
  // The column name, in the case that it appears in Kudu.
  19: optional string kudu_column_name
  24: optional bool is_primary_key_unique
  25: optional bool is_auto_incrementing

  // Here come the Iceberg-specific fields.
  20: optional bool is_iceberg_column
  21: optional i32 iceberg_field_id
  // Key and value field id for Iceberg column with Map type.
  22: optional i32 iceberg_field_map_key_id
  23: optional i32 iceberg_field_map_value_id
}

// Represents an HDFS file in a partition.
struct THdfsFileDesc {
  // File descriptor metadata serialized into a FlatBuffer
  // (defined in common/fbs/CatalogObjects.fbs).
  // TODO: Put this in a KRPC sidecar to avoid serialization cost.
  1: required binary file_desc_data

  // Additional file metadata serialized into a FlatBuffer
  // TODO: Put this in a KRPC sidecar to avoid serialization cost.
  2: optional binary file_metadata
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

// Represents the file format metadata for files stored in a HDFS table or partition.
struct THdfsStorageDescriptor {
  1: required byte lineDelim
  2: required byte fieldDelim
  3: required byte collectionDelim
  4: required byte mapKeyDelim
  5: required byte escapeChar
  6: required byte quoteChar
  7: required THdfsFileFormat fileFormat
  8: required i32 blockSize
}

// Represents an HDFS partition
// TODO(vercegovac): rename to TFsPartition
struct THdfsPartition {

  // ============================================================
  // Fields included in the "Descriptor" format sent to the backend
  // as part of query plans and fragments.
  // ============================================================

  // These are Literal expressions
  7: list<Exprs.TExpr> partitionKeyExprs

  10: optional THdfsPartitionLocation location

  // Unique (in the catalog) id of this partition. May be set to
  // PROTOTYPE_PARTITION_ID when this object is used to describe
  // a partition which will be created as part of a query.
  14: optional i64 id
  // The partition id of the previous instance that is replaced by this. Catalogd uses
  // this to send invalidations of stale partition instances for catalog-v2 coordinators.
  26: optional i64 prev_id = -1

  // ============================================================
  // Fields only included when the catalogd serializes a table to be
  // sent to the impalad as part of a catalog update.
  // ============================================================

  9: optional list<THdfsFileDesc> file_desc

  // List of ACID insert delta file descriptors.
  21: optional list<THdfsFileDesc> insert_file_desc

  // List of ACID delete delta file descriptors.
  22: optional list<THdfsFileDesc> delete_file_desc

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

  // These fields are required in catalog updates. Coordinators use them to locate the
  // related partition.
  23: optional string db_name
  24: optional string tbl_name
  25: optional string partition_name

  27: optional THdfsStorageDescriptor hdfs_storage_descriptor
}

// Constant partition ID used for THdfsPartition.prototype_partition below.
// Must be < 0 to avoid collisions
const i64 PROTOTYPE_PARTITION_ID = -1;

// Thrift representation of a Hive ACID valid write id list.
struct TValidWriteIdList {
  // Every write id greater than 'high_watermark' are invalid.
  1: optional i64 high_watermark

  // The smallest open write id.
  2: optional i64 min_open_write_id

  // Open or aborted write ids.
  3: optional list<i64> invalid_write_ids

  // Indexes of the aborted write ids in 'invalid_write_ids'. The write ids whose index
  // are not present here are open.
  4: optional list<i32> aborted_indexes
}

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
  // Partition metadata in the values can be empty (in cases only partition ids are used)
  // or only contain the partition name. Reflected by the following flags.
  4: required map<i64, THdfsPartition> partitions
  // True if the partition map contains full metadata of all partitions.
  14: optional bool has_full_partitions
  // True if the partition map contains partition names in all partition values.
  // False if the partition map contains empty partition values. In this case, only the
  // partition ids are usable.
  // Only valid when has_full_partitions is false.
  15: optional bool has_partition_names

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
  7: optional list<Types.TNetworkAddress> network_addresses,

  // Primary Keys information for HDFS Tables
  11: optional SqlConstraints.TSqlConstraints sql_constraints

  // True if the table is in Hive Full ACID format.
  12: optional bool is_full_acid = false

  // Set iff this is an acid table. The valid write ids list.
  13: optional TValidWriteIdList valid_write_ids

  // Bucket information for HDFS tables
  16: optional TBucketInfo bucket_info

  // Recently dropped partitions that are not yet synced to the catalog topic.
  // Only used in catalogd.
  17: optional list<THdfsPartition> dropped_partitions
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
  5: optional list<TKuduPartitionParam> hash_specs
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

  // Set to true if primary key of the Kudu table is unique.
  // Kudu engine automatically adds an auto-incrementing column in the table if
  // primary key is not unique, in this case, this field is set to false.
  5: optional bool is_primary_key_unique

  // Set to true if the table has auto-incrementing column
  6: optional bool has_auto_incrementing
}

struct TIcebergPartitionTransform {
  1: required TIcebergPartitionTransformType transform_type

  // Parameter for BUCKET and TRUNCATE transforms.
  2: optional i32 transform_param
}

struct TIcebergPartitionField {
  1: required i32 source_id
  2: required i32 field_id
  3: required string orig_field_name
  4: required string field_name
  5: required TIcebergPartitionTransform transform
  6: required Types.TScalarType type
}

struct TIcebergPartitionSpec {
  1: required i32 spec_id
  2: optional list<TIcebergPartitionField> partition_fields
}

struct TIcebergPartitionStats {
  1: required i64 num_files;
  2: required i64 num_rows;
  3: required i64 file_size_in_bytes;
}

// Contains maps from 128-bit Murmur3 hash of file path to its file descriptor
struct TIcebergContentFileStore {
  1: optional map<string, THdfsFileDesc> path_hash_to_data_file_without_deletes
  2: optional map<string, THdfsFileDesc> path_hash_to_data_file_with_deletes
  3: optional map<string, THdfsFileDesc> path_hash_to_position_delete_file
  4: optional map<string, THdfsFileDesc> path_hash_to_equality_delete_file
  5: optional bool has_avro
  6: optional bool has_orc
  7: optional bool has_parquet
}

// Represents a drop partition request for Iceberg tables
struct TIcebergDropPartitionRequest {
  // List of affected file paths (could be empty if the drop partition
  // request can be exchanged with a truncate command)
  1: required list<string> paths
  // Indicates whether the request could be exchanged with a truncate command
  2: required bool is_truncate
  // Number of affected partitions that will be dropped
  3: required i64 num_partitions
}

struct TIcebergTable {
  // Iceberg file system table location
  1: required string table_location
  2: required list<TIcebergPartitionSpec> partition_spec
  3: required i32 default_partition_spec_id
  // Iceberg data and delete files
  4: optional TIcebergContentFileStore content_files
  // Snapshot id of the org.apache.iceberg.Table object cached in the CatalogD
  5: optional i64 catalog_snapshot_id;
  // Iceberg 'write.parquet.compression-codec' and 'write.parquet.compression-level' table
  // properties
  6: optional TCompressionCodec parquet_compression_codec
  // Iceberg 'write.parquet.row-group-size-bytes' table property
  7: optional i64 parquet_row_group_size
  // Iceberg 'write.parquet.page-size-bytes' and 'write.parquet.dict-size-bytes' table
  // properties
  8: optional i64 parquet_plain_page_size;
  9: optional i64 parquet_dict_page_size;
  10: optional map<string, TIcebergPartitionStats> partition_stats;
}

// System Table identifiers.
// These are used as the table name, so should not be changed.
enum TSystemTableName {
  IMPALA_QUERY_LIVE = 0
}

// Represents a System Table
struct TSystemTable {
  1: required TSystemTableName table_name
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

  // List of virtual columns (empty list if table has no virtual columns)
  7: optional list<TColumn> virtual_columns

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

  // Set iff this a kudu table
  14: optional TKuduTable kudu_table

  // Set if this table needs storage access during metadata load.
  // Time used for storage loading in nanoseconds.
  16: optional i64 storage_metadata_load_time_ns

  // Set if this a iceberg table
  17: optional TIcebergTable iceberg_table

  // Comment of the table/view. Set only for FeIncompleteTable where msTable doesn't
  // exists.
  18: optional string tbl_comment

  // Set if this is a system table
  19: optional TSystemTable system_table
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
  STORAGE_TYPE = 5
  STORAGEHANDLER_URI = 6
  USER_DEFINED_FN = 7
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
  RWSTORAGE = 8
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

  // Set if scope is DATABASE or TABLE or USER_DEFINED_FN
  8: optional string db_name

  // Unqualified table name. Set if scope is TABLE.
  9: optional string table_name

  // Set if scope is URI
  10: optional string uri

  // Time this privilege was created (in milliseconds since epoch).
  11: optional i64 create_time_ms

  // Set if scope is COLUMN
  12: optional string column_name

  13: optional string storage_type

  14: optional string storage_url

  // Set if scope is USER_DEFINED_FN
  15: optional string fn_name
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

  // The catalog version last time when we reset the entire catalog
  2: required i64 last_reset_catalog_version
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

  // Set iff object type is HDFS_PARTITION
  12: optional THdfsPartition hdfs_partition
}
