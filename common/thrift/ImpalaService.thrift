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

include "ExecStats.thrift"
include "Status.thrift"
include "Types.thrift"
include "beeswax.thrift"
include "TCLIService.thrift"
include "RuntimeProfile.thrift"
include "Frontend.thrift"
include "BackendGflags.thrift"
include "Query.thrift"

// ImpalaService accepts query execution options through beeswax.Query.configuration in
// key:value form. For example, the list of strings could be:
//     "num_nodes:1", "abort_on_error:false"
// The valid keys are listed in this enum. They map to TQueryOptions.
// Note: If you add an option or change the default, you also need to update:
// - ImpalaInternalService.thrift: TQueryOptions
// - SetQueryOption(), SetQueryOptions()
// - TQueryOptionsToMap()
enum TImpalaQueryOptions {
  // if true, abort execution on the first error
  ABORT_ON_ERROR = 0

  // maximum # of errors to be reported; Unspecified or 0 indicates backend default
  MAX_ERRORS = 1

  // if true disable llvm codegen
  DISABLE_CODEGEN = 2

  // batch size to be used by backend; Unspecified or a size of 0 indicates backend
  // default
  BATCH_SIZE = 3

  // a per-machine approximate limit on the memory consumption of this query;
  // unspecified or a limit of 0 means no limit;
  // otherwise specified either as:
  // a) an int (= number of bytes);
  // b) a float followed by "M" (MB) or "G" (GB)
  MEM_LIMIT = 4

  // specifies the degree of parallelism with which to execute the query;
  // 1: single-node execution
  // NUM_NODES_ALL: executes on all nodes that contain relevant data
  // NUM_NODES_ALL_RACKS: executes on one node per rack that holds relevant data
  // > 1: executes on at most that many nodes at any point in time (ie, there can be
  //      more nodes than numNodes with plan fragments for this query, but at most
  //      numNodes would be active at any point in time)
  // Constants (NUM_NODES_ALL, NUM_NODES_ALL_RACKS) are defined in JavaConstants.thrift.
  NUM_NODES = 5

  // maximum length of the scan range; only applicable to HDFS scan range; Unspecified or
  // a length of 0 indicates backend default;
  MAX_SCAN_RANGE_LENGTH = 6

  MAX_IO_BUFFERS = 7 // Removed

  // Number of scanner threads.
  NUM_SCANNER_THREADS = 8

  ALLOW_UNSUPPORTED_FORMATS = 9 // Removed

  DEFAULT_ORDER_BY_LIMIT = 10 // Removed

  // DEBUG ONLY:
  // Accepted formats:
  // 1. ExecNode actions
  //  "[<instance idx>:]<node id>:<TExecNodePhase>:<TDebugAction>",
  //  the exec node with the given id will perform the specified action in the given
  //  phase. If the optional backend number (starting from 0) is specified, only that
  //  backend instance will perform the debug action, otherwise all backends will behave
  //  in that way.
  //  If the string doesn't have the required format or if any of its components is
  //  invalid, the option is ignored.
  //
  // 2. Global actions
  //  "<global label>:<arg0>:...:<argN>:<command>@<param0>@<param1>@...<paramN>",
  //  Used with the DebugAction() call, the action will be performed if the label and
  //  optional arguments all match. The arguments can be used to make the debug action
  //  context dependent, for example to only fail rpcs when a particular hostname matches.
  //  Note that some debug actions must be specified as a query option while others must
  //  be passed in with the startup flag.
  //  Available global actions:
  //  - SLEEP@<ms> will sleep for the 'ms' milliseconds.
  //  - JITTER@<ms>[@<probability>] will sleep for a random amount of time between 0
  //    and 'ms' milliseconds with the given probability. If <probability> is omitted,
  //    it is 1.0.
  //  - FAIL[@<probability>][@<error>] returns an INTERNAL_ERROR status with the given
  //    probability and error. If <probability> is omitted, it is 1.0. If 'error' is
  //    omitted, a generic error of the form: 'Debug Action: <label>:<action>' is used.
  //    The code executing the debug action may respond to different error messages by
  //    exercising different error paths.
  // Examples:
  // - AC_BEFORE_ADMISSION:SLEEP@1000
  //   Causes a 1 second sleep before queries are submitted to the admission controller.
  // - IMPALA_SERVICE_POOL:127.0.0.1:27002:TransmitData:FAIL@0.1@REJECT_TOO_BUSY
  //   Causes TransmitData rpcs to the third minicluster impalad to fail 10% of the time
  //   with a "service too busy" error.
  //
  // Only a single ExecNode action is allowed, but multiple global actions can be
  // specified. To specify multiple actions, separate them with "|".
  DEBUG_ACTION = 11

  ABORT_ON_DEFAULT_LIMIT_EXCEEDED = 12 // Removed

  // Compression codec when inserting into tables.
  // Valid values are "snappy", "gzip", "bzip2" and "none"
  // Leave blank to use default.
  COMPRESSION_CODEC = 13

  SEQ_COMPRESSION_MODE = 14 // Removed

  // HBase scan query option. If set and > 0, HBASE_CACHING is the value for
  // "hbase.client.Scan.setCaching()" when querying HBase table. Otherwise, use backend
  // default.
  // If the value is too high, then the hbase region server will have a hard time (GC
  // pressure and long response times). If the value is too small, then there will be
  // extra trips to the hbase region server.
  HBASE_CACHING = 15

  // HBase scan query option. If set, HBase scan will always set
  // "hbase.client.setCacheBlocks" to CACHE_BLOCKS. Default is false.
  // If the table is large and the query is doing big scan, set it to false to
  // avoid polluting the cache in the hbase region server.
  // If the table is small and the table is used several time, set it to true to improve
  // performance.
  HBASE_CACHE_BLOCKS = 16

  // Target file size for inserts into parquet tables. 0 uses the default.
  PARQUET_FILE_SIZE = 17

  // Level of detail for explain output (NORMAL, VERBOSE).
  EXPLAIN_LEVEL = 18

  // If true, waits for the result of all catalog operations to be processed by all
  // active impalad in the cluster before completing.
  SYNC_DDL = 19

  // Request pool this request should be submitted to. If not set
  // the pool is determined based on the user.
  REQUEST_POOL = 20

  V_CPU_CORES = 21 // Removed

  RESERVATION_REQUEST_TIMEOUT = 22 // Removed

  // if true, disables cached reads. This option has no effect if REPLICA_PREFERENCE is
  // configured.
  // TODO: IMPALA-4306: retire at compatibility-breaking version
  DISABLE_CACHED_READS = 23

  // Temporary testing flag
  DISABLE_OUTERMOST_TOPN = 24

  RM_INITIAL_MEM = 25 // Removed

  // Time, in s, before a query will be timed out if it is inactive. May not exceed
  // --idle_query_timeout if that flag > 0. If 0, falls back to --idle_query_timeout.
  QUERY_TIMEOUT_S = 26

  // Test hook for spill to disk operators
  BUFFER_POOL_LIMIT = 27

  // Transforms all count(distinct) aggregations into NDV()
  APPX_COUNT_DISTINCT = 28

  // If true, allows Impala to internally disable spilling for potentially
  // disastrous query plans. Impala will excercise this option if a query
  // has no plan hints, and at least one table is missing relevant stats.
  DISABLE_UNSAFE_SPILLS = 29

  // If the number of rows that are processed for a single query is below the
  // threshold, it will be executed on the coordinator only with codegen disabled
  EXEC_SINGLE_NODE_ROWS_THRESHOLD = 30

  // If true, use the table's metadata to produce the partition columns instead of table
  // scans whenever possible. This option is opt-in by default as this optimization may
  // produce different results than the scan based approach in some edge cases.
  OPTIMIZE_PARTITION_KEY_SCANS = 31

  // Prefered memory distance of replicas. This parameter determines the pool of replicas
  // among which scans will be scheduled in terms of the distance of the replica storage
  // from the impalad.
  REPLICA_PREFERENCE = 32

  // Enables random backend selection during scheduling.
  SCHEDULE_RANDOM_REPLICA = 33

  SCAN_NODE_CODEGEN_THRESHOLD = 34 // Removed

  // If true, the planner will not generate plans with streaming preaggregations.
  DISABLE_STREAMING_PREAGGREGATIONS = 35

  RUNTIME_FILTER_MODE = 36

  // Size (in bytes) of a runtime Bloom Filter. Will be rounded up to nearest power of
  // two.
  RUNTIME_BLOOM_FILTER_SIZE = 37

  // Time (in ms) to wait in scans for runtime filters to arrive.
  RUNTIME_FILTER_WAIT_TIME_MS = 38

  // If true, disable application of runtime filters to individual rows.
  DISABLE_ROW_RUNTIME_FILTERING = 39

  // Maximum number of bloom runtime filters allowed per query.
  MAX_NUM_RUNTIME_FILTERS = 40

  // If true, use UTF-8 annotation for string columns. Note that char and varchar columns
  // always use the annotation.
  PARQUET_ANNOTATE_STRINGS_UTF8 = 41

  // Determines how to resolve Parquet files' schemas in the absence of field IDs (which
  // is always, since fields IDs are NYI). Valid values are "position" and "name".
  PARQUET_FALLBACK_SCHEMA_RESOLUTION = 42

  // Multi-threaded execution: degree of parallelism = number of active threads per
  // backend
  MT_DOP = 43

  // If true, INSERT writes to S3 go directly to their final location rather than being
  // copied there by the coordinator. We cannot do this for INSERT OVERWRITES because for
  // those queries, the coordinator deletes all files in the final location before copying
  // the files there.
  // TODO: Find a way to get this working for INSERT OVERWRITEs too.
  S3_SKIP_INSERT_STAGING = 44

  // Maximum runtime bloom filter size, in bytes.
  RUNTIME_FILTER_MAX_SIZE = 45

  // Minimum runtime bloom filter size, in bytes.
  RUNTIME_FILTER_MIN_SIZE = 46

  // Prefetching behavior during hash tables' building and probing.
  PREFETCH_MODE = 47

  // Additional strict handling of invalid data parsing and type conversions.
  STRICT_MODE = 48

  // A limit on the amount of scratch directory space that can be used;
  // Unspecified or a limit of -1 means no limit;
  // Otherwise specified in the same way as MEM_LIMIT.
  SCRATCH_LIMIT = 49

  // Indicates whether the FE should rewrite Exprs for optimization purposes.
  // It's sometimes useful to disable rewrites for testing, e.g., expr-test.cc.
  ENABLE_EXPR_REWRITES = 50

  // Indicates whether to use the new decimal semantics, which includes better
  // rounding and output types for multiply / divide
  DECIMAL_V2 = 51

  // Indicates whether to use dictionary filtering for Parquet files
  PARQUET_DICTIONARY_FILTERING = 52

  // Policy for resolving nested array fields in Parquet files.
  // An Impala array type can have several different representations in
  // a Parquet schema (three, two, or one level). There is fundamental ambiguity
  // between the two and three level encodings with index-based field resolution.
  // The ambiguity can manually be resolved using this query option, or by using
  // PARQUET_FALLBACK_SCHEMA_RESOLUTION=name.
  PARQUET_ARRAY_RESOLUTION = 53

  // Indicates whether to read statistics from Parquet files and use them during query
  // processing. This includes skipping data based on the statistics and computing query
  // results like "select min()".
  PARQUET_READ_STATISTICS = 54

  // Join distribution mode that is used when the join inputs have an unknown
  // cardinality, e.g., because of missing table statistics.
  DEFAULT_JOIN_DISTRIBUTION_MODE = 55

  // If the number of rows processed per node is below the threshold and disable_codegen
  // is unset, codegen will be automatically be disabled by the planner.
  DISABLE_CODEGEN_ROWS_THRESHOLD = 56

  // The default spillable buffer size, in bytes.
  DEFAULT_SPILLABLE_BUFFER_SIZE = 57

  // The minimum spillable buffer size, in bytes.
  MIN_SPILLABLE_BUFFER_SIZE = 58

  // The maximum row size that memory is reserved for, in bytes.
  MAX_ROW_SIZE = 59

  // The time, in seconds, that a session may be idle for before it is closed (and all
  // running queries cancelled) by Impala. If 0, idle sessions never expire.
  IDLE_SESSION_TIMEOUT = 60

  // Minimum number of bytes that will be scanned in COMPUTE STATS TABLESAMPLE,
  // regardless of the user-supplied sampling percent.
  COMPUTE_STATS_MIN_SAMPLE_SIZE = 61

  // Time limit, in s, before a query will be timed out after it starts executing. Does
  // not include time spent in planning, scheduling or admission control. A value of 0
  // means no time limit.
  EXEC_TIME_LIMIT_S = 62

  // When a query has both grouping and distinct exprs, impala can optionally include the
  // distinct exprs in the hash exchange of the first aggregation phase to spread the data
  // among more nodes. However, this plan requires another hash exchange on the grouping
  // exprs in the second phase which is not required when omitting the distinct exprs in
  // the first phase. Shuffling by both is better if the grouping exprs have low NDVs.
  SHUFFLE_DISTINCT_EXPRS = 63

  // This only has an effect if memory-estimate-based admission control is enabled, i.e.
  // max_mem_resources is set for the pool and, *contrary to best practices*, MEM_LIMIT
  // is not set. In that case, then min(MAX_MEM_ESTIMATE_FOR_ADMISSION,
  // planner memory estimate) is used for admission control purposes. This provides a
  // workaround if the planner's memory estimate is too high and prevents a runnable
  // query from being admitted. 0 or -1 means this has no effect. Defaults to 0.
  MAX_MEM_ESTIMATE_FOR_ADMISSION = 64

  // Admission control will reject queries when the number of reserved threads per backend
  // for the query exceeds this number. 0 or -1 means this has no effect.
  THREAD_RESERVATION_LIMIT = 65

  // Admission control will reject queries when the total number of reserved threads
  // across all backends for the query exceeds this number. 0 or -1 means this has no
  // effect.
  THREAD_RESERVATION_AGGREGATE_LIMIT = 66

  // Overrides the -kudu_read_mode flag to set the consistency level for Kudu scans.
  // Possible values are DEFAULT, READ_LATEST, and READ_AT_SNAPSHOT.
  KUDU_READ_MODE = 67

  // Allow reading of erasure coded files.
  ALLOW_ERASURE_CODED_FILES = 68

  // The timezone used in UTC<->localtime conversions. The default is the OS's timezone
  // at the coordinator, which can be overridden by environment variable $TZ.
  TIMEZONE = 69

  // Scan bytes limit, after which a query will be terminated with an error.
  SCAN_BYTES_LIMIT = 70

  // CPU time limit in seconds, after which a query will be terminated with an error.
  // Note that until IMPALA-7318 is fixed, CPU usage can be very stale and this may not
  // terminate queries soon enough.
  CPU_LIMIT_S = 71

  // The max number of estimated bytes a TopN operator is allowed to materialize, if the
  // planner thinks a TopN operator will exceed this limit, it falls back to a TotalSort
  // operator which is capable of spilling to disk (unlike the TopN operator which keeps
  // everything in memory). 0 or -1 means this has no effect.
  TOPN_BYTES_LIMIT = 72

  // An opaque string, not used by Impala itself, that can be used to identify
  // the client, like a User-Agent in HTTP. Drivers should set this to
  // their version number. May also be used by tests to help identify queries.
  CLIENT_IDENTIFIER = 73

  // Probability to enable tracing of resource usage consumption on all fragment instance
  // executors of a query. Must be between 0 and 1 inclusive, 0 means no query will be
  // traced, 1 means all queries will be traced.
  RESOURCE_TRACE_RATIO = 74

  // The maximum number of executor candidates to consider when scheduling remote
  // HDFS ranges. When non-zero, the scheduler generates a consistent set of
  // executor candidates based on the filename and offset. This algorithm is designed
  // to avoid changing file to node mappings when nodes come and go. It then picks from
  // among the candidates by the same method used for local scan ranges. Limiting the
  // number of nodes that can read a single file provides a type of simulated locality.
  // This increases the efficiency of file-related caches (e.g. the HDFS file handle
  // cache). If set to 0, the number of executor candidates is unlimited, and remote
  // ranges will be scheduled across all executors.
  NUM_REMOTE_EXECUTOR_CANDIDATES = 75

  // A limit on the number of rows produced by the query. The query will be
  // canceled if the query is still executing after this limit is hit. A value
  // of 0 means there is no limit on the number of rows produced.
  NUM_ROWS_PRODUCED_LIMIT = 76

  // Set when attempting to load a planner testcase. Typically used by developers for
  // debugging a testcase. Should not be set in user clusters. If set, a warning
  // is emitted in the query runtime profile.
  PLANNER_TESTCASE_MODE = 77

  // Specifies the default table file format.
  DEFAULT_FILE_FORMAT = 78

  // The physical type and unit used when writing timestamps in Parquet.
  // Valid values: INT96_NANOS, INT64_MILLIS, INT64_MICROS, INT64_NANOS
  // Default: INT96_NANOS
  PARQUET_TIMESTAMP_TYPE = 79

  // Enable using the Parquet page index during scans. The page index contains min/max
  // statistics at page-level granularity. It can be used to skip pages and rows during
  // scanning.
  PARQUET_READ_PAGE_INDEX = 80

  // Enable writing the Parquet page index.
  PARQUET_WRITE_PAGE_INDEX = 81

  // Maximum number of rows written in a single Parquet data page.
  PARQUET_PAGE_ROW_COUNT_LIMIT = 82

  // Disable the attempt to compute an estimated number of rows in an
  // hdfs table.
  DISABLE_HDFS_NUM_ROWS_ESTIMATE = 83

  // Default hints for insert statement. Will be overridden by hints in the INSERT
  // statement, if any.
  DEFAULT_HINTS_INSERT_STATEMENT = 84

  // Enable spooling of query results. If true, query results will be spooled in
  // memory up to a specified memory limit. If the memory limit is hit, the
  // coordinator fragment will block until the client has consumed enough rows to free
  // up more memory. If false, client consumption driven back-pressure controls the rate
  // at which rows are materialized by the execution tree.
  SPOOL_QUERY_RESULTS = 85

  // Speficies the default transactional type for new HDFS tables.
  // Valid values: none, insert_only
  DEFAULT_TRANSACTIONAL_TYPE = 86

  // Limit on the total number of expressions in the statement. Statements that exceed
  // the limit will get an error during analysis. This is intended to set an upper
  // bound on the complexity of statements to avoid resource impacts such as excessive
  // time in analysis or codegen. This is enforced only for the first pass of analysis
  // before any rewrites are applied.
  STATEMENT_EXPRESSION_LIMIT = 87

  // Limit on the total length of a SQL statement. Statements that exceed the maximum
  // length will get an error before parsing/analysis. This is complementary to the
  // statement expression limit, because statements of a certain size are highly
  // likely to violate the statement expression limit. Rejecting them early avoids
  // the cost of parsing/analysis.
  MAX_STATEMENT_LENGTH_BYTES = 88

  // Disable the data cache.
  DISABLE_DATA_CACHE = 89

  // The maximum amount of memory used when spooling query results. If this value is
  // exceeded when spooling results, all memory will be unpinned and most likely spilled
  // to disk. Set to 100 MB by default. Only applicable if SPOOL_QUERY_RESULTS
  // is true. Setting this to 0 or -1 means the memory is unbounded. Cannot be set to
  // values below -1.
  MAX_RESULT_SPOOLING_MEM = 90

  // The maximum amount of memory that can be spilled when spooling query results. Must be
  // greater than or equal to MAX_RESULT_SPOOLING_MEM to allow unpinning all pinned memory
  // if the amount of spooled results exceeds MAX_RESULT_SPOOLING_MEM. If this value is
  // exceeded, the coordinator fragment will block until the client has consumed enough
  // rows to free up more memory. Set to 1 GB by default. Only applicable if
  // SPOOL_QUERY_RESULTS is true. Setting this to 0 or -1 means the memory is unbounded.
  // Cannot be set to values below -1.
  MAX_SPILLED_RESULT_SPOOLING_MEM = 91

  // Disable the normal key sampling of HBase tables in row count and row size estimation.
  // Set this to true will force the use of HMS table stats.
  DISABLE_HBASE_NUM_ROWS_ESTIMATE = 92

  // The maximum amount of time, in milliseconds, a fetch rows request (TFetchResultsReq)
  // from the client should spend fetching results (including waiting for results to
  // become available and materialize). When result spooling is enabled, a fetch request
  // to may read multiple RowBatches, in which case, the timeout controls how long the
  // client waits for all returned RowBatches to be produced. If the timeout is hit, the
  // client returns whatever rows it has already read. Defaults to 10000 milliseconds. A
  // value of 0 causes fetch requests to wait indefinitely.
  FETCH_ROWS_TIMEOUT_MS = 93

  // For testing purposes only. This can provide a datetime string to use as now() for
  // tests.
  NOW_STRING = 94

  // The split size of Parquet files when scanning non-block-based storage systems (e.g.
  // S3, ADLS, etc.). When reading from block-based storage systems (e.g. HDFS), Impala
  // sets the split size for Parquet files to the size of the blocks. This is done
  // because Impala assumes Parquet files have a single row group per block (which is
  // the recommended way Parquet files should be written). However, since non-block-based
  // storage systems have no concept of blocks, there is no way to derive a good default
  // value for Parquet split sizes. Defaults to 256 MB, which is the default size of
  // Parquet files written by Impala (Impala writes Parquet files with a single row
  // group per file). Must be >= 1 MB.
  PARQUET_OBJECT_STORE_SPLIT_SIZE = 95

  // For testing purposes only. A per executor approximate limit on the memory consumption
  // of this query. Only applied if MEM_LIMIT is not specified.
  // unspecified or a limit of 0 means no limit;
  // Otherwise specified either as:
  // a) an int (= number of bytes);
  // b) a float followed by "M" (MB) or "G" (GB)
  MEM_LIMIT_EXECUTORS = 96

  // The max number of estimated bytes eligible for a Broadcast operation during a join.
  // If the planner thinks the total bytes sent to all destinations of a broadcast
  // exchange will exceed this limit, it will not consider a broadcast and instead
  // fall back on a hash partition exchange. 0 or -1 means this has no effect.
  BROADCAST_BYTES_LIMIT = 97

  // The max reservation that each grouping class in a preaggregation will use.
  // 0 or -1 means this has no effect.
  PREAGG_BYTES_LIMIT = 98

  // Indicates whether the FE should rewrite disjunctive predicates to conjunctive
  // normal form (CNF) for optimization purposes. Default is False.
  ENABLE_CNF_REWRITES = 99

  // The max number of conjunctive normal form (CNF) exprs to create when converting
  // a disjunctive expression to CNF. Each AND counts as 1 expression. A value of
  // -1 or 0 means no limit. Default is 200.
  MAX_CNF_EXPRS = 100

  // Set the timestamp for Kudu snapshot reads in Unix time micros. Only valid if
  // KUDU_READ_MODE is set to READ_AT_SNAPSHOT.
  KUDU_SNAPSHOT_READ_TIMESTAMP_MICROS = 101

  // Transparently retry queries that fail due to cluster membership changes. A cluster
  // membership change includes blacklisting a node and the statestore detecting that a
  // node has been removed from the cluster membership. From Impala's perspective, a
  // retried query is a brand new query. From the client perspective, requests for the
  // failed query are transparently re-routed to the new query.
  RETRY_FAILED_QUERIES = 102

  // Enabled runtime filter types to be applied to scanner.
  // This option only apply to Kudu now, will apply to HDFS once we support
  // min-max filter for HDFS.
  //     BLOOM   - apply bloom filter only,
  //     MIN_MAX - apply min-max filter only.
  //     ALL     - apply both bloom filter and min-max filter (default).
  ENABLED_RUNTIME_FILTER_TYPES = 103

  // Enable asynchronous codegen.
  ASYNC_CODEGEN = 104

  // If true, the planner will consider adding a distinct aggregation to SEMI JOIN
  // operations. If false, disables the optimization (i.e. falls back to pre-Impala-4.0
  // behaviour).
  ENABLE_DISTINCT_SEMI_JOIN_OPTIMIZATION = 105

  // The max reservation that sorter will use for intermediate sort runs.
  // 0 or -1 means this has no effect.
  SORT_RUN_BYTES_LIMIT = 106

  // Sets an upper limit on the number of fs writer instances to be scheduled during
  // insert. Currently this limit only applies to HDFS inserts.
  MAX_FS_WRITERS = 107

  // When this query option is set, a refresh table statement will detect existing
  // partitions which have been changed in metastore and refresh them. By default, this
  // option is disabled since there is additional performance hit to fetch all the
  // partitions and detect if they are not same as ones in the catalogd. Currently, this
  // option is only applicable for refresh table statement.
  REFRESH_UPDATED_HMS_PARTITIONS = 108

  // If RETRY_FAILED_QUERIES and SPOOL_QUERY_RESULTS are enabled and this is true,
  // retryable queries will try to spool all results before returning any to the client.
  // If the result set is too large to fit into the spooling memory (including the spill
  // mem), results will be returned and the query will not be retryable. This may have
  // some performance impact. Set it to false then clients can fetch results immediately
  // when any of them are ready. Note that in this case, query retry will be skipped if
  // the client has fetched some results.
  SPOOL_ALL_RESULTS_FOR_RETRIES = 109

  // A value (0.0, 1.0) that is the target false positive probability for runtime bloom
  // filters. If not set, falls back to max_filter_error_rate.
  RUNTIME_FILTER_ERROR_RATE = 110

  // When true, TIMESTAMPs are interpreted in the local time zone (set in query option
  // TIMEZONE) when converting to and from Unix times.
  // When false, TIMESTAMPs are interpreted in the UTC time zone.
  USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS = 111

  // When true, TIMESTAMPs read from files written by Parquet-MR (used by Hive) will
  // be converted from UTC to local time. Writes are unaffected.

  CONVERT_LEGACY_HIVE_PARQUET_UTC_TIMESTAMPS = 112

  // Indicates whether the FE should attempt to transform outer joins into inner joins.
  ENABLE_OUTER_JOIN_TO_INNER_TRANSFORMATION = 113

  // Set the target scan range length for scanning kudu tables (in bytes). This is
  // used to split kudu scan tokens and is treated as a hint by kudu. Therefore,
  // does not guarantee a limit on the size of the scan range. If unspecified or
  // set to 0 disables this feature.
  TARGETED_KUDU_SCAN_RANGE_LENGTH = 114

  // Enable (>=0) or disable(<0) reporting of skews for a query in runtime profile.
  // When enabled, used as the CoV threshold value in the skew detection formula.
  REPORT_SKEW_LIMIT = 115

  // If true, for simple limit queries (limit with no order-by, group-by, aggregates,
  // joins, analytic functions but does allow where predicates) optimize the planning
  // time by only considering a small number of partitions. The number of files within
  // a partition is used as an approximation to number of rows (1 row per file).
  // This option is opt-in by default as this optimization may in some cases produce
  // fewer (but correct) rows than the limit value in the query.
  OPTIMIZE_SIMPLE_LIMIT = 116

  // When true, the degree of parallelism (if > 1) is used in costing decision
  // of a broadcast vs partition distribution.
  USE_DOP_FOR_COSTING = 117

  // A multiplying factor between 0 to 1000 that is applied to the costing decision of
  // a broadcast vs partition distribution. Fractional values between 0 to 1 favor
  // broadcast by reducing the build side cost of a broadcast join. Values above 1.0
  // favor partition distribution.
  BROADCAST_TO_PARTITION_FACTOR = 118

  // A limit on the number of join rows produced by the query. The query will be
  // canceled if the query is still executing after this limit is hit. A value
  // of 0 means there is no limit on the number of join rows produced.
  JOIN_ROWS_PRODUCED_LIMIT = 119

  // If true, strings are processed in a UTF-8 aware way, e.g. counting lengths by UTF-8
  // characters instead of bytes.
  UTF8_MODE = 120

  // If > 0, the rank()/row_number() pushdown into pre-analytic sorts is enabled
  // if the limit would be less than or equal to the threshold.
  // If 0 or -1, disables the optimization (i.e. falls back to pre-Impala-4.0
  // behaviour).
  // Default is 1000. Setting it higher increases the max size of the in-memory heaps
  // used in the top-n operation. The larger the heaps, the less beneficial the
  // optimization is compared to a full sort and the more potential for perf regressions.
  ANALYTIC_RANK_PUSHDOWN_THRESHOLD = 121

  // Specifiy a threshold between 0.0 and 1.0 to control the operation of overlap
  // predicates (via min/max filters) for equi-hash joins at the Parquet table scan
  // operator. Set to 0.0 to disable the feature. Set to 1.0 to evaluate all
  // overlap predicates. Set to a value between 0.0 and 1.0 to evaluate only those
  // with an overlap ratio less than the threshold.
  MINMAX_FILTER_THRESHOLD = 122

  // Minmax filtering level to be applied to Parquet scanners.
  //     ROW_GROUP - apply to row groups only,
  //     PAGE      - apply to row groups and pages only.
  //     ROW       - apply to row groups, pages and rows.
  MINMAX_FILTERING_LEVEL = 123

  // If true, compute the column min and max stats during compute stats.
  COMPUTE_COLUMN_MINMAX_STATS = 124

  // If true, show the min and max stats during show column stats.
  SHOW_COLUMN_MINMAX_STATS = 125

  // Default NDV scale settings, make it easier to change scale in SQL function
  // NDV(<expr>).
  DEFAULT_NDV_SCALE = 126

  // Policy with which to choose amongst multiple Kudu replicas.
  //     LEADER_ONLY     - Select the LEADER replica.
  //     CLOSEST_REPLICA - Select the closest replica to the client (default).
  KUDU_REPLICA_SELECTION = 127

  // If false, a truncate DDL operation will not delete table stats.
  // the default value of this is true as set in Query.thrift
  DELETE_STATS_IN_TRUNCATE = 128

  // Indicates whether to use Bloom filtering for Parquet files
  PARQUET_BLOOM_FILTERING = 129

  // If true, generate min/max filters when join into sorted columns.
  MINMAX_FILTER_SORTED_COLUMNS = 130

  // Control setting for taking the fast code path when min/max filtering sorted columns.
  //     OFF - Take the regular code path,
  //     ON  - Take the fast code path,
  //     VERIFICATION - Take both code paths and verify that the results from both are
  //                    the same.
  MINMAX_FILTER_FAST_CODE_PATH = 131

  // If true, Kudu's multi-row transaction is enabled.
  ENABLE_KUDU_TRANSACTION = 132

  // Indicates whether to use min/max filtering on partition columns
  MINMAX_FILTER_PARTITION_COLUMNS = 133

  // Controls when to write Parquet Bloom filters.
  //     NEVER      - never write Parquet Bloom filters
  //     TBL_PROPS  - write Parquet Bloom filters as set in table properties
  //     IF_NO_DICT - write Parquet Bloom filters if the row group is not fully
  //                  dictionary encoded
  //     ALWAYS     - always write Parquet Bloom filters, even if the row group is fully
  //                  dictionary encoded
  PARQUET_BLOOM_FILTER_WRITE = 134

  // Indicates whether to use ORC's search argument to push down predicates.
  ORC_READ_STATISTICS = 135
}

// The summary of a DML statement.
struct TDmlResult {
  // Number of modified rows per partition. Only applies to HDFS and Kudu tables.
  // The keys represent partitions to create, coded as k1=v1/k2=v2/k3=v3..., with
  // the root in an unpartitioned table being the empty string.
  1: required map<string, i64> rows_modified

  // Number of row operations attempted but not completed due to non-fatal errors
  // reported by the storage engine that Impala treats as warnings. Only applies to Kudu
  // tables. This includes errors due to duplicate/missing primary keys, nullability
  // constraint violations, and primary keys in uncovered partition ranges.
  // TODO: Provide a detailed breakdown of these counts by error. IMPALA-4416.
  2: optional i64 num_row_errors
}

// Response from a call to PingImpalaService
struct TPingImpalaServiceResp {
  // The Impala service's version string.
  1: string version

  // The Impalad's webserver address.
  2: string webserver_address
}

// Parameters for a ResetTable request which will invalidate a table's metadata.
// DEPRECATED.
struct TResetTableReq {
  // Name of the table's parent database.
  1: required string db_name

  // Name of the table.
  2: required string table_name
}

// PingImpalaHS2Service() - ImpalaHiveServer2Service version.
// Pings the Impala server to confirm that the server is alive and the session identified
// by 'sessionHandle' is open. Returns metadata about the server. This exists separate
// from the base HS2 GetInfo() methods because not all relevant metadata is accessible
// through GetInfo().
struct TPingImpalaHS2ServiceReq {
  1: required TCLIService.TSessionHandle sessionHandle
}

struct TPingImpalaHS2ServiceResp {
  1: required TCLIService.TStatus status

  // The Impala service's version string.
  2: optional string version

  // The Impalad's webserver address.
  3: optional string webserver_address

  // The Impalad's local monotonic time
  4: optional i64 timestamp
}

// CloseImpalaOperation()
//
// Extended version of CloseOperation() that, if the operation was a DML
// operation, returns statistics about the operation.
struct TCloseImpalaOperationReq {
  1: required TCLIService.TOperationHandle operationHandle
}

struct TCloseImpalaOperationResp {
  1: required TCLIService.TStatus status

  // Populated if the operation was a DML operation.
  2: optional TDmlResult dml_result
}

// For all rpc that return a TStatus as part of their result type,
// if the status_code field is set to anything other than OK, the contents
// of the remainder of the result type is undefined (typically not set)
service ImpalaService extends beeswax.BeeswaxService {
  // Cancel execution of query. Returns RUNTIME_ERROR if query_id
  // unknown.
  // This terminates all threads running on behalf of this query at
  // all nodes that were involved in the execution.
  // Throws BeeswaxException if the query handle is invalid (this doesn't
  // necessarily indicate an error: the query might have finished).
  Status.TStatus Cancel(1:beeswax.QueryHandle query_id)
      throws(1:beeswax.BeeswaxException error);

  // Invalidates all catalog metadata, forcing a reload
  // DEPRECATED; execute query "invalidate metadata" to refresh metadata
  Status.TStatus ResetCatalog();

  // Invalidates a specific table's catalog metadata, forcing a reload on the next access
  // DEPRECATED; execute query "refresh <table>" to refresh metadata
  Status.TStatus ResetTable(1:TResetTableReq request)

  // Returns the runtime profile string for the given query handle.
  string GetRuntimeProfile(1:beeswax.QueryHandle query_id)
      throws(1:beeswax.BeeswaxException error);

  // Closes the query handle and return the result summary of the insert.
  TDmlResult CloseInsert(1:beeswax.QueryHandle handle)
      throws(1:beeswax.QueryNotFoundException error, 2:beeswax.BeeswaxException error2);

  // Client calls this RPC to verify that the server is an ImpalaService. Returns the
  // server version.
  TPingImpalaServiceResp PingImpalaService();

  // Returns the summary of the current execution.
  ExecStats.TExecSummary GetExecSummary(1:beeswax.QueryHandle handle)
      throws(1:beeswax.QueryNotFoundException error, 2:beeswax.BeeswaxException error2);
}

// Impala HiveServer2 service

struct TGetExecSummaryReq {
  1: optional TCLIService.TOperationHandle operationHandle

  2: optional TCLIService.TSessionHandle sessionHandle

  // If true, returns the summaries of all query attempts. A TGetExecSummaryResp
  // always returns the profile for the most recent query attempt, regardless of the
  // query id specified. Clients should set this to true if they want to retrieve the
  // summaries of all query attempts (including the failed ones).
  3: optional bool include_query_attempts = false
}

struct TGetExecSummaryResp {
  1: required TCLIService.TStatus status

  2: optional ExecStats.TExecSummary summary

  // A list of all summaries of the failed query attempts.
  3: optional list<ExecStats.TExecSummary> failed_summaries
}

struct TGetRuntimeProfileReq {
  1: optional TCLIService.TOperationHandle operationHandle

  2: optional TCLIService.TSessionHandle sessionHandle

  3: optional RuntimeProfile.TRuntimeProfileFormat format =
      RuntimeProfile.TRuntimeProfileFormat.STRING

  // If true, returns the profiles of all query attempts. A TGetRuntimeProfileResp
  // always returns the profile for the most recent query attempt, regardless of the
  // query id specified. Clients should set this to true if they want to retrieve the
  // profiles of all query attempts (including the failed ones).
  4: optional bool include_query_attempts = false
}

struct TGetRuntimeProfileResp {
  1: required TCLIService.TStatus status

  // Will be set on success if TGetRuntimeProfileReq.format
  // was STRING, BASE64 or JSON.
  2: optional string profile

  // Will be set on success if TGetRuntimeProfileReq.format was THRIFT.
  3: optional RuntimeProfile.TRuntimeProfileTree thrift_profile

  // A list of all the failed query attempts in either STRING, BASE64 or JSON format.
  4: optional list<string> failed_profiles

  // A list of all the failed query attempts in THRIFT format.
  5: optional list<RuntimeProfile.TRuntimeProfileTree> failed_thrift_profiles
}

// ExecutePlannedStatement()
//
// Execute a statement where the ExecRequest has been externally supplied.
// The returned OperationHandle can be used to check on the
// status of the plan, and to fetch results once the
// plan has finished executing.
struct TExecutePlannedStatementReq {
  1: required TCLIService.TExecuteStatementReq statementReq

  // The plan to be executed
  2: required Frontend.TExecRequest plan
}

struct TGetBackendConfigReq {
  1: required TCLIService.TSessionHandle sessionHandle
}

struct TGetBackendConfigResp {
  1: required TCLIService.TStatus status

  2: required BackendGflags.TBackendGflags backend_config
}

struct TGetExecutorMembershipReq {
  1: required TCLIService.TSessionHandle sessionHandle
}

struct TGetExecutorMembershipResp {
  1: required TCLIService.TStatus status

  2: required Frontend.TUpdateExecutorMembershipRequest executor_membership
}

struct TInitQueryContextResp {
  1: required TCLIService.TStatus status

  2: required Query.TQueryCtx query_ctx
}

service ImpalaHiveServer2Service extends TCLIService.TCLIService {
  // Returns the exec summary for the given query. The exec summary is only valid for
  // queries that execute with Impala's backend, i.e. QUERY, DML and COMPUTE_STATS
  // queries. Otherwise a default-initialized TExecSummary is returned for
  // backwards-compatibility with impala-shell - see IMPALA-9729.
  TGetExecSummaryResp GetExecSummary(1:TGetExecSummaryReq req);

  // Returns the runtime profile string for the given query
  TGetRuntimeProfileResp GetRuntimeProfile(1:TGetRuntimeProfileReq req);

  // Client calls this RPC to verify that the server is an ImpalaService. Returns the
  // server version.
  TPingImpalaHS2ServiceResp PingImpalaHS2Service(1:TPingImpalaHS2ServiceReq req);

  // Same as HS2 CloseOperation but can return additional information.
  TCloseImpalaOperationResp CloseImpalaOperation(1:TCloseImpalaOperationReq req);

  // Returns an initialized TQueryCtx. Only supported for the "external fe" service.
  TInitQueryContextResp InitQueryContext();

  // Execute statement with supplied ExecRequest
  TCLIService.TExecuteStatementResp ExecutePlannedStatement(
      1:TExecutePlannedStatementReq req);

  // Returns the current TBackendGflags. Only supported for the "external fe" service.
  TGetBackendConfigResp GetBackendConfig(1:TGetBackendConfigReq req);

  // Returns the executor membership information. Only supported for the "external fe"
  // service.
  TGetExecutorMembershipResp GetExecutorMembership(1:TGetExecutorMembershipReq req);
}
