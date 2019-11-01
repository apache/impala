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

//
// This file contains global flags, ie, flags which don't belong to a particular
// component (and would therefore need to be DEFINE'd in every source file containing
// a main()), or flags that are referenced from multiple places and having them here
// calms the linker errors that would otherwise ensue.

#include <string>

#include "common/constant-strings.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

// This will be defaulted to the host name returned by the OS.
// This name is used in the principal generated for Kerberos authorization.
DEFINE_string(hostname, "", "Hostname to use for this daemon, also used as part of "
              "the Kerberos principal, if enabled. If not set, the system default will be"
              " used");

DEFINE_int32(be_port, 22000,
    "port on which thrift based ImpalaInternalService is exported");
DEFINE_int32(krpc_port, 27000,
    "port on which KRPC based ImpalaInternalService is exported");

// Kerberos is enabled if and only if principal is set.
DEFINE_string(principal, "", "Kerberos principal. If set, both client and backend "
    "network connections will use Kerberos encryption and authentication. Kerberos will "
    "not be used for internal or external connections if this is not set.");
DEFINE_string(be_principal, "", "Kerberos principal for backend network connections only,"
    "overriding --principal if set. Must not be set if --principal is not set.");
DEFINE_string(keytab_file, "", "Absolute path to Kerberos keytab file");
DEFINE_string(krb5_ccname, "/tmp/krb5cc_impala_internal", "Absolute path to the file "
    "based credentials cache that we pass to the KRB5CCNAME environment variable.");
DEFINE_string(krb5_conf, "", "Absolute path to Kerberos krb5.conf if in a non-standard "
    "location. Does not normally need to be set.");
DEFINE_string(krb5_debug_file, "", "Turn on Kerberos debugging and output to this file");

static const string mem_limit_help_msg = "Limit on process memory consumption. "
    "Includes the JVM's memory consumption only if --mem_limit_includes_jvm is true. "
    + Substitute(MEM_UNITS_HELP_MSG, "the physical memory");
DEFINE_string(mem_limit, "80%",  mem_limit_help_msg.c_str());

DEFINE_bool(mem_limit_includes_jvm, false,
    "If true, --mem_limit will include the JVM's max heap size and committed memory in "
    "the process memory limit.");

static const string buffer_pool_limit_help_msg = "(Advanced) Limit on buffer pool size. "
     + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit (minus the JVM heap if "
       "--mem_limit_includes_jvm is true)") + " "
    "The default value and behaviour of this flag may change between releases.";
DEFINE_string(buffer_pool_limit, "85%", buffer_pool_limit_help_msg.c_str());

static const string buffer_pool_clean_pages_limit_help_msg = "(Advanced) Limit on bytes "
    "of clean pages that will be accumulated in the buffer pool. "
     + Substitute(MEM_UNITS_HELP_MSG, "the buffer pool limit") + ".";
DEFINE_string(buffer_pool_clean_pages_limit, "10%",
    buffer_pool_clean_pages_limit_help_msg.c_str());

DEFINE_int64(min_buffer_size, 8 * 1024,
    "(Advanced) The minimum buffer size to use in the buffer pool");

DEFINE_bool(enable_process_lifetime_heap_profiling, false, "(Advanced) Enables heap "
    "profiling for the lifetime of the process. Profile output will be stored in the "
    "directory specified by -heap_profile_dir. Enabling this option will disable the "
    "on-demand/remote server profile handlers.");

DEFINE_string(heap_profile_dir, "", "Output directory to store heap profiles. If not set "
    "profiles are stored in the current working directory.");

DEFINE_int64(tcmalloc_max_total_thread_cache_bytes, 0, "(Advanced) Bound on the total "
    "amount of bytes allocated to TCMalloc thread caches. If left at 0 (default), use "
    "the default value in TCMalloc library.");

DEFINE_bool(abort_on_config_error, true, "Abort Impala startup if there are improper "
    "configs or running on unsupported hardware.");

DEFINE_bool(compact_catalog_topic, true, "If true, catalog updates sent via the "
    "statestore are compacted before transmission. This saves network bandwidth at the"
    " cost of a small quantity of CPU time. Enable this option in cluster with large"
    " catalogs. It must be enabled on both the catalog service, and all Impala demons.");

DEFINE_string(redaction_rules_file, "", "Absolute path to sensitive data redaction "
    "rules. The rules will be applied to all log messages and query text shown in the "
    "Web UI and audit records. Query results will not be affected. Refer to the "
    "documentation for the rule file format.");

DEFINE_bool(enable_minidumps, true, "Whether to enable minidump generation upon process "
    "crash or SIGUSR1.");

DEFINE_string(minidump_path, "minidumps", "Directory to write minidump files to. This "
    "can be either an absolute path or a path relative to log_dir. Each daemon will "
    "create an additional sub-directory to prevent naming conflicts and to make it "
    "easier to identify a crashing daemon. Minidump files contain crash-related "
    "information in a compressed format and will be written when a daemon exits "
    "unexpectedly, for example on an unhandled exception or signal. It is also possible "
    "to create minidumps on demand without exiting the process by sending SIGUSR1. "
    "Set to empty to disable writing minidump files.");

DEFINE_int32(max_minidumps, 9, "Maximum number of minidump files to keep per daemon. "
    "Older files are removed first. Set to 0 to keep all minidump files.");

DEFINE_int32(minidump_size_limit_hint_kb, 20480, "Size limit hint for minidump files in "
    "KB. If a minidump exceeds this value, then breakpad will reduce the stack memory it "
    "collects for each thread from 8KB to 2KB. However it will always include the full "
    "stack memory for the first 20 threads, including the thread that crashed.");

DEFINE_bool(load_auth_to_local_rules, false, "If true, load auth_to_local configuration "
    "from hdfs' core-site.xml. When enabled, impalad reads the rules from the property "
    "hadoop.security.auth_to_local and applies them to translate the Kerberos principal "
    "to its corresponding local user name for authorization.");

// Stress options that are only enabled in debug builds for testing.
#ifndef NDEBUG
DEFINE_int32(stress_fn_ctx_alloc, 0, "A stress option which causes memory allocations "
    "in function contexts to fail once every n allocations where n is the value of this "
    "flag. Effective in debug builds only.");
DEFINE_int32(stress_datastream_recvr_delay_ms, 0, "A stress option that causes data "
    "stream receiver registration to be delayed. Effective in debug builds only.");
DEFINE_bool(skip_file_runtime_filtering, false, "Skips file-based runtime filtering for"
    "testing purposes. Effective in debug builds only.");
DEFINE_int32(fault_injection_rpc_exception_type, 0, "A fault injection option that "
    "specifies the exception to be thrown in the caller side of an RPC call. Effective "
    "in debug builds only");
DEFINE_int32(stress_scratch_write_delay_ms, 0, "A stress option which causes writes to "
    "scratch files to be to be delayed to simulate slow writes.");
DEFINE_bool(thread_creation_fault_injection, false, "A fault injection option that "
    " causes calls to Thread::Create() to fail randomly 1% of the time on eligible "
    " codepaths. Effective in debug builds only.");
DEFINE_int32(stress_catalog_init_delay_ms, 0, "A stress option that injects extra delay"
    " in milliseconds when initializing an impalad's local catalog replica. Delay <= 0"
    " inject no delay.");
DEFINE_int32(stress_disk_read_delay_ms, 0, "A stress option that injects extra delay"
    " in milliseconds when the I/O manager is reading from disk.");
#endif

DEFINE_string(debug_actions, "", "For testing only. Uses the same format as the debug "
    "action query options, but allows for injection of debug actions in code paths where "
    "query options are not available.");

// Used for testing the path where the Kudu client is stubbed.
DEFINE_bool(disable_kudu, false, "If true, Kudu features will be disabled.");

// Timeout (ms) used in the FE for admin and metadata operations (set on the KuduClient),
// and in the BE for scans and writes (set on the KuduScanner and KuduSession
// accordingly).
DEFINE_int32(kudu_operation_timeout_ms, 3 * 60 * 1000, "Timeout (milliseconds) set for "
    "all Kudu operations. This must be a positive value, and there is no way to disable "
    "timeouts.");

#ifdef SLOW_BUILD
static const int32 default_kudu_client_rpc_timeout_ms = 60000;
#else
static const int32 default_kudu_client_rpc_timeout_ms = 0;
#endif

// Timeout (ms) for Kudu rpcs set in the BE on the KuduClient.
DEFINE_int32(kudu_client_rpc_timeout_ms, default_kudu_client_rpc_timeout_ms,
    "(Advanced) Timeout (milliseconds) set for individual Kudu client rpcs. An operation "
    "may consist of several rpcs, so this is expected to be less than "
    "kudu_operation_timeout_ms. This must be a positive value or it will be ignored and "
    "Kudu's default of 10s will be used. There is no way to disable timeouts.");

DEFINE_int64(inc_stats_size_limit_bytes, 200 * (1LL<<20), "Maximum size of "
    "incremental stats the catalog is allowed to serialize per table. "
    "This limit is set as a safety check, to prevent the JVM from "
    "hitting a maximum array limit of 1GB (or OOM) while building "
    "the thrift objects to send to impalads. By default, it's set to 200MB");

DEFINE_bool(enable_stats_extrapolation, false,
    "If true, uses table statistics computed with COMPUTE STATS "
    "to extrapolate the row counts of partitions.");

DEFINE_string(log_filename, "",
    "Prefix of log filename - "
    "full path is <log_dir>/<log_filename>.[INFO|WARN|ERROR|FATAL]");
DEFINE_bool(redirect_stdout_stderr, true,
    "If true, redirects stdout/stderr to INFO/ERROR log.");
DEFINE_int32(max_log_files, 10, "Maximum number of log files to retain per severity "
    "level. The most recent log files are retained. If set to 0, all log files are "
    "retained.");

// The read size is the preferred size of the reads issued to HDFS or the local FS.
// There is a trade off of latency and throughput, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
DEFINE_int32(read_size, 8 * 1024 * 1024, "(Advanced) The preferred I/O request size in "
    "bytes to issue to HDFS or the local filesystem. Increasing the read size will "
    "increase memory requirements. Decreasing the read size may decrease I/O "
    "throughput.");

DEFINE_string(reserved_words_version, "3.0.0", "Reserved words compatibility version. "
    "Reserved words cannot be used as identifiers in SQL. This flag determines the impala"
    " version from which the reserved word list is taken. The value must be one of "
    "[\"2.11.0\", \"3.0.0\"].");

DEFINE_bool_hidden(disable_catalog_data_ops_debug_only, false,
    "Disable catalog operations that require access to file-system data blocks. "
    "Examples are when catalog reads data blocks to load avro schemas and copy jars."
    "Use only for testing/debugging, not in deployed clusters.");

// TODO: this flag and others, since it requires multiple daemons to be set the
// same way, is error prone. One fix for this flag is to set it only on
// catalogd, propagate the setting as a property of the Catalog object, and let
// impalad uses act on this setting.
DEFINE_int32(invalidate_tables_timeout_s, 0, "If a table has not been referenced in a "
    "SQL statement for more than the configured amount of time, the catalog server will "
    "automatically evict its cached metadata about this table. This has the same effect "
    "as a user-initiated \"INVALIDATE METADATA\" statement on the table. Configuring "
    "this to 0 disables time-based automatic invalidation of tables. This is independent "
    "from memory-based invalidation configured by invalidate_tables_on_memory_pressure. "
    "To enable this feature, a non-zero flag must be applied to both catalogd and "
    "impalad.");

DEFINE_bool(invalidate_tables_on_memory_pressure, false, "Configure catalogd to "
    "invalidate recently unused tables when the old GC generation is almost full. This "
    "is independent from time-based invalidation configured by "
    "invalidate_table_timeout_s. To enable this feature, a true flag must be applied to "
    "both catalogd and impalad.");

DEFINE_int32(hms_event_polling_interval_s, 0,
    "Configure catalogd to invalidate cached table metadata based on metastore events. "
    "These metastore events could be generated by external systems like Apache Hive or "
    "a different Impala cluster using the same Hive metastore server as this one. "
    "A non-zero value of this flag sets the polling interval of catalogd in seconds to "
    "fetch new metastore events. A value of zero disables this feature. When enabled, "
    "this flag has the same effect as \"INVALIDATE METADATA\" statement on the table "
    "for certain metastore event types. Additionally, in case of events which detect "
    "creation or removal of objects from metastore, catalogd adds or removes such "
    "objects from its cached metadata. This feature is independent of time and memory "
    "based automatic invalidation of tables. Note that this is still an experimental "
    "feature and not recommended to be deployed on production systems until it is "
    "made generally available.");

DEFINE_string(blacklisted_dbs, "sys,information_schema",
    "Comma separated list for blacklisted databases. Configure which databases to be "
    "skipped for loading (in startup and global INVALIDATE METADATA). Users can't access,"
    " create, or drop databases which are blacklisted.");
DEFINE_string(blacklisted_tables, "",
    "Comma separated full names (in format: <db>.<table>) of blacklisted tables. "
    "Configure which tables to be skipped for loading (in startup and reseting metadata "
    "of the table). Users can't access, create, or drop tables which are blacklisted");

DEFINE_double_hidden(invalidate_tables_gc_old_gen_full_threshold, 0.6, "The threshold "
    "above which CatalogdTableInvalidator would consider the old generation to be almost "
    "full and trigger an invalidation on recently unused tables");

DEFINE_double_hidden(invalidate_tables_fraction_on_memory_pressure, 0.1,
    "The fraction of tables to invalidate when CatalogdTableInvalidator considers the "
    "old GC generation to be almost full.");

DEFINE_bool_hidden(unlock_mt_dop, false,
    "(Experimental) If true, allow specifying mt_dop for all queries.");

DEFINE_bool_hidden(mt_dop_auto_fallback, false,
    "(Experimental) If true, fall back to non-mt_dop if mt_dop query option is set and "
    "a query does not support it. Has no effect if --unlock_mt_dop is true.");

DEFINE_bool_hidden(recursively_list_partitions, true,
    "If true, recursively list the content of partition directories.");

DEFINE_bool(unlock_zorder_sort, false,
    "(Experimental) If true, enables using ZORDER option for SORT BY.");

DEFINE_bool(simplify_check_on_show_tables, false,
    "If true, only check SELECT privilege on SHOW TABLES or GET_TABLES when enabling "
    "authorization. If false, all privileges will be checked for visibility of a table. "
    "A table will show up if the user has any privileges on it. This flag is used to "
    "improve SHOW TABLES performance when using Sentry and have thousands of candidate "
    "tables to be checked. No performance gain is found in using Ranger");

// Set the slow RPC threshold to 2 minutes to avoid false positives (since TransmitData
// RPCs can take some time to process).
DEFINE_int64(impala_slow_rpc_threshold_ms, 2 * 60 * 1000,
    "(Advanced) Threshold for considering Impala internal RPCs to be unusually slow. "
    "Slow RPCs trigger additional logging and other diagnostics. Lowering this value "
    "may result in false positives"
    "This overrides KRPC's --rpc_duration_too_long_ms setting.");

// ++========================++
// || Startup flag graveyard ||
// ++========================++
//
//                       -----------
//           -----------/   R I P   ╲
//          /   R I P   ╲ -----------|-----------
//          |-----------|           |/   R I P   ╲
//          |           |   LLAMA   ||-----------|
//          | Old Aggs  |           ||           |
//          |           |    --     || Old Joins |
//          |    --     |           ||           |
//          |           |           ||    --     |
//          |           |~.~~.~~.~~~~|           |
//          ~~.~~.~~.~~~~            |           |
//                                   ~~.~~.~~.~~~~
// The flags have no effect but we don't want to prevent Impala from starting when they
// are provided on the command line after an upgrade. We issue a warning if the flag is
// set from the command line.
#define REMOVED_FLAG(flagname) \
  DEFINE_string_hidden(flagname, "__UNSET__", "Removed"); \
  DEFINE_validator(flagname, [](const char* name, const string& val) { \
      if (val != "__UNSET__") LOG(WARNING) << "Ignoring removed flag " << name; \
      return true; \
    });

REMOVED_FLAG(abfs_read_chunk_size);
REMOVED_FLAG(adls_read_chunk_size);
REMOVED_FLAG(authorization_policy_file);
REMOVED_FLAG(be_service_threads);
REMOVED_FLAG(cgroup_hierarchy_path);
REMOVED_FLAG(disable_admission_control);
REMOVED_FLAG(disable_mem_pools);
REMOVED_FLAG(enable_accept_queue_server);
REMOVED_FLAG(enable_partitioned_aggregation);
REMOVED_FLAG(enable_partitioned_hash_join);
REMOVED_FLAG(enable_phj_probe_side_filtering);
REMOVED_FLAG(enable_rm);
REMOVED_FLAG(kerberos_reinit_interval);
REMOVED_FLAG(llama_addresses);
REMOVED_FLAG(llama_callback_port);
REMOVED_FLAG(llama_host);
REMOVED_FLAG(llama_max_request_attempts);
REMOVED_FLAG(llama_port);
REMOVED_FLAG(llama_registration_timeout_secs);
REMOVED_FLAG(llama_registration_wait_secs);
REMOVED_FLAG(local_nodemanager_url);
REMOVED_FLAG(max_free_io_buffers);
REMOVED_FLAG(pull_incremental_statistics);
REMOVED_FLAG(report_status_retry_interval_ms);
REMOVED_FLAG(resource_broker_cnxn_attempts);
REMOVED_FLAG(resource_broker_cnxn_retry_interval_ms);
REMOVED_FLAG(resource_broker_recv_timeout);
REMOVED_FLAG(resource_broker_send_timeout);
REMOVED_FLAG(rm_always_use_defaults);
REMOVED_FLAG(rm_default_cpu_vcores);
REMOVED_FLAG(rm_default_memory);
REMOVED_FLAG(rpc_cnxn_attempts);
REMOVED_FLAG(rpc_cnxn_retry_interval_ms);
REMOVED_FLAG(skip_lzo_version_check);
REMOVED_FLAG(staging_cgroup);
REMOVED_FLAG(status_report_interval);
REMOVED_FLAG(status_report_max_retries);
REMOVED_FLAG(suppress_unknown_disk_id_warnings);
REMOVED_FLAG(use_krpc);
REMOVED_FLAG(use_kudu_kinit);
REMOVED_FLAG(use_statestore);
