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
DEFINE_int32_hidden(krpc_port, 27000,
    "port on which KRPC based ImpalaInternalService is exported");

// Kerberos is enabled if and only if principal is set.
DEFINE_string(principal, "", "Kerberos principal. If set, both client and backend "
    "network connections will use Kerberos encryption and authentication. Kerberos will "
    "not be used for internal or external connections if this is not set.");
DEFINE_string(be_principal, "", "Kerberos principal for backend network connections only,"
    "overriding --principal if set. Must not be set if --principal is not set.");
DEFINE_string(keytab_file, "", "Absolute path to Kerberos keytab file");
DEFINE_string(krb5_conf, "", "Absolute path to Kerberos krb5.conf if in a non-standard "
    "location. Does not normally need to be set.");
DEFINE_string(krb5_debug_file, "", "Turn on Kerberos debugging and output to this file");

static const string mem_limit_help_msg = "Limit on process memory consumption, "
    "excluding the JVM's memory consumption. "
    + Substitute(MEM_UNITS_HELP_MSG, "the physical memory");
DEFINE_string(mem_limit, "80%",  mem_limit_help_msg.c_str());

static const string buffer_pool_limit_help_msg = "(Advanced) Limit on buffer pool size. "
     + Substitute(MEM_UNITS_HELP_MSG, "the process memory limit") + " "
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

DEFINE_bool(disable_mem_pools, false, "Set to true to disable memory pooling. "
    "This can be used to help diagnose memory corruption issues.");

DEFINE_bool(compact_catalog_topic, false, "If true, catalog updates sent via the "
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
DEFINE_int32(fault_injection_rpc_delay_ms, 0, "A fault injection option that causes "
    "rpc server handling to be delayed to trigger an RPC timeout on the caller side. "
    "Effective in debug builds only.");
DEFINE_int32(fault_injection_rpc_type, 0, "A fault injection option that specifies "
    "which rpc call will be injected with the delay. Effective in debug builds only.");
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
#endif

// Used for testing the path where the Kudu client is stubbed.
DEFINE_bool(disable_kudu, false, "If true, Kudu features will be disabled.");

// Timeout (ms) used in the FE for admin and metadata operations (set on the KuduClient),
// and in the BE for scans and writes (set on the KuduScanner and KuduSession
// accordingly).
DEFINE_int32(kudu_operation_timeout_ms, 3 * 60 * 1000, "Timeout (milliseconds) set for "
    "all Kudu operations. This must be a positive value, and there is no way to disable "
    "timeouts.");

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

REMOVED_FLAG(be_service_threads);
REMOVED_FLAG(cgroup_hierarchy_path);
REMOVED_FLAG(enable_accept_queue_server);
REMOVED_FLAG(enable_partitioned_aggregation);
REMOVED_FLAG(enable_partitioned_hash_join);
REMOVED_FLAG(enable_phj_probe_side_filtering);
REMOVED_FLAG(enable_rm);
REMOVED_FLAG(llama_addresses);
REMOVED_FLAG(llama_callback_port);
REMOVED_FLAG(llama_host);
REMOVED_FLAG(llama_max_request_attempts);
REMOVED_FLAG(llama_port);
REMOVED_FLAG(llama_registration_timeout_secs);
REMOVED_FLAG(llama_registration_wait_secs);
REMOVED_FLAG(local_nodemanager_url);
REMOVED_FLAG(resource_broker_cnxn_attempts);
REMOVED_FLAG(resource_broker_cnxn_retry_interval_ms);
REMOVED_FLAG(resource_broker_recv_timeout);
REMOVED_FLAG(resource_broker_send_timeout);
REMOVED_FLAG(rm_always_use_defaults);
REMOVED_FLAG(rm_default_cpu_vcores);
REMOVED_FLAG(rm_default_memory);
REMOVED_FLAG(rpc_cnxn_attempts);
REMOVED_FLAG(rpc_cnxn_retry_interval_ms);
REMOVED_FLAG(staging_cgroup);
REMOVED_FLAG(suppress_unknown_disk_id_warnings);
REMOVED_FLAG(use_statestore);
