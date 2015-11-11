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

//
// This file contains global flags, ie, flags which don't belong to a particular
// component (and would therefore need to be DEFINE'd in every source file containing
// a main()), or flags that are referenced from multiple places and having them here
// calms the linker errors that would otherwise ensue.

#include "common/logging.h"

// This will be defaulted to the host name returned by the OS.
// This name is used in the principal generated for Kerberos authorization.
DEFINE_string(hostname, "", "Hostname to use for this daemon, also used as part of "
              "the Kerberos principal, if enabled. If not set, the system default will be"
              " used");

DEFINE_int32(be_port, 22000, "port on which ImpalaInternalService is exported");

// Kerberos is enabled if and only if principal is set.
DEFINE_string(principal, "", "Kerberos principal. If set, both client and backend network"
    "connections will use Kerberos encryption and authentication.");
DEFINE_string(be_principal, "", "Kerberos principal for backend network connections only,"
    "overriding --principal if set.");
DEFINE_string(keytab_file, "", "Absolute path to Kerberos keytab file");
DEFINE_string(krb5_conf, "", "Absolute path to Kerberos krb5.conf if in a non-standard "
    "location. Does not normally need to be set.");
DEFINE_string(krb5_debug_file, "", "Turn on Kerberos debugging and output to this file");

DEFINE_string(mem_limit, "80%", "Process memory limit specified as number of bytes "
              "('<int>[bB]?'), megabytes ('<float>[mM]'), gigabytes ('<float>[gG]'), "
              "or percentage of the physical memory ('<int>%'). "
              "Defaults to bytes if no unit is given");

DEFINE_bool(enable_process_lifetime_heap_profiling, false, "(Advanced) Enables heap "
    "profiling for the lifetime of the process. Profile output will be stored in the "
    "directory specified by -heap_profile_dir. Enabling this option will disable the "
    "on-demand/remote server profile handlers.");

DEFINE_string(heap_profile_dir, "", "Output directory to store heap profiles. If not set "
    " profiles are stored in the current working directory.");

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

// Stress option for testing failed memory allocation. Debug builds only.
#ifndef NDEBUG
DEFINE_int32(stress_free_pool_alloc, 0, "A stress option which causes memory allocations "
    "to fail once every n allocations where n is the value of this flag. Effective in "
    "debug builds only.");
#endif
