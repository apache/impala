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

DEFINE_string(planservice_host, "localhost", "Host on which planservice is running");
DEFINE_int32(planservice_port, 20000, "Port on which planservice is running");
DEFINE_int32(be_port, 22000, "port on which ImpalaInternalService is exported");

// Kerberos is enabled if and only if principal is set.
DEFINE_string(principal, "", "Kerberos principal name");
DEFINE_string(keytab_file, "", "Absolute path to Kerberos key file");

DEFINE_int64(mem_limit, 0, "Process memory limit in bytes; 0: no limit");
