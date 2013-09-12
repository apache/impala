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

#ifndef IMPALA_UTIL_CGROUPS_H
#define IMPALA_UTIL_CGROUPS_H

#include <string>
#include "common/status.h"
#include "util/thread.h"

namespace impala {

// Control Groups, or 'cgroups', are a Linux-specific mechanism for arbitrating resources
// amongst threads.
//
// CGroups are organised in a forest of 'hierarchies', each of which are mounted at a path
// in the filesystem. Each hierarchy contains one or more cgroups, arranged
// hierarchically. Each hierarchy has one or more 'subsystems' attached. Each subsystem
// represents a resource to manage, so for example there is a CPU subsystem and a MEMORY
// subsystem. There are rules about when subsystems may be attached to more than one
// hierarchy, which are out of scope of this description.
//
// Each thread running on a kernel with cgroups enabled belongs to exactly one cgroup in
// every hierarchy at once. Impala is only concerned with a single hierarchy that assigns
// CPU resources in the first instance. Threads are assigned to cgroups by writing their
// thread ID to a file in the special cgroup filesystem.
//
// For more information:
// access.redhat.com/site/documentation/en-US/Red_Hat_Enterprise_Linux/6/html/Resource_Management_Guide/
// www.kernel.org/doc/Documentation/cgroups/cgroups.txt

// Assigns a given thread to a cgroup, by writing its thread id to
// <prefix>/<cgroup>/tasks. If there is no file at that location, returns an
// error. Otherwise no attempt is made to check that the target belongs to a cgroup
// hierarchy due to the cost of reading and parsing cgroup information from the
// filesystem.
Status AssignThreadToCgroup(const Thread& thread, const std::string& prefix,
    const std::string& cgroup);

}

#endif
