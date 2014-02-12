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

#ifndef IMPALA_UTIL_CGROUPS_MGR_H
#define IMPALA_UTIL_CGROUPS_MGR_H

#include <string>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include "common/status.h"
#include "util/metrics.h"
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

// Manages the lifecycle of Impala-internal cgroups as well as the assignment of
// execution threads into cgroups.
// To execute queries Impala requests resources from Yarn via the Llama. Yarn returns
// granted resources via the Llama in the form or "RM resource ids" that conventionally
// correspond to a CGroups that the Yarn NM creates. Instead of directly using the
// NM-provided CGroups, Impala creates and manages its own CGroups for the
// following reasons:
// 1. In typical CM/Yarn setups, Impala would not have permissions to write to the tasks
//    file of NM-provided CGroups. It is arguably not even desirable (e.g., for security
//    reasons) for external process to be able to manipulate the permissions of
//    NM-generated CGroups either directly or indirectly.
// 2. Yarn-granted CGroups are created asynchronously (the AM calls to create the
//    CGroups are non-blocking). From Impala's perspective that means that once Impala
//    receives notice from the Llama that resources have been granted, it cannot
//    assume that the corresponding containers have been created (although the Yarn
//    NMs eventually will). While each of Impala's plan fragments could wait for the
//    CGroups to be created, it seems unnecessarily complicated and slow to do so.
// 3. Impala will probably want to manage its own CGroups eventually, e.g., for
//    optimistic query scheduling.
//
// In summary, the typical CGroups-related flow of an Impala query is as follows:
// 1. Impala receives granted resources from Llama and sends out plan fragments
// 2. On each node execution such a fragment, convert the Yarn resource id into
//    a CGroup that Impala should create and assign the query's threads to
// 3. Register the fragment(s) and the CGroup for the query with the
//    node-local CGroup manager. The registration creates the CGroup maintains a
//    count of all fragments using that CGroup.
// 4. Execute the fragments, assigning threads into the Impala-managed CGroup.
// 5. Complete the fragments by unregistering them with the CGroup from the node-local
//    CGroups manager. When the last fragment for a CGroup is unregistered, all threads
//    from that CGroup are relocated into a special staging CGroup, so that the now
//    unused CGroup can safely be deleted (otherwise, we'd have to wait for the OS to
//    drain all entries from the CGroup's tasks file)
class CgroupsMgr {
 public:
  CgroupsMgr(Metrics* metrics);

  // Sets the cgroups mgr's corresponding members and creates the staging cgroup
  // under <cgroups_hierarchy_path>/<staging_cgroup>. Returns a non-OK status if
  // creation of the staging cgroup failed, e.g., because of insufficient privileges.
  Status Init(const std::string& cgroups_hierarchy_path,
      const std::string& staging_cgroup);

  // Returns the cgroup Impala should create and use for enforcing granted resources
  // identified by the given unique ID (which usually corresponds to a query ID). Returns
  // an empty string if unique_id is empty.
  std::string UniqueIdToCgroup(const std::string& unique_id) const;

  // Returns the cgroup CPU shares corresponding to the given number of virtual cores.
  // Returns -1 if v_cpu_cores is <= 0 (which is invalid).
  int32_t VirtualCoresToCpuShares(int16_t v_cpu_cores);

  // Informs the cgroups mgr that a plan fragment intends to use the given cgroup.
  // If this is the first fragment requesting use of cgroup, then the cgroup will
  // be created and *is_first will be set to true (otherwise to false). In any case the
  // reference count active_cgroups_[cgroup] is incremented. Returns a non-OK status
  // if there was an error creating the cgroup.
  Status RegisterFragment(const TUniqueId& fragment_instance_id,
      const std::string& cgroup, bool* is_first);

  // Informs the cgroups mgr that a plan fragment using the given cgroup is complete.
  // Decrements the corresponding reference count active_cgroups_[cgroup]. If the
  // reference count reaches zero this function relocates all thread ids from
  // the cgroup to the staging_cgroup_ and drops cgroup (a cgroup with active thread ids
  // cannot be dropped, so we relocate the thread ids first).
  // Returns a non-OK status there was an error creating the cgroup.
  Status UnregisterFragment(const TUniqueId& fragment_instance_id,
      const std::string& cgroup);

  // Creates a cgroup at <cgroups_hierarchy_path_>/<cgroup>. Returns a non-OK status
  // if the cgroup creation failed, e.g., because of insufficient privileges.
  // If is_not_exists is true then no error is returned if the cgroup already exists.
  Status CreateCgroup(const std::string& cgroup, bool if_not_exists) const;

  // Drops the cgroup at <cgroups_hierarchy_path_>/<cgroup>. Returns a non-OK status
  // if the cgroup deletion failed, e.g., because of insufficient privileges.
  // If if_exists is true then no error is returned if the cgroup does not exist.
  Status DropCgroup(const std::string& cgroup, bool if_exists) const;

  // Sets the number of CPU shares for the given cgroup by writing num_shares into the
  // cgroup's cpu.shares file. Returns a non-OK status if there was an error writing
  // to the file, e.g., because of insufficient privileges.
  Status SetCpuShares(const std::string& cgroup, int32_t num_shares);

  // Assigns a given thread to a cgroup, by writing its thread id to
  // <cgroups_hierarchy_path_>/<cgroup>/tasks. If there is no file at that
  // location, returns an error. Otherwise no attempt is made to check that the
  // target belongs to a cgroup hierarchy due to the cost of reading and parsing
  // cgroup information from the filesystem.
  Status AssignThreadToCgroup(const Thread& thread, const std::string& cgroup) const;

  // Reads the <cgroups_hierarchy_path_>/<src_cgroup>/tasks file and writing all the
  // contained thread ids to <cgroups_hierarchy_path_>/<dst_cgroup>/tasks.
  // Assumes that the destination cgroup has already been created. Returns a non-OK
  // status if there was an error reading src_cgroup and/or writing dst_cgroup.
  Status RelocateThreads(const std::string& src_cgroup,
      const std::string& dst_cgroup) const;

 private:
  // Checks that the cgroups_hierarchy_path_ and the given cgroup under it exists.
  // Returns an error if either of them do not exist.
  // Returns the absolute cgroup path and the absolute path to its tasks file.
  Status GetCgroupPaths(const std::string& cgroup,
      std::string* cgroup_path, std::string* tasks_path) const;

   // Number of currently active Impala-managed cgroups.
   Metrics::PrimitiveMetric<int64_t>* active_cgroups_metric_;

   // Root of the CPU cgroup hierarchy. Created cgroups are placed directly under it.
   std::string cgroups_hierarchy_path_;

   // Cgroup that threads from completed queries are relocated into such that the
   // query's cgroup can be dropped.
   std::string staging_cgroup_;

   // Protects active_cgroups_.
   boost::mutex active_cgroups_lock_;

   // Process-wide map from cgroup to number of fragments using the cgroup.
   // A cgroup can be safely dropped once the number of fragments in the cgroup,
   // according to this map, reaches zero.
   boost::unordered_map<std::string, int32_t> active_cgroups_;
};

}

#endif
