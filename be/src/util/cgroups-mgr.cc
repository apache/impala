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

#include "util/cgroups-mgr.h"

#include <fstream>
#include <sstream>
#include <boost/filesystem.hpp>

using namespace impala;
using namespace std;
using namespace boost::filesystem;
using namespace boost;

namespace impala {

const string CgroupsMgr::IMPALA_CGROUP_SUFFIX = "_impala";
mutex CgroupsMgr::active_cgroups_lock_;
unordered_map<std::string, int32_t> CgroupsMgr::active_cgroups_;

CgroupsMgr::CgroupsMgr(Metrics* metrics) {
  active_cgroups_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric<int64_t>(
          "cgroups-mgr-active-cgroups", 0);
}

Status CgroupsMgr::Init(const std:: string cgroups_hierarchy_path,
      const std::string staging_cgroup) {
  cgroups_hierarchy_path_ = cgroups_hierarchy_path;
  staging_cgroup_ = staging_cgroup;
  // Set up the staging cgroup for Impala to retire execution threads into.
  RETURN_IF_ERROR(CreateCgroup(staging_cgroup, true));
  return Status::OK;
}

string CgroupsMgr::ResourceIdToCgroup(const string& rm_resource_id) const {
  if (rm_resource_id.empty()) return "";
  return rm_resource_id + IMPALA_CGROUP_SUFFIX;
}

Status CgroupsMgr::CreateCgroup(const string& cgroup, bool if_not_exists) const {
  stringstream cgroup_path_ss;
  cgroup_path_ss << cgroups_hierarchy_path_ << "/" << cgroup;
  string cgroup_path = cgroup_path_ss.str();
  try {
    // Returns false if the dir already exists, otherwise throws an exception.
    if (!create_directory(cgroup_path) && !if_not_exists) {
      stringstream err_msg;
      err_msg << "Failed to create CGroup at path " << cgroup_path
              << ". Path already exists.";
      return Status(err_msg.str());
    }
    LOG(INFO) << "Created CGroup " << cgroup_path;
  } catch (std::exception& e) {
    stringstream err_msg;
    err_msg << "Failed to create CGroup at path " << cgroup_path << ". " << e.what();
    return Status(err_msg.str());
  }
  return Status::OK;
}

Status CgroupsMgr::DropCgroup(const string& cgroup, bool if_exists) const {
  stringstream cgroup_path_ss;
  cgroup_path_ss << cgroups_hierarchy_path_ << "/" << cgroup;
  string cgroup_path = cgroup_path_ss.str();

  LOG(INFO) << "Dropping CGroup " << cgroups_hierarchy_path_ << " " << cgroup;
  try {
    if(!remove(cgroup_path) && !if_exists) {
      stringstream err_msg;
      err_msg << "Failed to create CGroup at path " << cgroup_path
              << ". Path does not exist.";
      return Status(err_msg.str());
    }
  } catch (std::exception& e) {
    stringstream err_msg;
    err_msg << "Failed to drop CGroup at path " << cgroup_path << ". " << e.what();
    return Status(err_msg.str());
  }
  return Status::OK;
}

Status CgroupsMgr::GetCgroupPaths(const std::string& cgroup,
    std::string* cgroup_path, std::string* tasks_path) const {
  stringstream cgroup_path_ss;
  cgroup_path_ss << cgroups_hierarchy_path_ << "/" << cgroup;
  *cgroup_path = cgroup_path_ss.str();
  if (!exists(*cgroup_path)) {
    stringstream err_msg;
    err_msg << "CGroup " << *cgroup_path << " does not exist";
    return Status(err_msg.str());
  }

  stringstream tasks_path_ss;
  tasks_path_ss << *cgroup_path << "/tasks";
  *tasks_path = tasks_path_ss.str();
  if (!exists(*tasks_path)) {
    stringstream err_msg;
    err_msg << "CGroup " << *cgroup_path << " does not have a /tasks file";
    return Status(err_msg.str());
  }
  return Status::OK;
}

Status CgroupsMgr::AssignThreadToCgroup(const Thread& thread,
    const string& cgroup) const {
  string cgroup_path;
  string tasks_path;
  RETURN_IF_ERROR(GetCgroupPaths(cgroup, &cgroup_path, &tasks_path));

  ofstream tasks(tasks_path.c_str(), ios::out | ios::app);
  if (!tasks.is_open()) {
    stringstream err_msg;
    err_msg << "CGroup tasks file: " << tasks_path << " is not writable by Impala";
    return Status(err_msg.str());
  }
  tasks << thread.tid() << endl;

  VLOG_ROW << "Thread " << thread.tid() << " moved to CGroup " << cgroup_path;
  tasks.close();
  return Status::OK;
}

Status CgroupsMgr::RelocateThreads(const string& src_cgroup,
    const string& dst_cgroup) const {
  string src_cgroup_path;
  string src_tasks_path;
  RETURN_IF_ERROR(GetCgroupPaths(src_cgroup, &src_cgroup_path, &src_tasks_path));

  string dst_cgroup_path;
  string dst_tasks_path;
  RETURN_IF_ERROR(GetCgroupPaths(dst_cgroup, &dst_cgroup_path, &dst_tasks_path));

  ifstream src_tasks(src_tasks_path.c_str());
  if (!src_tasks) {
    stringstream err_msg;
    err_msg << "Failed to open source CGroup tasks file at: " << src_tasks_path;
    return Status(err_msg.str());
  }

  ofstream dst_tasks(dst_tasks_path.c_str(), ios::out | ios::app);
  if (!dst_tasks) {
    stringstream err_msg;
    err_msg << "Failed to open destination CGroup tasks file at: " << dst_tasks_path;
    return Status(err_msg.str());
  }

  int32_t tid;
  while (src_tasks >> tid) {
    dst_tasks << tid << endl;
    // Attempting to write a non-existent tid/pid will result in an error,
    // so clear the error flags after every append.
    dst_tasks.clear();
    VLOG_ROW << "Relocating thread id " << tid << " from " << src_tasks_path
             << " to " << dst_tasks_path;
  }

  return Status::OK;
}

Status CgroupsMgr::RegisterFragment(const string& cgroup) {
  if (cgroup.empty() || cgroups_hierarchy_path_.empty()) return Status::OK;

  LOG(INFO) << "Registering fragment with CGroup "
            << cgroups_hierarchy_path_ << "/" << cgroup;
  lock_guard<mutex> l(active_cgroups_lock_);
  unordered_map<string, int32_t>::iterator entry = active_cgroups_.find(cgroup);
  if (entry == active_cgroups_.end()) {
    active_cgroups_[cgroup] = 1;
    RETURN_IF_ERROR(CreateCgroup(cgroup, false));
    active_cgroups_metric_->Increment(1);
  } else {
    int32_t* ref_count = &entry->second;
    ++(*ref_count);
  }
  return Status::OK;
}

Status CgroupsMgr::UnregisterFragment(const string& cgroup) {
  if (cgroup.empty() || cgroups_hierarchy_path_.empty()) return Status::OK;

  LOG(INFO) << "Unregistering fragment from CGroup "
            << cgroups_hierarchy_path_ << " " << cgroup;
  lock_guard<mutex> l(active_cgroups_lock_);
  unordered_map<string, int32_t>::iterator entry = active_cgroups_.find(cgroup);
  if (entry == active_cgroups_.end()) {
    stringstream err_msg;
    err_msg << "No active fragments for CGroup " << cgroup;
    return Status(err_msg.str());
  }

  int32_t* ref_count = &entry->second;
  --(*ref_count);
  if (*ref_count == 0) {
    RETURN_IF_ERROR(RelocateThreads(cgroup, staging_cgroup_));
    RETURN_IF_ERROR(DropCgroup(cgroup, false));
    active_cgroups_metric_->Increment(-1);
    active_cgroups_.erase(entry);
  }
  return Status::OK;
}

}
