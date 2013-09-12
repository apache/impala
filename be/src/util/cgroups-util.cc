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

#include "util/cgroups-util.h"

#include <fstream>
#include <sstream>
#include <boost/filesystem.hpp>

using namespace impala;
using namespace std;
using namespace boost::filesystem;
using namespace boost;

Status impala::AssignThreadToCgroup(const Thread& thread, const string& prefix,
    const string& cgroup) {
  stringstream cgroup_path_ss;
  cgroup_path_ss << prefix << "/" << cgroup;
  string cgroup_path = cgroup_path_ss.str();
  if (!exists(cgroup_path)) {
    stringstream err_msg;
    err_msg << "CGroup " << cgroup_path << " does not exist";
    return Status(err_msg.str());
  }

  stringstream tasks_path;
  tasks_path << cgroup_path << "/tasks";
  if (!exists(tasks_path.str())) {
    stringstream err_msg;
    err_msg << "CGroup " << cgroup_path << " does not have a /tasks file";
    return Status(err_msg.str());
  }

  ofstream tasks(tasks_path.str().c_str(), ios::out | ios::app);
  if (!tasks.is_open()) {
    stringstream err_msg;
    err_msg << "CGroup tasks file: " << tasks_path.str() << " is not writable by Impala";
    return Status(err_msg.str());
  }
  tasks << thread.tid() << endl;

  VLOG_ROW << "Thread " << thread.tid() << " moved to cgroup " << cgroup_path;
  tasks.close();
  return Status::OK;
}
