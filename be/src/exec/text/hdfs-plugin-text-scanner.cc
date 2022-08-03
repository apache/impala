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

#include "exec/text/hdfs-plugin-text-scanner.h"

#include <algorithm>

#include <hdfs.h>
#include <boost/algorithm/string.hpp>
#include "common/version.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/debug-util.h"
#include "util/dynamic-util.h"
#include "util/hdfs-util.h"
#include "util/string-util.h"

#include "common/names.h"

using namespace impala;

using boost::algorithm::to_lower_copy;
using boost::shared_lock;
using boost::shared_mutex;
using boost::upgrade_lock;
using boost::upgrade_to_unique_lock;
using std::find;

// LZO is no longer supported, so there are no plugins enabled by default. This is
// likely to be removed.
DEFINE_string(enabled_hdfs_text_scanner_plugins, "", "(Advanced) whitelist of HDFS "
    "text scanner plugins that Impala will try to dynamically load. Must be a "
    "comma-separated list of upper-case compression codec names. Each plugin implements "
    "support for decompression and hands off the decompressed bytes to Impala's builtin "
    "text parser for further processing (e.g. parsing delimited text).");

static const string LIB_IMPALA_TEMPLATE = "libimpala$0.so";

namespace impala {

shared_mutex HdfsPluginTextScanner::library_load_lock_;

std::unordered_map<string, HdfsPluginTextScanner::LoadedPlugin>
    HdfsPluginTextScanner::loaded_plugins_;

HdfsScanner* HdfsPluginTextScanner::GetHdfsPluginTextScanner(
    HdfsScanNodeBase* scan_node, RuntimeState* state, const string& plugin_name) {
  CreateScannerFn create_scanner_fn;
  {
    shared_lock<shared_mutex> l(library_load_lock_);
    // If the scanner was not loaded then no scans could be issued so we should
    // never get here without having loaded the scanner.
    auto it = loaded_plugins_.find(plugin_name);
    DCHECK(it != loaded_plugins_.end());
    create_scanner_fn = it->second.create_scanner_fn;
  }
  return create_scanner_fn(scan_node, state);
}

Status HdfsPluginTextScanner::IssueInitialRanges(HdfsScanNodeBase* scan_node,
    const vector<HdfsFileDesc*>& files, const string& plugin_name) {
  DCHECK(!files.empty());
  IssueInitialRangesFn issue_initial_ranges_fn;
  RETURN_IF_ERROR(CheckPluginEnabled(plugin_name));
  {
    upgrade_lock<shared_mutex> read_lock(library_load_lock_);
    auto it = loaded_plugins_.find(plugin_name);
    if (it == loaded_plugins_.end()) {
      // We haven't tried loading the library yet.
      upgrade_to_unique_lock<shared_mutex> write_lock(read_lock);
      it = loaded_plugins_.insert(make_pair(plugin_name, LoadedPlugin())).first;
      it->second.library_load_status = LoadPluginLibrary(plugin_name, &it->second);
      if (!it->second.library_load_status.ok()) {
        it->second.library_load_status.AddDetail(Substitute(
              "Error loading plugin library for $0. Check that the library is at "
              "version $1", plugin_name, GetDaemonBuildVersion()));
        return it->second.library_load_status;
      }
    } else {
      // We only try to load the library once - propagate the error if it previously
      // failed.
      RETURN_IF_ERROR(it->second.library_load_status);
    }
    issue_initial_ranges_fn = it->second.issue_initial_ranges_fn;
  }

  return issue_initial_ranges_fn(scan_node, files);
}

Status HdfsPluginTextScanner::CheckPluginEnabled(const string& plugin_name) {
  if (!CommaSeparatedContains(FLAGS_enabled_hdfs_text_scanner_plugins, plugin_name)) {
    return Status(Substitute("Scanner plugin '$0' is not one of the enabled plugins: '$1'",
          plugin_name, FLAGS_enabled_hdfs_text_scanner_plugins));
  }
  return Status::OK();
}

Status HdfsPluginTextScanner::LoadPluginLibrary(const string& plugin_name,
    LoadedPlugin* plugin) {
  RETURN_IF_ERROR(CheckPluginEnabled(plugin_name));
  GetPluginImpalaBuildVersionFn get_plugin_impala_build_version;
  void* handle;
  string lib_name = Substitute(LIB_IMPALA_TEMPLATE, to_lower_copy(plugin_name));
  RETURN_IF_ERROR(DynamicOpen(lib_name.c_str(), &handle));
  RETURN_IF_ERROR(DynamicLookup(handle, "GetImpalaBuildVersion",
      reinterpret_cast<void**>(&get_plugin_impala_build_version)));
  if (strcmp(get_plugin_impala_build_version(), GetDaemonBuildVersion()) != 0) {
    return Status(Substitute(
        "Scanner plugin $0 was built against Impala version $1 but the running Impala "
        "version is $2", plugin_name, get_plugin_impala_build_version(),
        GetDaemonBuildVersion()));
  }

  // Camel case the library name to generate correct symbol, e.g. "CreateFooTextScanner".
  string plugin_camelcase = to_lower_copy(plugin_name);
  plugin_camelcase[0] = toupper(plugin_camelcase[0]);
  string create_symbol = Substitute("Create$0TextScanner", plugin_camelcase);
  string issue_initial_ranges_symbol =
        Substitute("$0IssueInitialRangesImpl", plugin_camelcase);
  RETURN_IF_ERROR(DynamicLookup(handle, create_symbol.c_str(),
        reinterpret_cast<void**>(&plugin->create_scanner_fn)));
  RETURN_IF_ERROR(DynamicLookup(handle, issue_initial_ranges_symbol.c_str(),
        reinterpret_cast<void**>(&plugin->issue_initial_ranges_fn)));

  DCHECK(plugin->create_scanner_fn != nullptr);
  DCHECK(plugin->issue_initial_ranges_fn != nullptr);
  LOG(INFO) << "Loaded plugin library for " << plugin_name << ": " << lib_name;
  return Status::OK();
}

}
