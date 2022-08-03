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

#pragma once

#include <boost/thread/pthread/shared_mutex.hpp>

#include "common/status.h"
#include "exec/scan-node.h"
#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node-base.h"

namespace impala {

/// This is a wrapper for calling external implementations of text scanners for
/// compression formats that Impala does not have builtin support for.
/// The plugin scanners are implemented in dynamically linked libraries.
///
/// The two entry points are:
/// IssueInitialRanges -- issue calls to the I/O manager to read the file headers
/// GetHdfsPluginTextScanner -- returns a pointer to the Scanner object.
///
/// Plugin names should all be upper case. If the plugin name is FOO, then the plugin
/// library must be called "libimpalafoo.so" and it must contain the following exported
/// functions:
///   const char* GetImpalaBuildVersion();
///
///   void FooIssueInitialRangesImpl(HdfsScanNodeBase*,
///                                const std::vector<HdfsFileDesc*>&);
///
///   HdfsScanner* CreateFooTextScanner(HdfsScanNodeBase*, RuntimeState*);
///
class HdfsPluginTextScanner {
 public:
  static HdfsScanner* GetHdfsPluginTextScanner(HdfsScanNodeBase* scan_node,
      RuntimeState* state, const std::string& plugin_name);
  static Status IssueInitialRanges(HdfsScanNodeBase* scan_node,
     const std::vector<HdfsFileDesc*>& files, const std::string& plugin_name);

 private:
  // Typedefs for functions loaded from plugin shared objects.
  typedef const char* (*GetPluginImpalaBuildVersionFn)();
  typedef HdfsScanner* (*CreateScannerFn)
      (HdfsScanNodeBase* scan_node, RuntimeState* state);
  typedef Status (*IssueInitialRangesFn)(
      HdfsScanNodeBase* scan_node, const std::vector<HdfsFileDesc*>& files);

  struct LoadedPlugin {
    /// If non-OK, then we have tried and failed to load this plugin.
    Status library_load_status;

    /// Dynamically linked function to create the Scanner Object.
    CreateScannerFn create_scanner_fn = nullptr;

    /// Dynamically linked function to issue the initial scan ranges.
    IssueInitialRangesFn issue_initial_ranges_fn = nullptr;
  };

  /// Lock to protect loading of libraries and 'loaded_plugins_'. We only allow loading a
  /// single library at a time. Must be held in shared mode when accessing
  /// 'loaded_plugins_' and exclusive mode when loading a library.
  static boost::shared_mutex library_load_lock_;

  /// Map from upper case plugin name to the loaded plugin.
  /// Protected by 'library_load_lock_. Entries are never removed once loaded.
  static std::unordered_map<std::string, LoadedPlugin> loaded_plugins_;

  /// Return an error if the specified plugin isn't enabled.
  static Status CheckPluginEnabled(const std::string& plugin_name);

  /// Dynamically loads the required functions for the plugin identified by 'plugin_name'
  /// and populates 'create_scanner_fn' and 'issue_initial_ranges_fn' in 'plugin'.
  /// Returns an error if an error is encountered with loading the library or the
  /// functions. 'library_load_lock_' must be held by the caller in exclusive mode.
  static Status LoadPluginLibrary(const std::string& plugin_name, LoadedPlugin* plugin);
};
}
