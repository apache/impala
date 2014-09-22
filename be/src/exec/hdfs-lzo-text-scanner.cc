// Copyright (c) 2012 Cloudera, Inc.
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

#include "exec/hdfs-lzo-text-scanner.h"

#include <hdfs.h>
#include <boost/algorithm/string.hpp>
#include "common/version.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "runtime/runtime-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/dynamic-util.h"

using namespace boost;
using namespace boost::algorithm;
using namespace impala;
using namespace std;

DEFINE_bool(skip_lzo_version_check, false, "Disables checking the LZO library version "
            "against the running Impala version.");

const string HdfsLzoTextScanner::LIB_IMPALA_LZO = "libimpalalzo.so";

namespace impala {
Status HdfsLzoTextScanner::library_load_status_;

mutex HdfsLzoTextScanner::lzo_load_lock_;

const char* (*GetImpalaBuildVersion)();

HdfsScanner* (*HdfsLzoTextScanner::CreateLzoTextScanner)(
    HdfsScanNode* scan_node, RuntimeState* state);

Status (*HdfsLzoTextScanner::LzoIssueInitialRanges)(
    HdfsScanNode* scan_node, const std::vector<HdfsFileDesc*>& files);

HdfsScanner* HdfsLzoTextScanner::GetHdfsLzoTextScanner(
    HdfsScanNode* scan_node, RuntimeState* state) {

  // If the scanner was not loaded then no scans could be issued so we should
  // never get here without having loaded the scanner.
  DCHECK(CreateLzoTextScanner != NULL);

  return (*CreateLzoTextScanner)(scan_node, state);
}

Status HdfsLzoTextScanner::IssueInitialRanges(HdfsScanNode* scan_node,
    const vector<HdfsFileDesc*>& files) {
  if (LzoIssueInitialRanges == NULL) {
    lock_guard<mutex> l(lzo_load_lock_);
    if (library_load_status_.ok()) {
      // LzoIssueInitialRanges && library_load_status_.ok() means we haven't tried loading
      // the library yet.
      library_load_status_ = LoadLzoLibrary();
      if (!library_load_status_.ok()) {
        stringstream ss;
        ss << "Error loading impala-lzo library. Check that the impala-lzo library "
           << "is at version " << IMPALA_BUILD_VERSION;
        library_load_status_.AddErrorMsg(ss.str());
        return library_load_status_;
      }
    } else {
      // We only try to load the library once.
      return library_load_status_;
    }
  }

  return (*LzoIssueInitialRanges)(scan_node, files);
}

Status HdfsLzoTextScanner::LoadLzoLibrary() {
  void* handle;
  RETURN_IF_ERROR(DynamicOpen(LIB_IMPALA_LZO.c_str(), &handle));
  RETURN_IF_ERROR(DynamicLookup(handle,
      "GetImpalaBuildVersion", reinterpret_cast<void**>(&GetImpalaBuildVersion)));

  if (strcmp((*GetImpalaBuildVersion)(), IMPALA_BUILD_VERSION) != 0) {
    stringstream ss;
    ss << "Impala LZO library was built against Impala version "
       << (*GetImpalaBuildVersion)() << ", but the running Impala version is "
       << IMPALA_BUILD_VERSION;
    if (FLAGS_skip_lzo_version_check) {
      LOG(ERROR) << ss.str();
    } else {
      return Status(ss.str());
    }
  }

  RETURN_IF_ERROR(DynamicLookup(handle,
      "CreateLzoTextScanner", reinterpret_cast<void**>(&CreateLzoTextScanner)));
  RETURN_IF_ERROR(DynamicLookup(handle,
      "LzoIssueInitialRangesImpl", reinterpret_cast<void**>(&LzoIssueInitialRanges)));

  DCHECK(CreateLzoTextScanner != NULL);
  DCHECK(LzoIssueInitialRanges != NULL);
  LOG(INFO) << "Loaded impala-lzo library: " << LIB_IMPALA_LZO;
  return Status::OK;
}

}
