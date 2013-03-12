// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <hdfs.h>
#include <dlfcn.h>
#include <boost/algorithm/string.hpp>
#include "exec/hdfs-lzo-text-scanner.h"
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

const string HdfsLzoTextScanner::LIB_IMPALA_LZO = "libimpalalzo.so";

namespace impala {
bool HdfsLzoTextScanner::library_load_attempted_;

mutex HdfsLzoTextScanner::lzo_load_lock_;

HdfsScanner* (*HdfsLzoTextScanner::CreateLzoTextScanner)(
    HdfsScanNode* scan_node, RuntimeState* state);
void (*HdfsLzoTextScanner::LzoIssueInitialRanges)(
    HdfsScanNode* scan_node, const std::vector<HdfsFileDesc*>& files);

HdfsScanner* HdfsLzoTextScanner::GetHdfsLzoTextScanner(
    HdfsScanNode* scan_node, RuntimeState* state) {

  // If the scanner was not loaded then no scans could be issued so we should
  // never get here without having loaded the scanner.
  DCHECK(CreateLzoTextScanner != NULL);

  return (*CreateLzoTextScanner)(scan_node, state);
}

Status HdfsLzoTextScanner::IssueInitialRanges(RuntimeState* state,
    HdfsScanNode* scan_node, const vector<HdfsFileDesc*>& files) {
  if (LzoIssueInitialRanges == NULL) {
    lock_guard<mutex> l(lzo_load_lock_);
    if (!library_load_attempted_) {
      library_load_attempted_ = true;
      void* handle;
      RETURN_IF_ERROR(DynamicOpen(state, LIB_IMPALA_LZO, RTLD_NOW, &handle));
      RETURN_IF_ERROR(DynamicLookup(state, handle,
          "CreateLzoTextScanner", reinterpret_cast<void**>(&CreateLzoTextScanner)));
      RETURN_IF_ERROR(DynamicLookup(state, handle,
          "IssueInitialRanges", reinterpret_cast<void**>(&LzoIssueInitialRanges))); 
      LOG(INFO) << "Loaded impala-lzo library: " << LIB_IMPALA_LZO;
    }
  }
  // We only try to load the library once.  Just return status to
  // avoid flooding the log.
  if (LzoIssueInitialRanges == NULL) return Status("Lzo scanner load error");
  (*LzoIssueInitialRanges)(scan_node, files);
  return Status::OK;
}
}
