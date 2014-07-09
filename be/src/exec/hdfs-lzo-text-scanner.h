// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

#ifndef IMPALA_EXEC_HDFS_LZO_TEXT_SCANNER_H
#define IMPALA_EXEC_HDFS_LZO_TEXT_SCANNER_H

#include <common/status.h>
#include <exec/scan-node.h>
#include <exec/hdfs-scanner.h>
#include <exec/hdfs-scan-node.h>
#include <boost/thread/locks.hpp>

namespace impala {

// This is a wrapper for calling the external HdfsLzoTextScanner
// The LZO scanner class is implemented in a dynamically linked library so that
// Impala does not include GPL code.  The two entry points are:
// IssueInitialRanges -- issue calls to the I/O manager to read the file headers
// GetHdfsLzoTextScanner -- returns a pointer to the Scanner object.
class HdfsLzoTextScanner {
 public:
  static HdfsScanner* GetHdfsLzoTextScanner(HdfsScanNode* scan_node, RuntimeState* state);
  static Status IssueInitialRanges(HdfsScanNode* scan_node,
                                   const std::vector<HdfsFileDesc*>& files);

 private:
  // Impala LZO library name -- GPL code.
  const static std::string LIB_IMPALA_LZO;

  // If non-OK, then we have tried and failed to load the LZO library.
  static Status library_load_status_;

  // Lock to protect loading of the lzo file library.
  static boost::mutex lzo_load_lock_;

  // Dynamically linked function to create the Lzo Scanner Object.
  static HdfsScanner* (*CreateLzoTextScanner)
      (HdfsScanNode* scan_node, RuntimeState* state);

  // Dynamically linked function to set the initial scan ranges.
  static Status (*LzoIssueInitialRanges)(
      HdfsScanNode* scan_node, const std::vector<HdfsFileDesc*>& files);

  // Dynamically loads CreateLzoTextScanner and LzoIssueInitialRanges.
  // lzo_load_lock_ should be taken before calling this method.
  static Status LoadLzoLibrary();
};
}
#endif
