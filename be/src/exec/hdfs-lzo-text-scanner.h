// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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
  static Status IssueInitialRanges(RuntimeState* state,
      HdfsScanNode* scan_node, const std::vector<HdfsFileDesc*>& files);

 private:
  // Impala LZO library name -- GPL code.
  const static std::string LIB_IMPALA_LZO;

  // If true, then we have tried to load the LZO library.
  static bool library_load_attempted_;

  // Lock to protect loading of the lzo file library.
  static boost::mutex lzo_load_lock_;

  // Dynamically linked function to create the Lzo Scanner Object.
  static HdfsScanner* (*CreateLzoTextScanner)
      (HdfsScanNode* scan_node, RuntimeState* state);

  // Dynamically linked function to set the initial scan ranges.
  static void (*LzoIssueInitialRanges)(
      HdfsScanNode* scan_node, const std::vector<HdfsFileDesc*>& files);
};
}
#endif
