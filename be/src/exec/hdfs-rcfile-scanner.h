// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_HDFS_RCFILE_SCANNER_H_
#define IMPALA_EXEC_HDFS_RCFILE_SCANNER_H_

#include "exec/hdfs-scanner.h"
#include "exec/rcfile-reader.h"

namespace impala {

class HdfsScanNode;
class TupleDescriptor;
class Tuple;

// A scanner for reading RCFiles into tuples. Relies upn RCFileReader
// to do the heavy lifting.
class HdfsRCFileScanner : public HdfsScanner {
 public:
  HdfsRCFileScanner(HdfsScanNode* scan_node, const TupleDescriptor* tuple_desc,
                    Tuple* template_tuple, MemPool* tuple_pool);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual Status Prepare(RuntimeState* state, ByteStream* byte_stream);
  virtual Status InitCurrentScanRange(RuntimeState* state, HdfsScanRange* scan_range,
                                      ByteStream* byte_stream);

  void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  // Helper class for converting text to other types;
  boost::scoped_ptr<TextConverter> text_converter_;

  // RCFileReader used for reading the list of RCFiles
  boost::scoped_ptr<RCFileReader> rc_reader_;

  // The current row group
  boost::scoped_ptr<RCFileRowGroup> row_group_;

  // Maps column idx to a boolean indicating whether or not the column
  // needs to be read. Created in Prepare() and used to initialize
  // RCFileReader. TODO: clarify comment - what's the index here?
  std::vector<bool> column_idx_read_mask_;

  // Guard variable to prevent reading again from RCFileReader when
  // the file is exhausted but not all rows have been materialised
  // from the row group. Reset to false with every
  // InitCurrentScanRange.
  bool scan_range_fully_buffered_;
};

}

#endif
