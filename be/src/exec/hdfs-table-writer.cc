// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-table-writer.h"

using namespace std;

namespace impala {

HdfsTableWriter::HdfsTableWriter(OutputPartition* output,
                                 const HdfsPartitionDescriptor* partition,
                                 const HdfsTableDescriptor* table_desc,
                                 const vector<Expr*>& output_exprs)
  : output_(output),
    table_desc_(table_desc),
    output_exprs_(output_exprs) {
}

Status HdfsTableWriter::Write(const uint8_t* data, int32_t len) {
  int ret = hdfsWrite(output_->hdfs_connection, output_->tmp_hdfs_file, data, len);
  if (ret == -1) {
    stringstream msg;
    msg << "Failed to write row (length: " << len
        << " to Hdfs file: " << output_->tmp_hdfs_file_name;
    return Status(AppendHdfsErrorMessage(msg.str()));
  }
  return Status::OK;
}
}
