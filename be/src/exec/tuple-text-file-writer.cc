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

#include "exec/tuple-text-file-writer.h"

#include "common/logging.h"
#include "common/names.h"
#include "kudu/util/env.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"

namespace impala {

TupleTextFileWriter::TupleTextFileWriter(std::string path)
  : path_(std::move(path)), bytes_written_(0) {}

TupleTextFileWriter::~TupleTextFileWriter() {
  if (writer_.is_open()) writer_.close();
}

Status TupleTextFileWriter::Open() {
  VLOG_FILE << "Tuple Cache Debug: Opening " << path_ << " for text writing";
  writer_.open(path_, std::ios::out | std::ios::binary | std::ios::trunc);
  if (!writer_.is_open() || writer_.fail()) {
    return Status(TErrorCode::DISK_IO_ERROR,
        "Open tuple text writer on " + path_ + " failed", GetStrErrMsg());
  }
  return Status::OK();
}

Status TupleTextFileWriter::Write(RowBatch* row_batch) {
  FOREACH_ROW(row_batch, 0, build_batch_iter) {
    TupleRow* build_row = build_batch_iter.Get();
    DCHECK(build_row != nullptr);
    // Convert to the human-readable representation of the row.
    std::string row_str = PrintRow(build_row, *(row_batch->row_desc())) + '\n';
    writer_ << row_str;
    bytes_written_ += row_str.size();
  }
  return Status::OK();
}

void TupleTextFileWriter::Delete() {
  if (writer_.is_open()) writer_.close();
  // Delete the file directly using the actual path.
  kudu::Status status = kudu::Env::Default()->DeleteFile(path_);
  if (!status.ok()) {
    LOG(WARNING) << Substitute("Failed to delete $0: $1", path_, status.ToString());
  }
}

void TupleTextFileWriter::Commit() {
  DCHECK(writer_.is_open());
  writer_.close();
  if (writer_.fail()) {
    // An error occurred while writing or closing the file.
    string err_msg = GetStrErrMsg();
    LOG(WARNING) << "Failed to flush " << path_ << ": " << err_msg;
    return;
  }
  LOG(INFO) << "Tuple Cache Debug: Commit completed successfully for " << path_;
}

bool TupleTextFileWriter::IsEmpty() const {
  return BytesWritten() == 0;
}
} // namespace impala
