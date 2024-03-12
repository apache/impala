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

#include "exec/tuple-file-writer.h"

#include <boost/filesystem.hpp>

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/env.h"
#include "runtime/mem-tracker.h"
#include "runtime/outbound-row-batch.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/kudu-status-util.h"

#include "common/names.h"

namespace filesystem = boost::filesystem;

namespace impala {

static const char* UNIQUE_PATH_SUFFIX = ".%%%%";

TupleFileWriter::TupleFileWriter(
    std::string path, MemTracker* parent, RuntimeProfile* profile, size_t max_file_size)
  : path_(move(path)),
    temp_suffix_(filesystem::unique_path(UNIQUE_PATH_SUFFIX).string()),
    tracker_(new MemTracker(-1, "TupleFileWriter", parent)),
    write_timer_(profile ? ADD_TIMER(profile, "TupleCacheWriteTime") : nullptr),
    serialize_timer_(profile ? ADD_TIMER(profile, "TupleCacheSerializeTime") : nullptr),
    max_file_size_(max_file_size) {}

TupleFileWriter::~TupleFileWriter() {
  if (state_ != State::Uninitialized) {
    if (tmp_file_ != nullptr) {
      DCHECK_EQ(state_, State::InProgress);
      Abort();
    }

    // The final state is either committed or aborted. In either case, the temporary file
    // should be gone.
    DCHECK(state_ == State::Committed || state_ == State::Aborted);
    DCHECK(!kudu::Env::Default()->FileExists(TempPath()));
  }
  // MemTracker expects an explicit close.
  if (tracker_) tracker_->Close();
}

Status TupleFileWriter::Open(RuntimeState* state) {
  DCHECK_EQ(state_, State::Uninitialized);
  VLOG_FILE << "Tuple Cache: Opening " << TempPath() << " for writing";
  KUDU_RETURN_IF_ERROR(kudu::Env::Default()->NewWritableFile(TempPath(), &tmp_file_),
      "Failed to create tuple cache file");
  RETURN_IF_ERROR(DebugAction(state->query_options(), "TUPLE_FILE_WRITER_OPEN"));
  state_ = State::InProgress;
  return Status::OK();
}

Status TupleFileWriter::Write(RuntimeState* state, RowBatch* row_batch) {
  DCHECK_EQ(state_, State::InProgress);
  SCOPED_TIMER(write_timer_);
  // serialize and write row batch
  OutboundRowBatch out(make_shared<CharMemTrackerAllocator>(tracker_));
  {
    SCOPED_TIMER(serialize_timer_);
    // Passing in nullptr for 'compression_scrtach' disables compression.
    RETURN_IF_ERROR(row_batch->Serialize(&out, /* compression_scratch */ nullptr));
  }

  if (out.header()->num_rows() == 0) {
    DCHECK_EQ(out.header()->uncompressed_size(), 0);
    return Status::OK();
  }

  // Collect all of the pieces that we would want to write, then determine if writing
  // them would exceed the max file size.
  std::string header_buf;
  if (!out.header()->SerializeToString(&header_buf)) {
    return Status(TErrorCode::INTERNAL_ERROR,
        "Could not serialize RowBatchHeaderPB to string");
  }
  size_t header_len = header_buf.size();
  DCHECK_GT(header_len, 0);
  kudu::Slice tuple_data = out.TupleDataAsSlice();
  kudu::Slice tuple_offsets = out.TupleOffsetsAsSlice();
  size_t tuple_data_len = tuple_data.size();
  size_t tuple_offsets_len = tuple_offsets.size();
  DCHECK_GT(tuple_data_len, 0);
  DCHECK_GT(tuple_offsets_len, 0);

  // We write things in this order (sizes first, then the variable-sized data):
  // 1. The size of the header
  // 2. The size of the tuple data
  // 3. The size of the tuple offsets
  // 4. The serialized header
  // 5. The tuple data
  // 6. The tuple offsets
  std::vector<kudu::Slice> slices = {
      kudu::Slice(reinterpret_cast<const char*>(&header_len), sizeof(header_len)),
      kudu::Slice(reinterpret_cast<const char*>(&tuple_data_len),
          sizeof(tuple_data_len)),
      kudu::Slice(reinterpret_cast<const char*>(&tuple_offsets_len),
          sizeof(tuple_offsets_len)),
      kudu::Slice(header_buf),
      kudu::Slice(tuple_data),
      kudu::Slice(tuple_offsets)};

  // Enforce the max file size
  size_t num_bytes_to_write = 0;
  for (auto slice : slices) {
    num_bytes_to_write += slice.size();
  }
  if (BytesWritten() + num_bytes_to_write > max_file_size_) {
    exceeded_max_size_ = true;
    return Status(
        Substitute("Write of size $0 would cause $1 to exceed the maximum file size $2",
            num_bytes_to_write, TempPath(), max_file_size_));
  }

  RETURN_IF_ERROR(DebugAction(state->query_options(), "TUPLE_FILE_WRITER_WRITE"));

  KUDU_RETURN_IF_ERROR(
      tmp_file_->AppendV(kudu::ArrayView<const kudu::Slice>(slices)),
      "Failed to write to cache file");

  return Status::OK();
}

size_t TupleFileWriter::BytesWritten() const {
  DCHECK(tmp_file_ != nullptr);
  return tmp_file_->Size();
}

std::string TupleFileWriter::TempPath() const {
  return path_ + temp_suffix_;
}

void TupleFileWriter::Abort() {
  DCHECK_EQ(state_, State::InProgress);
  DCHECK(tmp_file_ != nullptr);

  kudu::Status status = tmp_file_->Close();
  if (!status.ok()) {
    LOG(WARNING) <<
        Substitute("Failed to close file $0: $1", TempPath(), status.ToString());
  }

  // Delete the file.
  status = kudu::Env::Default()->DeleteFile(TempPath());
  if (!status.ok()) {
    LOG(WARNING) <<
        Substitute("Failed to unlink $0: $1", TempPath(), status.ToString());
  }
  tmp_file_.reset();
  state_ = State::Aborted;
}

Status TupleFileWriter::Commit(RuntimeState* state) {
  DCHECK_EQ(state_, State::InProgress);
  DCHECK(tmp_file_ != nullptr);

  RETURN_IF_ERROR(DebugAction(state->query_options(), "TUPLE_FILE_WRITER_COMMIT"));

  KUDU_RETURN_IF_ERROR(tmp_file_->Sync(), "Failed to sync cache file");
  KUDU_RETURN_IF_ERROR(tmp_file_->Close(), "Failed to close cache file");

  std::string src = TempPath();
  KUDU_RETURN_IF_ERROR(kudu::Env::Default()->RenameFile(src, path_),
      "Failed to rename cache file");
  tmp_file_.reset();
  state_ = State::Committed;
  return Status::OK();
}

std::ostream& operator<<(std::ostream& out, const TupleFileWriter::State& state) {
  switch (state) {
  case TupleFileWriter::State::Uninitialized: out << "Uninitialized"; break;
  case TupleFileWriter::State::InProgress: out << "InProgress"; break;
  case TupleFileWriter::State::Committed: out << "Committed"; break;
  case TupleFileWriter::State::Aborted: out << "Aborted"; break;
  }
  return out;
}

} // namespace impala
