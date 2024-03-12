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

#include "exec/tuple-file-reader.h"

#include "gutil/strings/substitute.h"
#include "kudu/util/array_view.h"
#include "kudu/util/env.h"
#include "kudu/util/slice.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/kudu-status-util.h"

#include "common/names.h"

namespace impala {

TupleFileReader::TupleFileReader(
    const std::string& path, MemTracker* parent, RuntimeProfile* profile)
  : path_(path),
    tracker_(new MemTracker(-1, "TupleFileReader", parent)),
    read_timer_(profile ? ADD_TIMER(profile, "TupleCacheReadTime") : nullptr),
    deserialize_timer_(
        profile ? ADD_TIMER(profile, "TupleCacheDeserializeTime") : nullptr) {}

TupleFileReader::~TupleFileReader() {
  // MemTracker expects an explicit close.
  tracker_->Close();
}

Status TupleFileReader::Open(RuntimeState *state) {
  kudu::RWFileOptions opts;
  opts.mode = kudu::Env::OpenMode::MUST_EXIST;
  KUDU_RETURN_IF_ERROR(
        kudu::Env::Default()->NewRWFile(opts, path_, &reader_),
        "Failed to open tuple cache file");
  // Get the file size
  KUDU_RETURN_IF_ERROR(reader_->Size(&file_size_),
      "Failed to get size for the tuple cache file");
  RETURN_IF_ERROR(DebugAction(state->query_options(), "TUPLE_FILE_READER_OPEN"));
  return Status::OK();
}

Status TupleFileReader::GetNext(RuntimeState *state,
    BufferPool::ClientHandle* bpclient, RowBatch* output_row_batch, bool* eos) {
  if (offset_ == file_size_) {
    *eos = true;
    return Status::OK();
  }
  // Only the first rowbatch starts at the zero offset of the file.
  // We use this for injecting errors for testing purposes.
  bool first_rowbatch = offset_ == 0;
  *eos = false;
  SCOPED_TIMER(read_timer_);
  // Each block starts with the sizes of the variable chunks of data:
  // 1. The header size
  // 2. The tuple data size
  // 3. The tuple offsets size
  size_t header_len;
  size_t tuple_data_len;
  size_t tuple_offsets_len;

  vector<kudu::Slice> chunk_lens_slices = {
      kudu::Slice(reinterpret_cast<const char*>(&header_len), sizeof(header_len)),
      kudu::Slice(reinterpret_cast<const char*>(&tuple_data_len),
          sizeof(tuple_data_len)),
      kudu::Slice(reinterpret_cast<const char*>(&tuple_offsets_len),
          sizeof(tuple_offsets_len))};

  KUDU_RETURN_IF_ERROR(reader_->ReadV(offset_,
      kudu::ArrayView<kudu::Slice>(chunk_lens_slices)),
      "Failed to read cache file");

  if (header_len == 0 || tuple_data_len == 0 || tuple_offsets_len == 0) {
    string err_msg = Substitute("Invalid data lengths at offset $0 in $1: "
        "header_len=$2, tuple_data_len=$3, tuple_offsets_len=$4", offset_, path_,
        header_len, tuple_data_len, tuple_offsets_len);
    DCHECK(false) << err_msg;
    return Status(Substitute("Invalid tuple cache file: $0", err_msg));
  }
  offset_ += sizeof(header_len) + sizeof(tuple_data_len) + sizeof(tuple_offsets_len);

  // Now, we know the total size of the variable-length data, and we can read
  // it in a single chunk.
  size_t varlen_size = header_len + tuple_data_len + tuple_offsets_len;

  // Sanity check: The varlen_size shouldn't be larger than the rest of the file.
  // This protects us from doing very large memory allocations if any of the lengths
  // are bogus.
  if (offset_ + varlen_size > file_size_) {
    string err_msg = Substitute("Invalid data lengths at offset $0 in $1 exceed "
        "file size $2: header_len=$3, tuple_data_len=$4, tuple_offsets_len=$5",
        offset_, path_, file_size_, header_len, tuple_data_len, tuple_offsets_len);
    DCHECK(false) << err_msg;
    return Status(Substitute("Invalid tuple cache file: $0", err_msg));
  }
  std::unique_ptr<char []> varlen_data(new char[varlen_size]);

  kudu::Slice header_slice = kudu::Slice(varlen_data.get(), header_len);
  kudu::Slice tuple_data_slice =
      kudu::Slice(varlen_data.get() + header_len, tuple_data_len);
  kudu::Slice tuple_offsets_slice =
      kudu::Slice(varlen_data.get() + header_len + tuple_data_len, tuple_offsets_len);
  std::vector<kudu::Slice> varlen_data_slices =
    { header_slice, tuple_data_slice, tuple_offsets_slice };

  KUDU_RETURN_IF_ERROR(reader_->ReadV(offset_,
      kudu::ArrayView<kudu::Slice>(varlen_data_slices)),
      "Failed to read tuple cache file");
  offset_ += varlen_size;

  RowBatchHeaderPB header;
  // The header is at the start of the varlen data
  if (!header.ParseFromArray(header_slice.data(), header_slice.size())) {
    return Status(TErrorCode::INTERNAL_ERROR,
        "Could not deserialize RowBatchHeaderPB from disk cache");
  }

  std::unique_ptr<RowBatch> row_batch;
  {
    SCOPED_TIMER(deserialize_timer_);
    RETURN_IF_ERROR(RowBatch::FromProtobuf(output_row_batch->row_desc(), header,
        tuple_offsets_slice, tuple_data_slice, tracker_.get(), bpclient, &row_batch));
  }

  DCHECK_EQ(output_row_batch->num_rows(), 0);
  if (output_row_batch->capacity() < row_batch->num_rows()) {
    string err_msg =
        Substitute("Too many rows ($0) for the output row batch (capacity $1)",
            row_batch->num_rows(), output_row_batch->capacity());
    DCHECK(false) << err_msg;
    return Status(Substitute("Invalid tuple cache file: $0", err_msg));
  }

  // For testing, we inject an error as late as possible in this function. Nothing
  // after this point returns not-OK status. If we can recover from this point,
  // we can recover from previous points. There are two different cases: either
  // we want an error on the first row batch produced or we want an error on
  // a subsequent row batch.
  if (first_rowbatch) {
    RETURN_IF_ERROR(
        DebugAction(state->query_options(), "TUPLE_FILE_READER_FIRST_GETNEXT"));
  } else {
    RETURN_IF_ERROR(
        DebugAction(state->query_options(), "TUPLE_FILE_READER_SECOND_GETNEXT"));
  }
  // Set eos after any possibility of a not-OK status
  if (offset_ == file_size_) {
    *eos = true;
  }
  output_row_batch->AddRows(row_batch->num_rows());
  for (int row = 0; row < row_batch->num_rows(); row++) {
    TupleRow* src = row_batch->GetRow(row);
    TupleRow* dest = output_row_batch->GetRow(row);
    // if the input row is shorter than the output row, make sure not to leave
    // uninitialized Tuple* around
    output_row_batch->ClearRow(dest);
    // this works as expected if rows from input_batch form a prefix of
    // rows in output_batch
    row_batch->CopyRow(src, dest);
  }
  output_row_batch->CommitRows(row_batch->num_rows());
  row_batch->TransferResourceOwnership(output_row_batch);

  return Status::OK();
}

} // namespace impala
