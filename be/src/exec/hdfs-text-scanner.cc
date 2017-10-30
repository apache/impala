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

#include "exec/hdfs-text-scanner.h"

#include <memory>

#include "codegen/llvm-codegen.h"
#include "exec/delimited-text-parser.h"
#include "exec/delimited-text-parser.inline.h"
#include "exec/hdfs-lzo-text-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.h"
#include "exec/text-converter.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/codec.h"
#include "util/decompress.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"

#include "common/names.h"

using boost::algorithm::ends_with;
using boost::algorithm::to_lower;
using namespace impala;
using namespace llvm;
using namespace strings;

const char* HdfsTextScanner::LLVM_CLASS_NAME = "class.impala::HdfsTextScanner";

// Suffix for lzo index file: hdfs-filename.index
const string HdfsTextScanner::LZO_INDEX_SUFFIX = ".index";

// Number of bytes to read when the previous attempt to streaming decompress did not make
// progress.
const int64_t COMPRESSED_DATA_FIXED_READ_SIZE = 1 * 1024 * 1024;

HdfsTextScanner::HdfsTextScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
    : HdfsScanner(scan_node, state),
      byte_buffer_ptr_(nullptr),
      byte_buffer_end_(nullptr),
      byte_buffer_read_size_(0),
      only_parsing_header_(false),
      scan_state_(CONSTRUCTED),
      boundary_pool_(new MemPool(scan_node->mem_tracker())),
      boundary_row_(boundary_pool_.get()),
      boundary_column_(boundary_pool_.get()),
      slot_idx_(0),
      batch_start_ptr_(nullptr),
      error_in_row_(false),
      partial_tuple_(nullptr),
      parse_delimiter_timer_(nullptr) {
}

HdfsTextScanner::~HdfsTextScanner() {
}

Status HdfsTextScanner::IssueInitialRanges(HdfsScanNodeBase* scan_node,
    const vector<HdfsFileDesc*>& files) {
  vector<DiskIoMgr::ScanRange*> compressed_text_scan_ranges;
  int compressed_text_files = 0;
  vector<HdfsFileDesc*> lzo_text_files;
  for (int i = 0; i < files.size(); ++i) {
    THdfsCompression::type compression = files[i]->file_compression;
    switch (compression) {
      case THdfsCompression::NONE:
        // For uncompressed text we just issue all ranges at once.
        // TODO: Lz4 is splittable, should be treated similarly.
        RETURN_IF_ERROR(scan_node->AddDiskIoRanges(files[i]));
        break;

      case THdfsCompression::GZIP:
      case THdfsCompression::SNAPPY:
      case THdfsCompression::SNAPPY_BLOCKED:
      case THdfsCompression::BZIP2:
        ++compressed_text_files;
        for (int j = 0; j < files[i]->splits.size(); ++j) {
          // In order to decompress gzip-, snappy- and bzip2-compressed text files, we
          // need to read entire files. Only read a file if we're assigned the first split
          // to avoid reading multi-block files with multiple scanners.
          DiskIoMgr::ScanRange* split = files[i]->splits[j];

          // We only process the split that starts at offset 0.
          if (split->offset() != 0) {
            // We are expecting each file to be one hdfs block (so all the scan range
            // offsets should be 0).  This is not incorrect but we will issue a warning.
            scan_node->runtime_state()->LogError(ErrorMsg(
                TErrorCode::COMPRESSED_FILE_MULTIPLE_BLOCKS,
                files[i]->filename, split->offset()));
            // We assign the entire file to one scan range, so mark all but one split
            // (i.e. the first split) as complete.
            scan_node->RangeComplete(THdfsFileFormat::TEXT, compression);
            continue;
          }

          // Populate the list of compressed text scan ranges.
          DCHECK_GT(files[i]->file_length, 0);
          ScanRangeMetadata* metadata =
              static_cast<ScanRangeMetadata*>(split->meta_data());
          DiskIoMgr::ScanRange* file_range = scan_node->AllocateScanRange(files[i]->fs,
              files[i]->filename.c_str(), files[i]->file_length, 0,
              metadata->partition_id, split->disk_id(), split->expected_local(),
              DiskIoMgr::BufferOpts(split->try_cache(), files[i]->mtime));
          compressed_text_scan_ranges.push_back(file_range);
          scan_node->max_compressed_text_file_length()->Set(files[i]->file_length);
        }
        break;

      case THdfsCompression::LZO:
        // lzo-compressed text need to be processed by the specialized HdfsLzoTextScanner.
        // Note that any LZO_INDEX files (no matter what the case of their suffix) will be
        // filtered by the planner.
        {
        #ifndef NDEBUG
          // No straightforward way to do this in one line inside a DCHECK, so for once
          // we'll explicitly use NDEBUG to avoid executing debug-only code.
          string lower_filename = files[i]->filename;
          to_lower(lower_filename);
          DCHECK(!ends_with(lower_filename, LZO_INDEX_SUFFIX));
        #endif
          lzo_text_files.push_back(files[i]);
        }
        break;

      default:
        DCHECK(false);
    }
  }
  RETURN_IF_ERROR(scan_node->AddDiskIoRanges(compressed_text_scan_ranges,
          compressed_text_files));
  if (lzo_text_files.size() > 0) {
    // This will dlopen the lzo binary and can fail if the lzo binary is not present.
    RETURN_IF_ERROR(HdfsLzoTextScanner::IssueInitialRanges(scan_node, lzo_text_files));
  }

  return Status::OK();
}

void HdfsTextScanner::Close(RowBatch* row_batch) {
  DCHECK(!is_closed_);
  // Need to close the decompressor before transferring the remaining resources to
  // 'row_batch' because in some cases there is memory allocated in the decompressor_'s
  // temp_memory_pool_.
  if (decompressor_ != nullptr) {
    decompressor_->Close();
    decompressor_.reset();
  }
  boundary_pool_->FreeAll();
  if (row_batch != nullptr) {
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    row_batch->tuple_data_pool()->AcquireData(data_buffer_pool_.get(), false);
    context_->ReleaseCompletedResources(row_batch, true);
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
          unique_ptr<RowBatch>(row_batch));
    }
  } else {
    template_tuple_pool_->FreeAll();
    data_buffer_pool_->FreeAll();
    context_->ReleaseCompletedResources(nullptr, true);
  }

  // Verify all resources (if any) have been transferred or freed.
  DCHECK_EQ(template_tuple_pool_.get()->total_allocated_bytes(), 0);
  DCHECK_EQ(data_buffer_pool_.get()->total_allocated_bytes(), 0);
  DCHECK_EQ(boundary_pool_.get()->total_allocated_bytes(), 0);
  DCHECK_EQ(context_->num_completed_io_buffers(), 0);
  if (!only_parsing_header_) {
    scan_node_->RangeComplete(THdfsFileFormat::TEXT,
        stream_->file_desc()->file_compression);
  }
  CloseInternal();
}

Status HdfsTextScanner::InitNewRange() {
  DCHECK_EQ(scan_state_, CONSTRUCTED);
  // Compressed text does not reference data in the io buffers directly. In such case, we
  // can recycle the buffers in the stream_ more promptly.
  if (stream_->file_desc()->file_compression != THdfsCompression::NONE) {
    stream_->set_contains_tuple_data(false);
  }

  // Update the decompressor based on the compression type of the file in the context.
  DCHECK(stream_->file_desc()->file_compression != THdfsCompression::SNAPPY)
      << "FE should have generated SNAPPY_BLOCKED instead.";
  RETURN_IF_ERROR(UpdateDecompressor(stream_->file_desc()->file_compression));

  HdfsPartitionDescriptor* hdfs_partition = context_->partition_descriptor();
  char field_delim = hdfs_partition->field_delim();
  char collection_delim = hdfs_partition->collection_delim();
  if (scan_node_->materialized_slots().size() == 0) {
    field_delim = '\0';
    collection_delim = '\0';
  }

  delimited_text_parser_.reset(new DelimitedTextParser(
      scan_node_->hdfs_table()->num_cols(), scan_node_->num_partition_keys(),
      scan_node_->is_materialized_col(), hdfs_partition->line_delim(),
      field_delim, collection_delim, hdfs_partition->escape_char()));
  text_converter_.reset(new TextConverter(hdfs_partition->escape_char(),
      scan_node_->hdfs_table()->null_column_value(), true,
      state_->strict_mode()));

  RETURN_IF_ERROR(ResetScanner());
  scan_state_ = SCAN_RANGE_INITIALIZED;
  return Status::OK();
}

Status HdfsTextScanner::ResetScanner() {
  // Assumes that N partition keys occupy entries 0 through N-1 in materialized_slots_.
  // If this changes, we will need another layer of indirection to map text-file column
  // indexes to their destination slot.
  slot_idx_ = 0;

  error_in_row_ = false;
  boundary_column_.Clear();
  boundary_row_.Clear();
  delimited_text_parser_->ParserReset();
  byte_buffer_ptr_ = byte_buffer_end_ = nullptr;
  partial_tuple_ = nullptr;

  // Initialize codegen fn
  RETURN_IF_ERROR(InitializeWriteTuplesFn(
      context_->partition_descriptor(), THdfsFileFormat::TEXT, "HdfsTextScanner"));
  return Status::OK();
}

Status HdfsTextScanner::FinishScanRange(RowBatch* row_batch) {
  DCHECK(!row_batch->AtCapacity());
  DCHECK_EQ(byte_buffer_ptr_, byte_buffer_end_);

  bool split_delimiter;
  RETURN_IF_ERROR(CheckForSplitDelimiter(&split_delimiter));
  if (split_delimiter) {
    // If the scan range ends on the '\r' of a "\r\n", the next tuple is considered part
    // of the next scan range. Nothing to do since we already fully parsed the previous
    // tuple.
    DCHECK(!delimited_text_parser_->HasUnfinishedTuple());
    DCHECK(partial_tuple_ == nullptr);
    DCHECK(boundary_column_.IsEmpty());
    DCHECK(boundary_row_.IsEmpty());
    scan_state_ = DONE;
    return Status::OK();
  }

  // For text we always need to scan past the scan range to find the next delimiter
  while (true) {
    DCHECK_EQ(scan_state_, PAST_SCAN_RANGE);
    bool eosr = true;
    Status status = Status::OK();
    byte_buffer_read_size_ = 0;

    // If compressed text, then there is nothing more to be read.
    // TODO: calling FillByteBuffer() at eof() can cause
    // ScannerContext::Stream::GetNextBuffer to DCHECK. Fix this.
    if (decompressor_.get() == nullptr && !stream_->eof()) {
      status = FillByteBuffer(row_batch->tuple_data_pool(), &eosr, NEXT_BLOCK_READ_SIZE);
    }

    if (!status.ok() || byte_buffer_read_size_ == 0) {
      if (status.IsCancelled()) return status;

      if (!status.ok()) {
        stringstream ss;
        ss << "Read failed while trying to finish scan range: " << stream_->filename()
           << ":" << stream_->file_offset() << endl << status.GetDetail();
        RETURN_IF_ERROR(state_->LogOrReturnError(
            ErrorMsg(TErrorCode::GENERAL, ss.str())));
      } else if (partial_tuple_ != nullptr || !boundary_column_.IsEmpty() ||
          !boundary_row_.IsEmpty() ||
          (delimited_text_parser_->HasUnfinishedTuple() &&
              (!scan_node_->materialized_slots().empty() ||
                  scan_node_->num_materialized_partition_keys() > 0))) {
        // There is data in the partial column because there is a missing row delimiter
        // at the end of the file. Copy the data into a new string buffer that gets
        // memory from the row batch pool, so that the boundary pool could be freed.
        StringBuffer sb(row_batch->tuple_data_pool());
        RETURN_IF_ERROR(sb.Append(boundary_column_.buffer(), boundary_column_.len()));
        boundary_column_.Clear();
        char* col = sb.buffer();
        int num_fields = 0;
        RETURN_IF_ERROR(delimited_text_parser_->FillColumns<true>(sb.len(),
            &col, &num_fields, field_locations_.data()));

        TupleRow* tuple_row_mem = row_batch->GetRow(row_batch->AddRow());
        int max_tuples = row_batch->capacity() - row_batch->num_rows();
        DCHECK_GE(max_tuples, 1);
        // Set variables for proper error outputting on boundary tuple
        batch_start_ptr_ = boundary_row_.buffer();
        row_end_locations_[0] = batch_start_ptr_ + boundary_row_.len();
        int num_tuples =
            WriteFields(num_fields, 1, row_batch->tuple_data_pool(), tuple_row_mem);
        DCHECK_LE(num_tuples, 1);
        DCHECK_GE(num_tuples, 0);
        COUNTER_ADD(scan_node_->rows_read_counter(), num_tuples);
        RETURN_IF_ERROR(CommitRows(num_tuples, row_batch));
      } else if (delimited_text_parser_->HasUnfinishedTuple()) {
        DCHECK(scan_node_->materialized_slots().empty());
        DCHECK_EQ(scan_node_->num_materialized_partition_keys(), 0);
        // If no fields are materialized we do not update boundary_column_, or
        // boundary_row_. However, we still need to handle the case of partial tuple due
        // to missing tuple delimiter at the end of file.
        RETURN_IF_ERROR(CommitRows(1, row_batch));
      }
      break;
    }

    DCHECK(eosr);

    int num_tuples;
    RETURN_IF_ERROR(ProcessRange(row_batch, &num_tuples));
    if (num_tuples == 1) break;
    DCHECK_EQ(num_tuples, 0);
  }

  DCHECK(boundary_column_.IsEmpty()) << "Must finish processing boundary column";
  scan_state_ = DONE;
  return Status::OK();
}

Status HdfsTextScanner::ProcessRange(RowBatch* row_batch, int* num_tuples) {
  DCHECK(scan_state_ == FIRST_TUPLE_FOUND || scan_state_ == PAST_SCAN_RANGE);

  MemPool* pool = row_batch->tuple_data_pool();
  bool eosr = stream_->eosr() || scan_state_ == PAST_SCAN_RANGE;
  while (true) {
    if (!eosr && byte_buffer_ptr_ == byte_buffer_end_) {
      RETURN_IF_ERROR(FillByteBuffer(pool, &eosr));
    }

    TupleRow* tuple_row_mem = row_batch->GetRow(row_batch->AddRow());
    int max_tuples = row_batch->capacity() - row_batch->num_rows();

    if (scan_state_ == PAST_SCAN_RANGE) {
      // byte_buffer_ptr_ is already set from FinishScanRange()
      max_tuples = 1;
      eosr = true;
    }

    *num_tuples = 0;
    int num_fields = 0;

    DCHECK_GT(max_tuples, 0);

    batch_start_ptr_ = byte_buffer_ptr_;
    char* col_start = byte_buffer_ptr_;
    {
      // Parse the bytes for delimiters and store their offsets in field_locations_
      SCOPED_TIMER(parse_delimiter_timer_);
      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(max_tuples,
          byte_buffer_end_ - byte_buffer_ptr_, &byte_buffer_ptr_,
          row_end_locations_.data(), field_locations_.data(), num_tuples,
          &num_fields, &col_start));
    }

    // Materialize the tuples into the in memory format for this query
    int num_tuples_materialized = 0;
    if (scan_node_->materialized_slots().size() != 0 &&
        (num_fields > 0 || *num_tuples > 0)) {
      // There can be one partial tuple which returned no more fields from this buffer.
      DCHECK_LE(*num_tuples, num_fields + 1);
      if (!boundary_column_.IsEmpty()) {
        RETURN_IF_ERROR(CopyBoundaryField(field_locations_.data(), pool));
        boundary_column_.Clear();
      }
      num_tuples_materialized = WriteFields(num_fields, *num_tuples, pool, tuple_row_mem);
      DCHECK_GE(num_tuples_materialized, 0);
      RETURN_IF_ERROR(parse_status_);
      if (*num_tuples > 0) {
        // If we saw any tuple delimiters, clear the boundary_row_.
        boundary_row_.Clear();
      }
    } else if (*num_tuples != 0) {
      SCOPED_TIMER(scan_node_->materialize_tuple_timer());
      // If we are doing count(*) then we return tuples only containing partition keys
      boundary_row_.Clear();
      num_tuples_materialized = WriteTemplateTuples(tuple_row_mem, *num_tuples);
    }
    COUNTER_ADD(scan_node_->rows_read_counter(), *num_tuples);

    // Save contents that are split across buffers if we are going to return this column
    if (col_start != byte_buffer_ptr_ && delimited_text_parser_->ReturnCurrentColumn()) {
      DCHECK_EQ(byte_buffer_ptr_, byte_buffer_end_);
      RETURN_IF_ERROR(boundary_column_.Append(col_start, byte_buffer_ptr_ - col_start));
      char* last_row = nullptr;
      if (*num_tuples == 0) {
        last_row = batch_start_ptr_;
      } else {
        last_row = row_end_locations_[*num_tuples - 1] + 1;
      }
      RETURN_IF_ERROR(boundary_row_.Append(last_row, byte_buffer_ptr_ - last_row));
    }
    RETURN_IF_ERROR(CommitRows(num_tuples_materialized, row_batch));

    // Already past the scan range and attempting to complete the last row.
    if (scan_state_ == PAST_SCAN_RANGE) break;

    // Scan range is done. Transition to PAST_SCAN_RANGE.
    if (byte_buffer_ptr_ == byte_buffer_end_ && eosr) {
      scan_state_ = PAST_SCAN_RANGE;
      break;
    }

    if (row_batch->AtCapacity() || scan_node_->ReachedLimit()) break;
  }
  return Status::OK();
}

Status HdfsTextScanner::GetNextInternal(RowBatch* row_batch) {
  DCHECK(!eos_);
  DCHECK_GE(scan_state_, SCAN_RANGE_INITIALIZED);
  DCHECK_NE(scan_state_, DONE);

  if (scan_state_ == SCAN_RANGE_INITIALIZED) {
    // Find the first tuple.  If tuple_found is false, it means we went through the entire
    // scan range without finding a single tuple.  The bytes will be picked up by the scan
    // range before.
    RETURN_IF_ERROR(FindFirstTuple(row_batch->tuple_data_pool()));
    if (scan_state_ != FIRST_TUPLE_FOUND) {
      eos_ = true;
      scan_state_ = DONE;
      return Status::OK();
    }
  }

  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_mem_));
  tuple_ = reinterpret_cast<Tuple*>(tuple_mem_);

  if (scan_state_ == FIRST_TUPLE_FOUND) {
    int num_tuples;
    RETURN_IF_ERROR(ProcessRange(row_batch, &num_tuples));
  }
  if (scan_node_->ReachedLimit()) {
    eos_ = true;
    scan_state_ = DONE;
    return Status::OK();
  }
  if (scan_state_ == PAST_SCAN_RANGE && !row_batch->AtCapacity()) {
    RETURN_IF_ERROR(FinishScanRange(row_batch));
    DCHECK_EQ(scan_state_, DONE);
    eos_ = true;
  }
  return Status::OK();
}

Status HdfsTextScanner::FillByteBuffer(MemPool* pool, bool* eosr, int num_bytes) {
  *eosr = false;

  if (decompressor_.get() == nullptr) {
    Status status;
    if (num_bytes > 0) {
      stream_->GetBytes(num_bytes, reinterpret_cast<uint8_t**>(&byte_buffer_ptr_),
          &byte_buffer_read_size_, &status);
    } else {
      DCHECK_EQ(num_bytes, 0);
      status = stream_->GetBuffer(false, reinterpret_cast<uint8_t**>(&byte_buffer_ptr_),
          &byte_buffer_read_size_);
    }
    RETURN_IF_ERROR(status);
    *eosr = stream_->eosr();
  } else if (decompressor_->supports_streaming()) {
    DCHECK_EQ(num_bytes, 0);
    RETURN_IF_ERROR(FillByteBufferCompressedStream(pool, eosr));
  } else {
    DCHECK_EQ(num_bytes, 0);
    RETURN_IF_ERROR(FillByteBufferCompressedFile(eosr));
  }

  byte_buffer_end_ = byte_buffer_ptr_ + byte_buffer_read_size_;
  return Status::OK();
}

Status HdfsTextScanner::DecompressBufferStream(int64_t bytes_to_read,
    uint8_t** decompressed_buffer, int64_t* decompressed_len, bool *eosr) {
  // Some decompressors, such as Bzip2 API (version 0.9 and later) and Gzip can
  // decompress buffers that are read from stream_, so we don't need to read the
  // whole file in once. A compressed buffer is passed to ProcessBlockStreaming
  // but it may not consume all of the input.
  uint8_t* compressed_buffer_ptr = nullptr;
  int64_t compressed_buffer_size = 0;
  // We don't know how many bytes ProcessBlockStreaming() will consume so we set
  // peek=true and then later advance the stream using SkipBytes().
  if (bytes_to_read == -1) {
    RETURN_IF_ERROR(stream_->GetBuffer(true, &compressed_buffer_ptr,
        &compressed_buffer_size));
  } else {
    DCHECK_GT(bytes_to_read, 0);
    Status status;
    stream_->GetBytes(bytes_to_read, &compressed_buffer_ptr, &compressed_buffer_size,
        &status, true);
    RETURN_IF_ERROR(status);
  }
  int64_t compressed_buffer_bytes_read = 0;
  bool stream_end = false;
  {
    SCOPED_TIMER(decompress_timer_);
    Status status = decompressor_->ProcessBlockStreaming(compressed_buffer_size,
        compressed_buffer_ptr, &compressed_buffer_bytes_read, decompressed_len,
        decompressed_buffer, &stream_end);
    if (!status.ok()) {
      stringstream ss;
      ss << status.GetDetail() << "file=" << stream_->filename()
          << ", offset=" << stream_->file_offset();
      status.AddDetail(ss.str());
      return status;
    }
    DCHECK_GE(compressed_buffer_size, compressed_buffer_bytes_read);
  }
  // Skip the bytes in stream_ that were decompressed.
  Status status;
  stream_->SkipBytes(compressed_buffer_bytes_read, &status);
  RETURN_IF_ERROR(status);

  if (stream_->eosr()) {
    if (stream_end) {
      *eosr = true;
    } else {
      return Status(TErrorCode::COMPRESSED_FILE_TRUNCATED, stream_->filename());
    }
  } else if (*decompressed_len == 0) {
    return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_NO_PROGRESS,
        stream_->filename());
  }

  return Status::OK();
}

Status HdfsTextScanner::FillByteBufferCompressedStream(MemPool* pool, bool* eosr) {
  // We're about to create a new decompression buffer (if we can't reuse). Attach the
  // memory from previous decompression rounds to 'pool'.
  if (!decompressor_->reuse_output_buffer()) {
    if (pool != nullptr) {
      pool->AcquireData(data_buffer_pool_.get(), false);
    } else {
      data_buffer_pool_->FreeAll();
    }
  }

  uint8_t* decompressed_buffer = nullptr;
  int64_t decompressed_len = 0;
  // Set bytes_to_read = -1 because we don't know how much data decompressor need.
  // Just read the first available buffer within the scan range.
  Status status = DecompressBufferStream(-1, &decompressed_buffer, &decompressed_len,
      eosr);
  if (status.code() == TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_NO_PROGRESS) {
    // It's possible (but very unlikely) that ProcessBlockStreaming() wasn't able to
    // make progress if the compressed buffer returned by GetBytes() is too small.
    // (Note that this did not even occur in simple experiments where the input buffer
    // is always 1 byte, but we need to handle this case to be defensive.) In this
    // case, try again with a reasonably large fixed size buffer. If we still did not
    // make progress, then return an error.
    LOG(INFO) << status.GetDetail();
    status = DecompressBufferStream(COMPRESSED_DATA_FIXED_READ_SIZE,
        &decompressed_buffer, &decompressed_len, eosr);
  }
  RETURN_IF_ERROR(status);
  byte_buffer_ptr_ = reinterpret_cast<char*>(decompressed_buffer);
  byte_buffer_read_size_ = decompressed_len;

  if (*eosr) {
    DCHECK(stream_->eosr());
    context_->ReleaseCompletedResources(nullptr, true);
  }

  return Status::OK();
}

Status HdfsTextScanner::FillByteBufferCompressedFile(bool* eosr) {
  // For other compressed text: attempt to read and decompress the entire file, point
  // to the decompressed buffer, and then continue normal processing.
  DCHECK(decompression_type_ != THdfsCompression::SNAPPY);
  HdfsFileDesc* desc = scan_node_->GetFileDesc(
      context_->partition_descriptor()->id(), stream_->filename());
  int64_t file_size = desc->file_length;
  DCHECK_GT(file_size, 0);

  Status status;
  stream_->GetBytes(file_size, reinterpret_cast<uint8_t**>(&byte_buffer_ptr_),
      &byte_buffer_read_size_, &status);
  RETURN_IF_ERROR(status);

  // If didn't read anything, return.
  if (byte_buffer_read_size_ == 0) {
    *eosr = true;
    return Status::OK();
  }

  // Need to read the entire file.
  if (file_size > byte_buffer_read_size_) {
    stringstream ss;
    ss << "Expected to read a compressed text file of size " << file_size << " bytes. "
       << "But only read " << byte_buffer_read_size_ << " bytes. This may indicate "
       << "data file corruption. (file: " << stream_->filename() << ").";
    return Status(ss.str());
  }

  // Decompress and adjust the byte_buffer_ptr_ and byte_buffer_read_size_ accordingly.
  int64_t decompressed_len = 0;
  uint8_t* decompressed_buffer = nullptr;
  SCOPED_TIMER(decompress_timer_);
  // TODO: Once the writers are in, add tests with very large compressed files (4GB)
  // that could overflow.
  RETURN_IF_ERROR(decompressor_->ProcessBlock(false, byte_buffer_read_size_,
      reinterpret_cast<uint8_t*>(byte_buffer_ptr_), &decompressed_len,
      &decompressed_buffer));

  // Inform 'stream_' that the buffer with the compressed text can be released.
  context_->ReleaseCompletedResources(nullptr, true);

  VLOG_FILE << "Decompressed " << byte_buffer_read_size_ << " to " << decompressed_len;
  byte_buffer_ptr_ = reinterpret_cast<char*>(decompressed_buffer);
  byte_buffer_read_size_ = decompressed_len;
  *eosr = stream_->eosr();
  return Status::OK();
}

Status HdfsTextScanner::FindFirstTuple(MemPool* pool) {
  DCHECK_EQ(scan_state_, SCAN_RANGE_INITIALIZED);

  // Either we're at the start of the file and thus skip all header lines, or we're in the
  // middle of the file and look for the next tuple.
  bool tuple_found = true;
  int num_rows_to_skip = stream_->scan_range()->offset() == 0
      ? scan_node_->skip_header_line_count() : 1;
  if (num_rows_to_skip > 0) {
    int num_skipped_rows = 0;
    bool eosr = false;
    tuple_found = false;
    // Offset maybe not point to a tuple boundary, skip ahead to the first tuple start in
    // this scan range (if one exists).
    do {
      RETURN_IF_ERROR(FillByteBuffer(nullptr, &eosr));

      delimited_text_parser_->ParserReset();
      SCOPED_TIMER(parse_delimiter_timer_);
      int64_t next_tuple_offset = 0;
      int64_t bytes_left = byte_buffer_read_size_;
      while (num_skipped_rows < num_rows_to_skip) {
        next_tuple_offset = delimited_text_parser_->FindFirstInstance(byte_buffer_ptr_,
          bytes_left);
        if (next_tuple_offset == -1) break;
        byte_buffer_ptr_ += next_tuple_offset;
        bytes_left -= next_tuple_offset;
        ++num_skipped_rows;
      }
      if (next_tuple_offset != -1) tuple_found = true;
    } while (!tuple_found && !eosr);

    // Special case: if the first delimiter is at the end of the current buffer, it's
    // possible it's a split "\r\n" delimiter.
    if (tuple_found && byte_buffer_ptr_ == byte_buffer_end_) {
      bool split_delimiter;
      RETURN_IF_ERROR(CheckForSplitDelimiter(&split_delimiter));
      if (split_delimiter) {
        if (eosr) {
          // Split delimiter at the end of the scan range. The next tuple is considered
          // part of the next scan range, so we report no tuple found.
          tuple_found = false;
        } else {
          // Split delimiter at the end of the current buffer, but not eosr. Advance to
          // the correct position in the next buffer.
          RETURN_IF_ERROR(FillByteBuffer(pool, &eosr));
          DCHECK_GT(byte_buffer_read_size_, 0);
          DCHECK_EQ(*byte_buffer_ptr_, '\n');
          byte_buffer_ptr_ += 1;
        }
      }
    }
    if (num_rows_to_skip > 1 && num_skipped_rows != num_rows_to_skip) {
      DCHECK(!tuple_found);
      stringstream ss;
      ss << "Could only skip " << num_skipped_rows << " header lines in first scan range "
         << "but expected " << num_rows_to_skip << ". Try increasing "
         << "max_scan_range_length to a value larger than the size of the file's header.";
      return Status(ss.str());
    }
  }
  if (tuple_found) scan_state_ = FIRST_TUPLE_FOUND;
  DCHECK(delimited_text_parser_->AtTupleStart());
  return Status::OK();
}

Status HdfsTextScanner::CheckForSplitDelimiter(bool* split_delimiter) {
  DCHECK_EQ(byte_buffer_ptr_, byte_buffer_end_);
  *split_delimiter = false;

  // Nothing in buffer
  if (byte_buffer_read_size_ == 0) return Status::OK();

  // If the line delimiter is "\n" (meaning we also accept "\r" and "\r\n" as delimiters)
  // and the current buffer ends with '\r', this could be a "\r\n" delimiter.
  bool split_delimiter_possible = context_->partition_descriptor()->line_delim() == '\n'
      && *(byte_buffer_end_ - 1) == '\r';
  if (!split_delimiter_possible) return Status::OK();

  // The '\r' may be escaped. If it's not the text parser will report a complete tuple.
  if (delimited_text_parser_->HasUnfinishedTuple()) return Status::OK();

  // Peek ahead one byte to see if the '\r' is followed by '\n'.
  Status status;
  uint8_t* next_byte;
  int64_t out_len;
  stream_->GetBytes(1, &next_byte, &out_len, &status, /*peek*/ true);
  RETURN_IF_ERROR(status);

  // No more bytes after current buffer
  if (out_len == 0) return Status::OK();

  *split_delimiter = *next_byte == '\n';
  return Status::OK();
}

// Codegen for materializing parsed data into tuples.  The function WriteCompleteTuple is
// codegen'd using the IRBuilder for the specific tuple description.  This function
// is then injected into the cross-compiled driving function, WriteAlignedTuples().
Status HdfsTextScanner::Codegen(HdfsScanNodeBase* node,
    const vector<ScalarExpr*>& conjuncts, Function** write_aligned_tuples_fn) {
  *write_aligned_tuples_fn = nullptr;
  DCHECK(node->runtime_state()->ShouldCodegen());
  LlvmCodeGen* codegen = node->runtime_state()->codegen();
  DCHECK(codegen != nullptr);
  Function* write_complete_tuple_fn;
  RETURN_IF_ERROR(CodegenWriteCompleteTuple(node, codegen, conjuncts,
      &write_complete_tuple_fn));
  DCHECK(write_complete_tuple_fn != nullptr);
  RETURN_IF_ERROR(CodegenWriteAlignedTuples(node, codegen, write_complete_tuple_fn,
      write_aligned_tuples_fn));
  DCHECK(*write_aligned_tuples_fn != nullptr);
  return Status::OK();
}

Status HdfsTextScanner::Open(ScannerContext* context) {
  RETURN_IF_ERROR(HdfsScanner::Open(context));

  parse_delimiter_timer_ = ADD_CHILD_TIMER(scan_node_->runtime_profile(),
      "DelimiterParseTime", ScanNode::SCANNER_THREAD_TOTAL_WALLCLOCK_TIME);

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());
  row_end_locations_.resize(state_->batch_size());

  // Reset state for new scan range
  RETURN_IF_ERROR(InitNewRange());
  return Status::OK();
}

// This function deals with tuples that straddle blocks. There are two cases:
// 1. There is already a partial tuple in flight from the previous time around.
//    This tuple can either be fully materialized (all the materialized columns have
//    been processed but we haven't seen the tuple delimiter yet) or only partially
//    materialized.  In this case num_tuples can be greater than num_fields
// 2. There is a non-fully materialized tuple at the end.  The cols that have been
//    parsed so far are written to 'tuple_' and the remaining ones will be picked up
//    (case 1) the next time around.
int HdfsTextScanner::WriteFields(int num_fields, int num_tuples, MemPool* pool,
    TupleRow* row) {
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  DCHECK(boundary_column_.IsEmpty());

  FieldLocation* fields = field_locations_.data();

  int num_tuples_processed = 0;
  int num_tuples_materialized = 0;
  // Write remaining fields, if any, from the previous partial tuple.
  if (slot_idx_ != 0) {
    DCHECK(tuple_ != nullptr);
    int num_partial_fields = scan_node_->materialized_slots().size() - slot_idx_;
    num_partial_fields = min(num_partial_fields, num_fields);
    WritePartialTuple(fields, num_partial_fields);

    // This handles case 1.  If the tuple is complete and we've found a tuple delimiter
    // this time around (i.e. num_tuples > 0), add it to the row batch.  Otherwise,
    // it will get picked up the next time around
    if (slot_idx_ == scan_node_->materialized_slots().size() && num_tuples > 0) {
      if (UNLIKELY(error_in_row_)) {
        if (state_->abort_on_error()) {
          parse_status_ = Status(state_->ErrorLog());
        } else {
          LogRowParseError();
        }
        if (!parse_status_.ok()) return 0;
        error_in_row_ = false;
      }

      CopyAndClearPartialTuple(pool);

      row->SetTuple(scan_node_->tuple_idx(), tuple_);

      slot_idx_ = 0;
      ++num_tuples_processed;
      --num_tuples;

      if (EvalConjuncts(row)) {
        ++num_tuples_materialized;
        tuple_ = next_tuple(tuple_byte_size_, tuple_);
        row = next_row(row);
      }
    }

    num_fields -= num_partial_fields;
    fields += num_partial_fields;
  }

  // Write complete tuples.  The current field, if any, is at the start of a tuple.
  if (num_tuples > 0) {
    int max_added_tuples = (scan_node_->limit() == -1) ?
        num_tuples : scan_node_->limit() - scan_node_->rows_returned();
    int tuples_returned = 0;
    // Call jitted function if possible
    if (write_tuples_fn_ != nullptr) {
      // HdfsScanner::InitializeWriteTuplesFn() will skip codegen if there are string
      // slots and escape characters. TextConverter::WriteSlot() will be used instead.
      DCHECK(scan_node_->tuple_desc()->string_slots().empty() ||
          delimited_text_parser_->escape_char() == '\0');
      tuples_returned = write_tuples_fn_(this, pool, row, sizeof(Tuple*), fields,
          num_tuples, max_added_tuples, scan_node_->materialized_slots().size(),
          num_tuples_processed);
    } else {
      tuples_returned = WriteAlignedTuples(pool, row, sizeof(Tuple*), fields,
          num_tuples, max_added_tuples, scan_node_->materialized_slots().size(),
          num_tuples_processed);
    }
    if (tuples_returned == -1) return 0;
    DCHECK_EQ(slot_idx_, 0);

    num_tuples_materialized += tuples_returned;
    num_fields -= num_tuples * scan_node_->materialized_slots().size();
    fields += num_tuples * scan_node_->materialized_slots().size();
  }

  DCHECK_GE(num_fields, 0);
  DCHECK_LE(num_fields, scan_node_->materialized_slots().size());

  // Write out the remaining slots (resulting in a partially materialized tuple)
  if (num_fields != 0) {
    DCHECK(tuple_ != nullptr);
    partial_tuple_ = Tuple::Create(tuple_byte_size_, boundary_pool_.get());
    InitTuple(template_tuple_, partial_tuple_);
    // If there have been no materialized tuples at this point, copy string data
    // out of byte_buffer and reuse the byte_buffer.  The copied data can be at
    // most one tuple's worth.
    WritePartialTuple(fields, num_fields);
  }
  DCHECK_LE(slot_idx_, scan_node_->materialized_slots().size());
  return num_tuples_materialized;
}

Status HdfsTextScanner::CopyBoundaryField(FieldLocation* data, MemPool* pool) {
  bool needs_escape = data->len < 0;
  int copy_len = needs_escape ? -data->len : data->len;
  int64_t total_len = copy_len + boundary_column_.len();
  char* str_data = reinterpret_cast<char*>(pool->TryAllocateUnaligned(total_len));
  if (UNLIKELY(str_data == nullptr)) {
    string details = Substitute("HdfsTextScanner::CopyBoundaryField() failed to allocate "
        "$0 bytes.", total_len);
    return pool->mem_tracker()->MemLimitExceeded(state_, details, total_len);
  }
  memcpy(str_data, boundary_column_.buffer(), boundary_column_.len());
  memcpy(str_data + boundary_column_.len(), data->start, copy_len);
  data->start = str_data;
  data->len = needs_escape ? -total_len : total_len;
  return Status::OK();
}

void HdfsTextScanner::WritePartialTuple(FieldLocation* fields, int num_fields) {
  for (int i = 0; i < num_fields; ++i) {
    bool need_escape = false;
    int len = fields[i].len;
    if (len < 0) {
      len = -len;
      need_escape = true;
    }

    const SlotDescriptor* desc = scan_node_->materialized_slots()[slot_idx_];
    if (!text_converter_->WriteSlot(desc, partial_tuple_,
        fields[i].start, len, true, need_escape, boundary_pool_.get())) {
      ReportColumnParseError(desc, fields[i].start, len);
      error_in_row_ = true;
    }
    ++slot_idx_;
  }
}

void HdfsTextScanner::CopyAndClearPartialTuple(MemPool* pool) {
  DCHECK(tuple_ != nullptr);
  partial_tuple_->DeepCopy(tuple_, *scan_node_->tuple_desc(), pool);
  boundary_row_.Reset();
  boundary_column_.Reset();
  boundary_pool_->Clear();
  partial_tuple_ = nullptr;
}
