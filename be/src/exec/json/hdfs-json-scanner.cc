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

#include "exec/json/hdfs-json-scanner.h"

#include "common/names.h"
#include "common/status.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.h"
#include "exec/text-converter.inline.h"
#include "runtime/multi-precision.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "util/decompress.h"
#include "util/debug-util.h"
#include "util/scope-exit-trigger.h"
#include "util/string-parser.h"

#include <gutil/strings/substitute.h>
#include <map>

using namespace impala;
using namespace impala::io;
using namespace strings;

DEFINE_bool(enable_json_scanner, true,
    "If set false, disable reading from json format tables.");

HdfsJsonScanner::HdfsJsonScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
    : HdfsScanner(scan_node, state),
      scanner_state_(CREATED),
      buffer_status_(Status::OK()),
      tuple_row_(nullptr),
      current_pool_(nullptr),
      error_in_row_(false),
      num_tuples_materialized_(0),
      parse_json_timer_(nullptr),
      get_buffer_timer_(nullptr) { }

HdfsJsonScanner::~HdfsJsonScanner() { }

Status HdfsJsonScanner::Open(ScannerContext* context) {
  DCHECK_EQ(scanner_state_, CREATED);
  RETURN_IF_ERROR(HdfsScanner::Open(context));

  parse_json_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "ParseJsonTime");
  get_buffer_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "GetDataBufferTime");
  RETURN_IF_ERROR(InitNewRange());
  scanner_state_ = OPENED;
  return Status::OK();
}

void HdfsJsonScanner::Close(RowBatch* row_batch) {
  DCHECK(!is_closed_);
  // Need to close the decompressor before transferring the remaining resources to
  // 'row_batch' because in some cases there is memory allocated in the decompressor_'s
  // temp_memory_pool_.
  if (decompressor_ != nullptr) {
    decompressor_->Close();
    decompressor_.reset();
  }

  if (row_batch != nullptr) {
    row_batch->tuple_data_pool()->AcquireData(template_tuple_pool_.get(), false);
    if (scan_node_->HasRowBatchQueue()) {
      static_cast<HdfsScanNode*>(scan_node_)->AddMaterializedRowBatch(
          std::unique_ptr<RowBatch>(row_batch));
    }
  } else {
    template_tuple_pool_->FreeAll();
  }
  // The JsonParser always copies values instead of referencing them, so it doesn't
  // reference the data in the data_buffer_pool_. Therefore, we don't need
  // row_batch to acquire data from the data_buffer_pool_, so we could always
  // call FreeAll().
  data_buffer_pool_->FreeAll();
  context_->ReleaseCompletedResources(true);

  // Verify all resources (if any) have been transferred or freed.
  DCHECK_EQ(template_tuple_pool_.get()->total_allocated_bytes(), 0);
  DCHECK_EQ(data_buffer_pool_.get()->total_allocated_bytes(), 0);
  if (stream_ != nullptr) {
    scan_node_->RangeComplete(THdfsFileFormat::JSON,
        stream_->file_desc()->file_compression);
  }
  CloseInternal();
}

Status HdfsJsonScanner::InitNewRange() {
  DCHECK_EQ(scanner_state_, CREATED);

  auto compression_type = stream_ ->file_desc()->file_compression;
  // Update the decompressor based on the compression type of the file in the context.
  DCHECK(compression_type != THdfsCompression::SNAPPY)
      << "FE should have generated SNAPPY_BLOCKED instead.";
  // In Hadoop, text files compressed into .DEFLATE files contain
  // deflate with zlib wrappings as opposed to raw deflate, which
  // is what THdfsCompression::DEFLATE implies. Since deflate is
  // the default compression algorithm used in Hadoop, it makes
  // sense to map it to type DEFAULT in Impala instead
  if (compression_type == THdfsCompression::DEFLATE) {
    compression_type = THdfsCompression::DEFAULT;
  }
  RETURN_IF_ERROR(UpdateDecompressor(compression_type));

  // TODO: Optmize for zero slots scan (e.g. count(*)).
  vector<string> schema;
  schema.reserve(scan_node_->materialized_slots().size());
  for (const SlotDescriptor* slot : scan_node_->materialized_slots()) {
    schema.push_back(scan_node_->hdfs_table()->GetColumnDesc(slot).name());
  }

  text_converter_.reset(new TextConverter('\\', "", false, state_->strict_mode()));
  json_parser_.reset(new JsonParser<HdfsJsonScanner>(schema, this));
  json_parser_->ResetParser();
  return Status::OK();
}

Status HdfsJsonScanner::GetNextInternal(RowBatch* row_batch) {
  DCHECK(!eos_);
  DCHECK_GE(scanner_state_, OPENED);
  DCHECK_NE(scanner_state_, FINISHED);

  current_pool_ = row_batch->tuple_data_pool();

  if (scanner_state_ == OPENED) {
    // Find the first tuple. If scanner_state_ is not SCANNING, it means we went through
    // the entire scan range without finding a single tuple. The bytes will be picked up
    // by the previous scan range in the same file.
    RETURN_IF_ERROR(FindFirstTuple());
    if (scanner_state_ != SCANNING) {
      eos_ = true;
      scanner_state_ = FINISHED;
      return Status::OK();
    }
  }

  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(
      row_batch->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_mem_));
  tuple_ = reinterpret_cast<Tuple*>(tuple_mem_);
  tuple_row_ = row_batch->GetRow(row_batch->AddRow());

  while (scanner_state_ == SCANNING) {
    num_tuples_materialized_ = 0;
    int num_tuples = 0;
    int max_tuples = row_batch->capacity() - row_batch->num_rows();

    RETURN_IF_ERROR(ParseWrapper(max_tuples, &num_tuples));
    COUNTER_ADD(scan_node_->rows_read_counter(), num_tuples);

    // Because the processes of parsing JSON, materializing tuples, and even reading data
    // are intertwined, it can be expensive to accurately time them individually.
    // Therefore, we using this method to measure the time it takes to materialize tuples,
    // please note that the value obtained will always be inflated because the time it
    // takes to parse JSON is also included.
    // TODO: find a better way.
    COUNTER_SET(scan_node_->materialize_tuple_timer(),
        parse_json_timer_->value() - get_buffer_timer_->value());

    RETURN_IF_ERROR(CommitRows(num_tuples_materialized_, row_batch));

    if (row_batch->AtCapacity() || scan_node_->ReachedLimitShared()) break;
  }

  if (scanner_state_ >= PAST_SCANNING || scan_node_->ReachedLimitShared()) {
    eos_ = true;
    scanner_state_ = FINISHED;
  }
  return Status::OK();
}

Status HdfsJsonScanner::FindFirstTuple() {
  DCHECK_EQ(scanner_state_, OPENED);
  if (stream_->scan_range()->offset() == 0) {
    scanner_state_ = SCANNING;
    return Status::OK();
  }
  SCOPED_TIMER(parse_json_timer_);
  if (json_parser_->MoveToNextJson()) {
    scanner_state_ = SCANNING;
    DCHECK_OK(buffer_status_);
  }
  return buffer_status_;
}

Status HdfsJsonScanner::ParseWrapper(int max_tuples, int* num_tuples) {
  DCHECK(json_parser_->IsTidy());
  SCOPED_TIMER(parse_json_timer_);
  Status status = json_parser_->Parse(max_tuples, num_tuples);
  RETURN_IF_ERROR(buffer_status_);
  return status;
}

bool HdfsJsonScanner::HandleConvertError(const SlotDescriptor* desc, const char* data,
    int len) {
  error_in_row_ = true;
  tuple_->SetNull(desc->null_indicator_offset());
  if (state_->LogHasSpace() || state_->abort_on_error()) {
    const HdfsTableDescriptor* table = scan_node_->hdfs_table();
    constexpr int max_view_len = 50;
    string data_view = string(data, std::min(len, max_view_len));
    if (len > max_view_len) data_view += "...";
    string msg = Substitute("Error converting column: $0.$1.$2, type: $3, data: '$4'",
        table->database(), table->name(), table->GetColumnDesc(desc).name(),
        desc->type().DebugString(), data_view);

    if (state_->LogHasSpace()) {
      state_->LogError(ErrorMsg(TErrorCode::GENERAL, msg), 2);
    }

    if (state_->abort_on_error() && parse_status_.ok()) parse_status_ = Status(msg);
  }
  return parse_status_.ok();
}

Status HdfsJsonScanner::HandleError(rapidjson::ParseErrorCode error, size_t offset) {
  if (error == rapidjson::kParseErrorTermination) {
    DCHECK(!parse_status_.ok());
    RETURN_IF_ERROR(state_->LogOrReturnError(parse_status_.msg()));
    parse_status_ = Status::OK();
  } else {
    RETURN_IF_ERROR(state_->LogOrReturnError(ErrorMsg::Init(TErrorCode::GENERAL,
        Substitute("Parse Json file error, file: $0, offset: $1, cause by: $2",
        stream_->filename(), stream_->scan_range()->offset() + offset,
        rapidjson::GetParseError_En(error)))));
  }
  return Status::OK();
}

static bool AllWhitespaceBeforeNewline(uint8_t* begin, int64_t len) {
  DCHECK(len >= 0);
  uint8_t* end = begin + len;
  while (begin != end) {
    switch (*begin++) {
      case '\r':
      case '\t':
      case ' ': break;
      case '\n': return true;
      default: return false;
    }
  }
  return false;
}

Status HdfsJsonScanner::FillBytesToBuffer(uint8_t** buffer, int64_t* bytes_read) {
  if (scanner_state_ == PAST_SCANNING) {
    // In the PAST_SCANNING state, we only read a small block data at a time for scanning.
    // If the parser completes the parsing of the last json object, it will exit the loop
    // due to BreakParse().
    Status status;
    if (UNLIKELY(!stream_->GetBytes(NEXT_BLOCK_READ_SIZE, buffer, bytes_read, &status))) {
      DCHECK(!status.ok());
      return status;
    }

    // A special case is when the first character of the next scan range is a newline
    // character (perhaps with other whitespace characters before it). Our scan should
    // stop at the first newline character in the next range, while the parser skips
    // whitespace characters. If we don't handle this case, the first line of the next
    // range will be scanned twice. Therefore, we need to return directly here to inform
    // the parser that eos has been reached.
    if (UNLIKELY(AllWhitespaceBeforeNewline(*buffer, *bytes_read))) {
      scanner_state_ = FINISHED;
      *bytes_read = 0;
    }
  } else {
    RETURN_IF_ERROR(stream_->GetBuffer(false, buffer, bytes_read));
  }
  return Status::OK();
}

void HdfsJsonScanner::GetNextBuffer(const char** begin, const char** end) {
  DCHECK(*begin == *end);
  SCOPED_TIMER(get_buffer_timer_);

  // The eosr indicates that we have scanned all data within the scan range. If the
  // scanner state is OPENED, it means that we encountered eosr in FindFirstTuple(),
  // indicating that there is no start of tuple in this scan range, this will be handle by
  // the previous scan range in the same file. If the scanner state is SCANNING, it means
  // that we have completed scanning the data within the range, and need to read the next
  // range of data to complete the scan.
  if (stream_->eosr()) {
    if (scanner_state_ == OPENED) return;
    if (scanner_state_ == SCANNING) scanner_state_ = PAST_SCANNING;
  }

  if (stream_->eof() || scanner_state_ == FINISHED) return;

  uint8_t* next_buffer_begin = nullptr;
  int64_t next_buffer_size = 0;
  if (decompressor_ == nullptr) {
    buffer_status_ = FillBytesToBuffer(&next_buffer_begin, &next_buffer_size);
  } else if (decompressor_->supports_streaming()) {
    bool eosr = false;
    // The JsonParser always copies values instead of referencing them, so it doesn't
    // reference the data in the data_buffer_pool_. Therefore, we don't need current_pool_
    // to acquire data from the data_buffer_pool_, so we pass nullptr to
    // DecompressStreamToBuffer().
    buffer_status_ = DecompressStreamToBuffer(&next_buffer_begin, &next_buffer_size,
        nullptr, &eosr);
  } else {
    buffer_status_ = DecompressFileToBuffer(&next_buffer_begin, &next_buffer_size);
    if (next_buffer_size == 0) scanner_state_ = FINISHED;
  }
  RETURN_VOID_IF_ERROR(buffer_status_);
  if (UNLIKELY(next_buffer_size == 0)) return;

  *begin = reinterpret_cast<char*>(next_buffer_begin);
  *end = *begin + next_buffer_size;
}

void HdfsJsonScanner::InitRow() {
  InitTuple(template_tuple_, tuple_);
}

void HdfsJsonScanner::SubmitRow() {
  tuple_row_->SetTuple(0, tuple_);
  if (EvalConjuncts(tuple_row_)) {
    ++num_tuples_materialized_;
    tuple_ = next_tuple(tuple_byte_size() ,tuple_);
    tuple_row_ = next_row(tuple_row_);
  }
  if (UNLIKELY(error_in_row_)) {
    LogRowParseError();
    error_in_row_ = false;
  }
}

bool HdfsJsonScanner::InvokeWriteSlot(const SlotDescriptor* slot_desc, const char* data,
    int len) {
  // TODO: Support Invoke CodeGend WriteSlot
  if (LIKELY(text_converter_->WriteSlot(slot_desc, tuple_, data, len, true, false,
      current_pool_))) {
    return true;
  }
  return HandleConvertError(slot_desc, data, len);
}

void HdfsJsonScanner::AddNull(int index) {
  const SlotDescriptor* slot_desc = scan_node_->materialized_slots()[index];
  tuple_->SetNull(slot_desc->null_indicator_offset());
}

bool HdfsJsonScanner::AddBool(int index, bool value) {
  const SlotDescriptor* slot_desc = scan_node_->materialized_slots()[index];
  if (UNLIKELY(slot_desc->type().type != TYPE_BOOLEAN)) {
    return InvokeWriteSlot(slot_desc, value ? "true" : "false", value ? 4 : 5);
  }
  void* slot = tuple_->GetSlot(slot_desc->tuple_offset());
  *reinterpret_cast<bool*>(slot) = value;
  return true;
}

bool HdfsJsonScanner::AddString(int index, const char* str, uint32_t len) {
  const SlotDescriptor* slot_desc = scan_node_->materialized_slots()[index];
  return InvokeWriteSlot(slot_desc, str, len);
}

bool HdfsJsonScanner::AddNumber(int index, const char* str, uint32_t len) {
  const SlotDescriptor* slot_desc = scan_node_->materialized_slots()[index];
  return InvokeWriteSlot(slot_desc, str, len);
}
