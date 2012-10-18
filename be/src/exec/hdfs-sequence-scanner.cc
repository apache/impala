// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-sequence-scanner.h"

#include "codegen/llvm-codegen.h"
#include "exec/delimited-text-parser.inline.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scan-range-context.h"
#include "exec/serde-utils.inline.h"
#include "exec/text-converter.inline.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/string-search.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;

const char* const HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.io.Text";

const int HdfsSequenceScanner::HEADER_SIZE = 1024;

const uint8_t HdfsSequenceScanner::SEQFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};

static const uint8_t SEQUENCE_FILE_RECORD_DELIMITER = 0xff;

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsSequenceScanner::HdfsSequenceScanner(HdfsScanNode* scan_node, RuntimeState* state) 
    : HdfsScanner(scan_node, state, NULL),
      header_(NULL),
      unparsed_data_buffer_pool_(new MemPool()),
      unparsed_data_buffer_(NULL),
      num_buffered_records_in_compressed_block_(0),
      have_sync_(false),
      block_start_(0) {
}

HdfsSequenceScanner::~HdfsSequenceScanner() {
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      unparsed_data_buffer_pool_->peak_allocated_bytes());
}

void HdfsSequenceScanner::IssueInitialRanges(HdfsScanNode* scan_node, 
    const vector<HdfsFileDesc*>& files) {
  // Issue just the header range for each file.  When the header is complete,
  // we'll issue the ranges for that file.  
  for (int i = 0; i < files.size(); ++i) {
    int64_t partition_id = reinterpret_cast<int64_t>(files[i]->ranges[0]->meta_data());
    // TODO: add remote disk id and plumb that through to the io mgr.  It should have
    // 1 queue for each NIC as well?
    DiskIoMgr::ScanRange* header_range = scan_node->AllocateScanRange(
        files[i]->filename.c_str(), HEADER_SIZE, 0, partition_id, -1);
    scan_node->AddDiskIoRange(header_range);
  }
}

void HdfsSequenceScanner::IssueFileRanges(const char* filename) {
  HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename);
  scan_node_->AddDiskIoRange(file_desc);
}

// Codegen for materialized parsed data into tuples.  
// TODO: sequence file scanner needs to be split into a cross compiled ir file,
// probably just for the block compressed path.  WriteCompleteTuple should be 
// injected into that function.
Function* HdfsSequenceScanner::Codegen(HdfsScanNode* node) {
  LlvmCodeGen* codegen = node->runtime_state()->llvm_codegen();
  if (codegen == NULL) return NULL;
  Function* write_complete_tuple_fn = CodegenWriteCompleteTuple(node, codegen);
  if (write_complete_tuple_fn == NULL) return NULL;
  return CodegenWriteAlignedTuples(node, codegen, write_complete_tuple_fn);
}

Status HdfsSequenceScanner::ProcessScanRange(ScanRangeContext* context) {
  context_ = context;

  header_ = reinterpret_cast<FileHeader*>(
      scan_node_->GetFileMetadata(context_->filename()));
  if (header_ == NULL) {
    // This is the initial scan range just to parse the header
    only_parsing_header_ = true;
    header_ = state_->obj_pool()->Add(new FileHeader());
    RETURN_IF_ERROR(ReadFileHeader());

    // Header is parsed, set the metadata in the scan node and issue more ranges
    scan_node_->SetFileMetadata(context_->filename(), header_);
    IssueFileRanges(context_->filename());
    return Status::OK;
  }

  // Initialize state for new scan range
  RETURN_IF_ERROR(InitNewRange());

  // Find the first record
  bool found;
  RETURN_IF_ERROR(FindFirstRecord(&found));
  if (!found) return Status::OK;

  // Process Range.
  // We can continue through errors by skipping to to the next SYNC hash.
  Status status;
  int64_t first_error_offset = 0;
  int num_errors = 0;

  do {
    status = ProcessRange();
    // Save the offset of any error.
    if (first_error_offset == 0) first_error_offset = context_->file_offset();

    // Catch errors from file format parsing.  We call some utilities
    // that do not log errors so generate a reasonable message.
    // TODO: The utilities should log errors.
    if (!status.ok()) {
      if (state_->LogHasSpace()) {
        stringstream ss;
        ss << "Format error in record or block header ";
        if (context_->eosr()) {
          ss << "at end of file.";
        } else {
          ss << "at offset: "  << block_start_;
        }
        state_->LogError(ss.str());
      }
    }

    // If no errors or we abort on error then exit loop.
    if (state_->abort_on_error() || status.ok()) break;

    // If we are not at the end of the scan range, try to recover.
    if (!context_->eosr()) {
      parse_status_ = Status::OK;
      ++num_errors;
      status = SkipToSync();
      if (context_->eosr()) break;

      // If block compressed, reset the number of records.
      // We will be at the beginning of a block.
      num_buffered_records_in_compressed_block_ = 0;
    }
  } while (!context_->eosr());

  if (num_errors != 0 || !status.ok()) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss  << "First error while processing: " << context_->filename()
          << " at offset: "  << first_error_offset;
      state_->LogError(ss.str());
      state_->ReportFileErrors(context_->filename(), num_errors == 0 ? 1 : num_errors);
    }
    if (state_->abort_on_error()) return status;
  }

  // All done with this scan range.
  return Status::OK;
}

Status HdfsSequenceScanner::Close() {
  context_->AcquirePool(unparsed_data_buffer_pool_.get());
  if (!only_parsing_header_) scan_node_->RangeComplete();
  context_->Complete();
  return Status::OK;
}
  
Status HdfsSequenceScanner::InitNewRange() {
  DCHECK(header_ != NULL);
  only_parsing_header_ = false;

  HdfsPartitionDescriptor* hdfs_partition = context_->partition_descriptor();
  
  text_converter_.reset(new TextConverter(hdfs_partition->escape_char()));
  
  delimited_text_parser_.reset(new DelimitedTextParser(scan_node_, '\0',
      hdfs_partition->field_delim(), hdfs_partition->collection_delim(),
      hdfs_partition->escape_char()));
  
  num_buffered_records_in_compressed_block_ = 0;

  template_tuple_ = context_->template_tuple();

  if (header_->is_compressed) {
    // For record-compressed data we always want to copy since they tend to be
    // small and occupy a bigger mempool chunk.
    if (!header_->is_blk_compressed) context_->set_compact_data(true);
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        unparsed_data_buffer_pool_.get(), context_->compact_data(),
        header_->codec, &decompressor_));
  }
  
  // Initialize codegen fn
  RETURN_IF_ERROR(InitializeCodegenFn(hdfs_partition, 
      THdfsFileFormat::SEQUENCE_FILE, "HdfsSequenceScanner"));
  return Status::OK;
}

Status HdfsSequenceScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  record_locations_.resize(state_->batch_size());
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());
  
  decompress_timer_ = ADD_COUNTER(
      scan_node_->runtime_profile(), "DecompressionTime", TCounterType::CPU_TICKS);

  return Status::OK;
}

int FindSyncBlock(const uint8_t* buffer, int buffer_len, 
    const uint8_t* sync, int sync_len) {
  char marker_and_sync[4 + sync_len];
  marker_and_sync[0] = marker_and_sync[1] = 
      marker_and_sync[2] = marker_and_sync[3] = 0xff;
  memcpy(marker_and_sync + 4, sync, sync_len);

  StringValue needle(marker_and_sync, 4 + sync_len);
  StringValue haystack(
      const_cast<char*>(reinterpret_cast<const char*>(buffer)), buffer_len);

  StringSearch search(&needle);
  int offset = search.Search(&haystack);
  if (offset == -1) return buffer_len;
  return offset;
}

Status HdfsSequenceScanner::FindFirstRecord(bool* found) {
  Status status;
  if (context_->scan_range()->offset() == 0) {
    // scan range that starts at the beginning of the file, just skip ahead by
    // the header size.
    RETURN_IF_FALSE(SerDeUtils::SkipBytes(context_, header_->header_size, &status));
    *found = true;
    return Status::OK;
  }

  RETURN_IF_ERROR(SkipToSync());
  *found = !context_->eosr();
  return Status::OK;
}

inline Status HdfsSequenceScanner::GetRecord(uint8_t** record_ptr,
                                             int64_t* record_len, bool *eosr) {
  // There are 2 cases:
  //  Record-compressed -- like a regular record, but the data is compressed.
  //  Uncompressed.
    
  block_start_ = context_->file_offset();
  bool sync;
  *eosr = context_->eosr();
  Status stat = ReadBlockHeader(&sync);
  if (!stat.ok()) {
    *record_ptr = NULL;
    if (*eosr) return Status::OK;
    return stat;
  }

  // If we read a sync mark and are past the end of the scan range we are done.
  if (sync && *eosr) {
    *record_ptr = NULL;
    return Status::OK;
  }

  // If we have not read the end the next sync mark keep going.
  *eosr = false;

  // We don't look at the keys, only the values.
  RETURN_IF_FALSE(SerDeUtils::SkipBytes(context_, current_key_length_, &parse_status_));

  if (header_->is_compressed) {
    int in_size = current_block_length_ - current_key_length_;
    // Check for a reasonable size
    if (in_size > context_->scan_range()->len() || in_size < 0) {
      stringstream ss;
      ss << "Compressed record size is: " << in_size;
      if (state_->LogHasSpace()) state_->LogError(ss.str());
      return Status(ss.str());
    }
    uint8_t* compressed_data;
    RETURN_IF_FALSE(
        SerDeUtils::ReadBytes(context_, in_size, &compressed_data, &parse_status_));

    int len = 0;
    RETURN_IF_ERROR(decompressor_->ProcessBlock(in_size, compressed_data,
        &len, &unparsed_data_buffer_));
    *record_ptr = unparsed_data_buffer_;
    // Read the length of the record.
    int size = SerDeUtils::GetVLong(*record_ptr, record_len);
    if (size == -1) {
        stringstream ss;
        ss << "Invalid record size";
        if (state_->LogHasSpace()) state_->LogError(ss.str());
        return Status(ss.str());
    }
    *record_ptr += size;
  } else {
    // Uncompressed records
    RETURN_IF_FALSE(SerDeUtils::ReadVLong(context_, record_len, &parse_status_));
    if (*record_len > context_->scan_range()->len() || *record_len < 0) {
      stringstream ss;
      ss << "Record length is: " << record_len;
      if (state_->LogHasSpace()) state_->LogError(ss.str());
      return Status(ss.str());
    }
    RETURN_IF_FALSE(
        SerDeUtils::ReadBytes(context_, *record_len, record_ptr, &parse_status_));
  }
  return Status::OK;
}

// Process block compressed sequence files.  This is the most used sequence file
// format.  The general strategy is to process the data in large chunks to minimize
// function calls.  The process is:
// 1. Decompress an entire block
// 2. In row batch sizes:
//   a. Collect the start of records and their lengths
//   b. Parse cols locations to field_locations_
//   c. Materialize those field locations to row batches
// This mimics the technique for text.
// This function only returns on error or when the entire scan range is complete.
Status HdfsSequenceScanner::ProcessBlockCompressedScanRange() {
  DCHECK(header_->is_blk_compressed);

  while (!context_->eosr() || num_buffered_records_in_compressed_block_ > 0) {
    if (num_buffered_records_in_compressed_block_ == 0) {
      if (context_->eosr()) return Status::OK;
      // No more decompressed data, decompress the next block
      RETURN_IF_ERROR(ReadCompressedBlock());
      if (num_buffered_records_in_compressed_block_ < 0) return parse_status_;
    }
    
    MemPool* pool;
    TupleRow* tuple_row;
    int64_t max_tuples = context_->GetMemory(&pool, &tuple_, &tuple_row);
    int num_to_commit = min(max_tuples, num_buffered_records_in_compressed_block_);
    num_buffered_records_in_compressed_block_ -= num_to_commit;

    if (scan_node_->materialized_slots().empty()) {
      // Handle case where there are no slots to materialize (e.g. count(*))
      num_to_commit = WriteEmptyTuples(context_, tuple_row, num_to_commit);
      if (num_to_commit > 0) context_->CommitRows(num_to_commit);
      continue;
    }

    // 2a. Parse record starts and lengths
    int field_location_offset = 0;
    for (int i = 0; i < num_to_commit; ++i) {
      DCHECK_LT(i, record_locations_.size());
      int bytes_read = SerDeUtils::GetVLong(
          next_record_in_compressed_block_, &record_locations_[i].len);
      if (UNLIKELY(bytes_read == -1)) {
        stringstream ss;
        ss << "Invalid record size in compressed block.";
        if (state_->LogHasSpace()) state_->LogError(ss.str());
        return Status(ss.str());
      }
      next_record_in_compressed_block_ += bytes_read;
      record_locations_[i].record = next_record_in_compressed_block_;
      next_record_in_compressed_block_ += record_locations_[i].len;
    }
      
    // 2b. Parse records to find field locations.
    for (int i = 0; i < num_to_commit; ++i) {
      int num_fields = 0;
      if (delimited_text_parser_->escape_char() == '\0') {
        delimited_text_parser_->ParseSingleTuple<false>(record_locations_[i].len, 
            reinterpret_cast<char*>(record_locations_[i].record), 
            &field_locations_[field_location_offset], &num_fields);
      } else {
        delimited_text_parser_->ParseSingleTuple<true>(record_locations_[i].len, 
            reinterpret_cast<char*>(record_locations_[i].record), 
            &field_locations_[field_location_offset], &num_fields);
      }
      DCHECK_EQ(num_fields, scan_node_->materialized_slots().size());
      field_location_offset += num_fields;
      DCHECK_LE(field_location_offset, field_locations_.size());
    }

    int max_added_tuples = (scan_node_->limit() == -1) ?
          num_to_commit : scan_node_->limit() - scan_node_->rows_returned();

    // Materialize parsed cols to tuples
    SCOPED_TIMER(scan_node_->materialize_tuple_timer());
    // Call jitted function if possible
    int tuples_returned;
    if (write_tuples_fn_ != NULL) {
      // last argument: seq always starts at record_location[0]
      tuples_returned = write_tuples_fn_(this, pool, tuple_row, 
          context_->row_byte_size(), &field_locations_[0], num_to_commit, 
          max_added_tuples, scan_node_->materialized_slots().size(), 0); 
    } else {
      tuples_returned = WriteAlignedTuples(pool, tuple_row, 
          context_->row_byte_size(), &field_locations_[0], num_to_commit, 
          max_added_tuples, scan_node_->materialized_slots().size(), 0);
    }

    if (tuples_returned == -1) return parse_status_;
    context_->CommitRows(tuples_returned);
  }

  return Status::OK;
}

Status HdfsSequenceScanner::ProcessRange() {
  // Block compressed is handled separately to minimize function calls.
  if (header_->is_blk_compressed) return ProcessBlockCompressedScanRange();
  
  // We count the time here since there is too much overhead to do
  // this on each record.
  SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  
  bool eosr = false;
  while (!eosr) {
    // Current record to process and its length.
    uint8_t* record = NULL;
    int64_t record_len = 0;
    // Get the next compressed or uncompressed record.
    RETURN_IF_ERROR(GetRecord(&record, &record_len, &eosr));

    if (eosr) {
      DCHECK(record == NULL);
      break;
    }

    MemPool* pool;
    TupleRow* tuple_row_mem;
    int max_tuples = context_->GetMemory(&pool, &tuple_, &tuple_row_mem);
    DCHECK_GT(max_tuples, 0);
    
    // Parse the current record.
    bool add_row = false;

    // Parse the current record.
    if (scan_node_->materialized_slots().size() != 0) {
      char* col_start;
      uint8_t* record_start = record;
      int num_tuples = 0;
      int num_fields = 0;
      char* row_end_loc;
      uint8_t error_in_row = false;

      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(
          1, record_len, reinterpret_cast<char**>(&record), &row_end_loc,
          &field_locations_[0], &num_tuples, &num_fields, &col_start));
      DCHECK(num_tuples == 1);
      
      uint8_t errors[num_fields];
      memset(errors, 0, sizeof(errors));

      add_row = WriteCompleteTuple(pool, &field_locations_[0], tuple_, tuple_row_mem,
          template_tuple_, &errors[0], &error_in_row);

      if (UNLIKELY(error_in_row)) {
        for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
          if (errors[i]) {
            const SlotDescriptor* desc = scan_node_->materialized_slots()[i];
            ReportColumnParseError(
                desc, field_locations_[i].start, field_locations_[i].len);
          }
        }
        // Report all the fields that have errors.
        ++num_errors_in_file_;
        if (state_->LogHasSpace()) {
          stringstream ss;
          ss << "file: " << context_->filename() << endl
             << "record: " << string(reinterpret_cast<char*>(record_start), record_len);
          state_->LogError(ss.str());
        }
        if (state_->abort_on_error()) {
          state_->ReportFileErrors(context_->filename(), 1);
          return Status(state_->ErrorLog());
        }
      }
    } else {
      add_row = WriteEmptyTuples(context_, tuple_row_mem, 1);
    }

    if (add_row) context_->CommitRows(1);
    if (scan_node_->ReachedLimit()) break;
  }

  return Status::OK;
}

Status HdfsSequenceScanner::ReadFileHeader() {
  uint8_t* header;

  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context_, 
      sizeof(SEQFILE_VERSION_HEADER), &header, &parse_status_));

  if (memcmp(header, SEQFILE_VERSION_HEADER, sizeof(SEQFILE_VERSION_HEADER))) {
    stringstream ss;
    ss << "Invalid SEQFILE_VERSION_HEADER: '"
       << SerDeUtils::HexDump(header, sizeof(SEQFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  // We don't care what this is since we don't use the keys.
  RETURN_IF_FALSE(SerDeUtils::SkipText(context_, &parse_status_));

  uint8_t* class_name;
  int len;
  RETURN_IF_FALSE(SerDeUtils::ReadText(context_, &class_name, &len, &parse_status_));
  if (memcmp(class_name, HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME, len)) {
    stringstream ss;
    ss << "Invalid SEQFILE_VALUE_CLASS_NAME: '"
       << string(reinterpret_cast<char*>(class_name), len) << "'";
    return Status(ss.str());
  }

  RETURN_IF_FALSE(
      SerDeUtils::ReadBoolean(context_, &header_->is_compressed, &parse_status_));
  RETURN_IF_FALSE(
      SerDeUtils::ReadBoolean(context_, &header_->is_blk_compressed, &parse_status_));
  
  if (header_->is_compressed) {
    uint8_t* codec_ptr;
    RETURN_IF_FALSE(SerDeUtils::ReadText(context_, &codec_ptr, &len, &parse_status_));
    header_->codec = string(reinterpret_cast<char*>(codec_ptr), len);
  }
  VLOG_FILE << context_->filename() << ": "
            << (header_->is_compressed ? 
                (header_->is_blk_compressed ?  "block compressed" : "record compressed") :
                "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;

  RETURN_IF_ERROR(ReadFileHeaderMetadata());
  RETURN_IF_ERROR(ReadSync());
  header_->header_size = context_->total_bytes_returned();
  return Status::OK;
}

Status HdfsSequenceScanner::ReadFileHeaderMetadata() {
  int map_size = 0;
  RETURN_IF_FALSE(SerDeUtils::ReadInt(context_, &map_size, &parse_status_));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_FALSE(SerDeUtils::SkipText(context_, &parse_status_));
    RETURN_IF_FALSE(SerDeUtils::SkipText(context_, &parse_status_));
  }
  return Status::OK;
}

inline Status HdfsSequenceScanner::ReadSync() {
  uint8_t* sync;
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context_, SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);
  return Status::OK;
}

Status HdfsSequenceScanner::ReadBlockHeader(bool* sync) {
  RETURN_IF_FALSE(SerDeUtils::ReadInt(context_, &current_block_length_, &parse_status_));
  *sync = false;
  if (current_block_length_ == HdfsSequenceScanner::SYNC_MARKER) {
    RETURN_IF_ERROR(CheckSync());
    RETURN_IF_FALSE(SerDeUtils::ReadInt(context_, &current_block_length_, &parse_status_));
    *sync = true;
  }
  if (current_block_length_ < 0) {
    stringstream ss;
    int64_t position = context_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad block length: " << current_block_length_;
    return Status(ss.str());
  }
  
  RETURN_IF_FALSE(SerDeUtils::ReadInt(context_, &current_key_length_, &parse_status_));
  if (current_key_length_ < 0) {
    stringstream ss;
    int64_t position = context_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad key length: " << current_key_length_;
    return Status(ss.str());
  }

  return Status::OK;
}

Status HdfsSequenceScanner::CheckSync() {
  uint8_t* hash;
  RETURN_IF_FALSE(SerDeUtils::ReadBytes(context_, 
        HdfsSequenceScanner::SYNC_HASH_SIZE, &hash, &parse_status_));

  bool sync_compares_equal = memcmp(static_cast<void*>(hash),
      static_cast<void*>(header_->sync), SYNC_HASH_SIZE) == 0;
  if (!sync_compares_equal) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss << "Bad sync hash in HdfsSequenceScanner at file offset " 
         << (context_->file_offset() - SYNC_HASH_SIZE) << "." << endl
         << "Expected: '"
         << SerDeUtils::HexDump(header_->sync, SYNC_HASH_SIZE)
         << "'" << endl
         << "Actual:   '"
         << SerDeUtils::HexDump(hash, SYNC_HASH_SIZE) << "'";
      state_->LogError(ss.str());
    }
    return Status("Bad sync hash");
  }
  return Status::OK;
}

Status HdfsSequenceScanner::SkipToSync() {
  bool eosr = false;
  int offset = SYNC_HASH_SIZE;
  int buffer_len;
  do {
    uint8_t* buffer;
  
    RETURN_IF_ERROR(context_->GetRawBytes(&buffer, &buffer_len, &eosr));
    offset = FindSyncBlock(buffer, buffer_len, header_->sync, SYNC_HASH_SIZE);
    DCHECK_LE(offset, buffer_len);

    // We need to check for a sync that spans buffers.
    if (offset == buffer_len) {
      // The marker (-1) and the sync can start anywhere in the
      // last 19 bytes of the buffer.  
      static const int tail_size = SYNC_HASH_SIZE + sizeof(int32_t) - 1;
      uint8_t* bp = buffer + buffer_len - tail_size;
      uint8_t save[2 * tail_size];

      // Save the tail of the buffer.
      memcpy(save, bp, tail_size);

      // Read the next buffer.
      RETURN_IF_FALSE(SerDeUtils::SkipBytes(context_, offset, &parse_status_));
      RETURN_IF_ERROR(context_->GetRawBytes(&buffer, &buffer_len, &eosr));
      offset = buffer_len;
      if (buffer_len >= tail_size) {
        memcpy(save + tail_size, buffer, tail_size);
        offset = FindSyncBlock(save, 2 * tail_size, header_->sync, SYNC_HASH_SIZE);

        // The sync mark does not span the buffers search the whole new buffer.
        if (offset == 2 * tail_size) {
          offset = buffer_len;
          continue;
        }

        have_sync_ = true;
        // Adjust the offset to be relative to the start of the new buffer
        offset -= tail_size;
        // Adjust offset to be past the sync since it spans buffers.
        offset += SYNC_HASH_SIZE + sizeof(int32_t);
      }
    }

    // Advance to the offset.  If have_sync_ is set then this is past the sync block.
    if (offset != 0) {
      RETURN_IF_FALSE(SerDeUtils::SkipBytes(context_, offset, &parse_status_));
    }
  } while (offset >= buffer_len && !eosr);

  if (!eosr) {
    VLOG_FILE << "Found sync for: " << context_->filename()
              << " at " << context_->file_offset() - (have_sync_ ? offset : 0);
  }

  return Status::OK;
}

Status HdfsSequenceScanner::ReadCompressedBlock() {
  // We are reading a new compressed block.  Pass the previous buffer pool 
  // bytes to the batch.  We don't need them anymore.
  if (!context_->compact_data()) {
    context_->AcquirePool(unparsed_data_buffer_pool_.get());
  }

  block_start_ = context_->file_offset();
  if (have_sync_) {
    // We skipped ahead on an error and read the sync block.
    have_sync_ = false;
  } else {
    // Read the sync indicator and check the sync block.
    int sync_indicator;
    RETURN_IF_FALSE(SerDeUtils::ReadInt(context_, &sync_indicator, &parse_status_));
    if (sync_indicator != -1) {
      if (state_->LogHasSpace()) {
        stringstream ss;
        ss << "Expecting sync indicator (-1) at file offset "
           << (context_->file_offset() - sizeof(int)) << ".  " 
           << "Sync indicator found " << sync_indicator << ".";
        state_->LogError(ss.str());
      }
      return Status("Bad sync hash");
    }
    RETURN_IF_ERROR(CheckSync());
  }

  RETURN_IF_FALSE(SerDeUtils::ReadVLong(context_, 
      &num_buffered_records_in_compressed_block_, &parse_status_));
  if (num_buffered_records_in_compressed_block_ < 0) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss << "Bad compressed block record count: "
         << num_buffered_records_in_compressed_block_;
      state_->LogError(ss.str());
    }
    return Status("bad record count");
  }

  // Skip the compressed key length and key buffers, we don't need them.
  RETURN_IF_FALSE(SerDeUtils::SkipText(context_, &parse_status_));
  RETURN_IF_FALSE(SerDeUtils::SkipText(context_, &parse_status_));

  // Skip the compressed value length buffer. We don't need these either since the
  // records are in Text format with length included.
  RETURN_IF_FALSE(SerDeUtils::SkipText(context_, &parse_status_));

  // Read the compressed value buffer from the unbuffered stream.
  int block_size = 0;
  RETURN_IF_FALSE(SerDeUtils::ReadVInt(context_, &block_size, &parse_status_));
  // Check for a reasonable size
  if (block_size > MAX_BLOCK_SIZE || block_size < 0) {
    stringstream ss;
    ss << "Compressed block size is: " << block_size;
    if (state_->LogHasSpace()) state_->LogError(ss.str());
    return Status(ss.str());
  }
  
  uint8_t* compressed_data = NULL;
  RETURN_IF_FALSE(
      SerDeUtils::ReadBytes(context_, block_size, &compressed_data, &parse_status_));

  int len = 0;
  SCOPED_TIMER(decompress_timer_);
  RETURN_IF_ERROR(decompressor_->ProcessBlock(block_size, compressed_data,
      &len, &unparsed_data_buffer_));
  next_record_in_compressed_block_ = unparsed_data_buffer_;
  return Status::OK;
}

void HdfsSequenceScanner::LogRowParseError(stringstream* ss, int row_idx) {
  DCHECK(state_->LogHasSpace());
  DCHECK_LT(row_idx, record_locations_.size());
  *ss << string(reinterpret_cast<const char*>(record_locations_[row_idx].record), 
                  record_locations_[row_idx].len);
}

