// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-sequence-scanner.h"

#include "exec/buffered-byte-stream.h"
#include "exec/delimited-text-parser.h"
#include "exec/hdfs-scan-node.h"
#include "exec/scan-range-context.h"
#include "exec/serde-utils.inline.h"
#include "exec/text-converter.inline.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/string-search.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

using namespace std;
using namespace boost;
using namespace impala;

const char* const HdfsSequenceScanner::SEQFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.io.BytesWritable";

const char* const HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.io.Text";

const int HdfsSequenceScanner::HEADER_SIZE = 1024;

const uint8_t HdfsSequenceScanner::SEQFILE_VERSION_HEADER[4] = {'S', 'E', 'Q', 6};

static const uint8_t SEQUENCE_FILE_RECORD_DELIMITER = 0xff;

HdfsSequenceScanner::HdfsSequenceScanner(HdfsScanNode* scan_node, RuntimeState* state) 
    : HdfsScanner(scan_node, state, NULL),
      header_(NULL),
      unparsed_data_buffer_pool_(new MemPool()),
      unparsed_data_buffer_(NULL),
      num_buffered_records_in_compressed_block_(0) {
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

  // Process Range
  if (found) {
    RETURN_IF_ERROR(ProcessRange());
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
  return Status::OK;
}

Status HdfsSequenceScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());

  // Allocate the scratch space for two pass parsing.  The most fields we can go
  // through in one parse pass is the batch size (tuples) * the number of fields per tuple
  // TODO: This should probably be based on L2/L3 cache sizes (as should the batch size)
  field_locations_.resize(state_->batch_size() * scan_node_->materialized_slots().size());

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
  if (context_->scan_range()->offset() == 0) {
    // scan range that starts at the beginning of the file, just skip ahead the header 
    // size.
    SerDeUtils::SkipBytes(context_, header_->header_size);
    *found = true;
    return Status::OK;
  }

  *found = false;
  // Look for the first sync block
  bool eosr;
  while (!eosr) {
    uint8_t* buffer;
    int buffer_len;
    
    RETURN_IF_ERROR(context_->GetRawBytes(&buffer, &buffer_len, &eosr));
    int offset = FindSyncBlock(buffer, buffer_len, header_->sync, SYNC_HASH_SIZE);
    DCHECK_LE(offset, buffer_len);

    // Advance to the offset
    RETURN_IF_ERROR(SerDeUtils::SkipBytes(context_, offset));
    if (offset < buffer_len) {
      // Found sync marker, advance to it
      *found = true;
      return Status::OK;
    } 
  }
  return Status::OK;
}

inline Status HdfsSequenceScanner::GetRecordFromCompressedBlock(
    uint8_t** record_ptr, int64_t* record_len) {
  if (num_buffered_records_in_compressed_block_ == 0) {
    if (context_->eosr()) return Status::OK;
    RETURN_IF_ERROR(ReadCompressedBlock());
  }
  // Adjust next_record_ to move past the size of the length indicator.
  int size = SerDeUtils::GetVLong(next_record_in_compressed_block_, record_len);
  next_record_in_compressed_block_ += size;
  *record_ptr = next_record_in_compressed_block_;
  // Point to the length of the next record.
  next_record_in_compressed_block_ += *record_len;
  --num_buffered_records_in_compressed_block_;
  return Status::OK;
}

inline Status HdfsSequenceScanner::GetRecord(uint8_t** record_ptr, int64_t* record_len) {
  // If we are past the end of the range we must read to the next sync block.
  bool sync;
  Status stat = ReadBlockHeader(&sync);
  if (!stat.ok()) {
    if (context_->eosr()) return Status::OK;
    return stat;
  }
  if (sync && context_->eosr()) return Status::OK;

  // We don't look at the keys, only the values.
  RETURN_IF_ERROR(SerDeUtils::SkipBytes(context_, current_key_length_));

  if (header_->is_compressed) {
    int in_size = current_block_length_ - current_key_length_;
    uint8_t* compressed_data;
    RETURN_IF_ERROR(SerDeUtils::ReadBytes(context_, in_size, &compressed_data));

    int len = 0;
    RETURN_IF_ERROR(decompressor_->ProcessBlock(in_size, compressed_data,
        &len, &unparsed_data_buffer_));
    *record_ptr = unparsed_data_buffer_;
    // Read the length of the record.
    int size = SerDeUtils::GetVLong(*record_ptr, record_len);
    *record_ptr += size;
  } else {
    // Uncompressed records
    RETURN_IF_ERROR(SerDeUtils::ReadVLong(context_, record_len));
    RETURN_IF_ERROR(SerDeUtils::ReadBytes(context_, *record_len, record_ptr));
  }
  return Status::OK;
}

Status HdfsSequenceScanner::ProcessRange() {
  // We count the time here since there is too much overhead to do
  // this on each record.
  COUNTER_SCOPED_TIMER(scan_node_->materialize_tuple_timer());
  
  while (!context_->eosr() || num_buffered_records_in_compressed_block_ > 0) {
    // Current record to process and its length.
    uint8_t* record = NULL;
    int64_t record_len;
    // Get the next record and record length.
    // There are 3 cases:
    //  Block-compressed -- each block contains several records.
    //  Record-compressed -- like a regular record, but the data is compressed.
    //  Uncompressed.
    if (header_->is_blk_compressed) {
      RETURN_IF_ERROR(GetRecordFromCompressedBlock(&record, &record_len));
    } else {
      // Get the next compressed or uncompressed record.
      RETURN_IF_ERROR(GetRecord(&record, &record_len));
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

      RETURN_IF_ERROR(delimited_text_parser_->ParseFieldLocations(
          1, record_len, reinterpret_cast<char**>(&record), &row_end_loc,
          &field_locations_, &num_tuples, &num_fields, &col_start));
      DCHECK(num_tuples == 1);

      if (!WriteFields(pool, tuple_row_mem, num_fields, &add_row).ok()) {
        // Report all the fields that have errors.
        unique_lock<mutex> l(state_->errors_lock());
        ++num_errors_in_file_;
        if (state_->LogHasSpace()) {
          state_->error_stream() << "file: " << context_->filename() << endl;
          state_->error_stream() << "record: ";
          state_->error_stream()
              << string(reinterpret_cast<char*>(record_start), record_len);
          state_->LogErrorStream();
        }
        if (state_->abort_on_error()) {
          state_->ReportFileErrors(context_->filename(), 1);
          return Status("Aborted HdfsSequenceScanner due to parse errors."
                        "View error log for details.");
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

// TODO: apply conjuncts as slots get materialized and skip to the end of the row
// if we determine it's not a match.
Status HdfsSequenceScanner::WriteFields(MemPool* pool, TupleRow* row, 
    int num_fields, bool* add_row) {
  DCHECK_EQ(num_fields, scan_node_->materialized_slots().size());

  // Keep track of where lines begin as we write out fields for error reporting
  int next_line_offset = 0;

  // Initialize tuple_ from the partition key template tuple before writing the slots
  InitTuple(template_tuple_, tuple_);

  // Loop through all the parsed_data and parse out the values to slots
  bool error_in_row = false;
  for (int n = 0; n < num_fields; ++n) {
    int need_escape = false;
    int len = field_locations_[n].len;
    if (len < 0) {
      len = -len;
      need_escape = true;
    }
    next_line_offset += (len + 1);

    const SlotDescriptor* desc = scan_node_->materialized_slots()[n];
    if (!text_converter_->WriteSlot(desc, tuple_,
        reinterpret_cast<char*>(field_locations_[n].start),
        len, context_->compact_data(), need_escape, pool)) {
      ReportColumnParseError(desc, field_locations_[n].start, len);
      error_in_row = true;
    }
  }

  // Set tuple in tuple row and evaluate the conjuncts
  row->SetTuple(tuple_idx_, tuple_);
  *add_row = ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, row);

  if (error_in_row) return Status("Conversion from string failed");
  return Status::OK;
}

Status HdfsSequenceScanner::ReadFileHeader() {
  uint8_t* header;

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context_, sizeof(SEQFILE_VERSION_HEADER), &header));
  Status status;
  if (memcmp(header, SEQFILE_VERSION_HEADER, sizeof(SEQFILE_VERSION_HEADER))) {
    stringstream ss;
    ss << "Invalid SEQFILE_VERSION_HEADER: '"
       << SerDeUtils::HexDump(header, sizeof(SEQFILE_VERSION_HEADER)) << "'" << endl;
    status.AddErrorMsg(ss.str());
  }

  // We don't care what this is since we don't use the keys.
  RETURN_IF_ERROR(SerDeUtils::SkipText(context_));

  uint8_t* class_name;
  int len;
  RETURN_IF_ERROR(SerDeUtils::ReadText(context_, &class_name, &len));
  if (memcmp(class_name, HdfsSequenceScanner::SEQFILE_VALUE_CLASS_NAME, len)) {
    stringstream ss;
    ss << "Invalid SEQFILE_VALUE_CLASS_NAME: '"
       << string(reinterpret_cast<char*>(class_name), len) << "'" << endl;
    status.AddErrorMsg(ss.str());
  }

  if (!status.ok()) {
    stringstream ss;
    ss << "Invalid header information: " << context_->filename();
    status.AddErrorMsg(ss.str());
    return status;
  }

  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(context_, &header_->is_compressed));
  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(context_, &header_->is_blk_compressed));
  
  if (header_->is_compressed) {
    uint8_t* codec_ptr;
    RETURN_IF_ERROR(SerDeUtils::ReadText(context_, &codec_ptr, &len));
    header_->codec = string(reinterpret_cast<char*>(codec_ptr), len);
  }
  VLOG_FILE << context_->filename() << ": "
            << (header_->is_compressed ? 
                (header_->is_blk_compressed ?  "block compressed" : "record compresed") : 
                "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;

  RETURN_IF_ERROR(ReadFileHeaderMetadata());
  RETURN_IF_ERROR(ReadSync());
  header_->header_size = context_->total_bytes_returned();
  return Status::OK;
}

Status HdfsSequenceScanner::ReadFileHeaderMetadata() {
  int map_size = 0;
  RETURN_IF_ERROR(SerDeUtils::ReadInt(context_, &map_size));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_ERROR(SerDeUtils::SkipText(context_));
    RETURN_IF_ERROR(SerDeUtils::SkipText(context_));
  }
  return Status::OK;
}

inline Status HdfsSequenceScanner::ReadSync() {
  uint8_t* sync;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context_, SYNC_HASH_SIZE, &sync));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);
  return Status::OK;
}

Status HdfsSequenceScanner::ReadBlockHeader(bool* sync) {
  RETURN_IF_ERROR(SerDeUtils::ReadInt(context_, &current_block_length_));
  *sync = false;
  if (current_block_length_ == HdfsSequenceScanner::SYNC_MARKER) {
    RETURN_IF_ERROR(CheckSync());
    RETURN_IF_ERROR(SerDeUtils::ReadInt(context_, &current_block_length_));
    *sync = true;
  }
  if (current_block_length_ < 0) {
    stringstream ss;
    int64_t position = context_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad block length: " << current_block_length_ << " in file: "
       << context_->filename() << " at offset: " << position;
    return Status(ss.str());
  }
  
  RETURN_IF_ERROR(SerDeUtils::ReadInt(context_, &current_key_length_));
  if (current_key_length_ < 0) {
    stringstream ss;
    int64_t position = context_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad key length: " << current_key_length_ << " in file: "
       << context_->filename() << " at offset: " << position;
    return Status(ss.str());
  }

  return Status::OK;
}

Status HdfsSequenceScanner::CheckSync() {
  uint8_t* hash;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context_, 
        HdfsSequenceScanner::SYNC_HASH_SIZE, &hash));

  bool sync_compares_equal = memcmp(static_cast<void*>(hash),
      static_cast<void*>(header_->sync), HdfsSequenceScanner::SYNC_HASH_SIZE) == 0;
  if (!sync_compares_equal) {
    unique_lock<mutex> l(state_->errors_lock());
    if (state_->LogHasSpace()) {
      state_->error_stream() << "Bad sync hash in current HdfsSequenceScanner: "
           << context_->filename() << "." << endl
           << "Expected: '"
           << SerDeUtils::HexDump(header_->sync, HdfsSequenceScanner::SYNC_HASH_SIZE)
           << "'" << endl
           << "Actual:   '"
           << SerDeUtils::HexDump(hash, HdfsSequenceScanner::SYNC_HASH_SIZE)
           << "'" << endl;
      state_->LogErrorStream();
    }
    return Status("Bad sync hash");
  }
  return Status::OK;
}

Status HdfsSequenceScanner::ReadCompressedBlock() {
  // We are reading a new compressed block.  Pass the previous buffer pool 
  // bytes to the batch.  We don't need them anymore.
  if (!context_->compact_data()) {
    context_->AcquirePool(unparsed_data_buffer_pool_.get());
  }

  // Read the sync indicator and check the sync block.
  RETURN_IF_ERROR(SerDeUtils::SkipBytes(context_, sizeof(uint32_t)));
  RETURN_IF_ERROR(CheckSync());

  RETURN_IF_ERROR(SerDeUtils::ReadVLong(context_, 
      &num_buffered_records_in_compressed_block_));

  // Read the compressed key length and key buffers, we don't need them.
  RETURN_IF_ERROR(SerDeUtils::SkipText(context_));
  RETURN_IF_ERROR(SerDeUtils::SkipText(context_));

  // Read the compressed value length buffer. We don't need these either since the
  // records are in Text format with length included.
  RETURN_IF_ERROR(SerDeUtils::SkipText(context_));

  // Read the compressed value buffer from the unbuffered stream.
  int block_size = 0;
  RETURN_IF_ERROR(SerDeUtils::ReadVInt(context_, &block_size));
  uint8_t* compressed_data = NULL;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(context_, block_size, &compressed_data));

  int len = 0;
  RETURN_IF_ERROR(decompressor_->ProcessBlock(block_size, compressed_data,
      &len, &unparsed_data_buffer_));
  next_record_in_compressed_block_ = unparsed_data_buffer_;
  return Status::OK;
}
