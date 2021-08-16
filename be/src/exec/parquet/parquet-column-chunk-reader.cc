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

#include "exec/parquet/parquet-column-chunk-reader.h"

#include <string>

#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/scoped-buffer.h"
#include "util/codec.h"

#include "common/names.h"

using namespace impala::io;

using parquet::Encoding;

namespace impala {

const string PARQUET_PAGE_MEM_LIMIT_EXCEEDED =
    "ParquetColumnChunkReader::$0() failed to allocate $1 bytes for $2.";

// In 1.1, we had a bug where the dictionary page metadata was not set. Returns true
// if this matches those versions and compatibility workarounds need to be used.
static bool RequiresSkippedDictionaryHeaderCheck(
    const ParquetFileVersion& v) {
  if (v.application != "impala") return false;
  return v.VersionEq(1,1,0) || (v.VersionEq(1,2,0) && v.is_impala_internal);
}

ParquetColumnChunkReader::ParquetColumnChunkReader(HdfsParquetScanner* parent,
    string schema_name, int slot_id, ValueMemoryType value_mem_type)
  : parent_(parent),
    schema_name_(schema_name),
    page_reader_(parent, schema_name),
    slot_id_(slot_id),
    data_page_pool_(new MemPool(parent->scan_node_->mem_tracker())),
    value_mem_type_(value_mem_type)
{
}

ParquetColumnChunkReader::~ParquetColumnChunkReader() {}

Status ParquetColumnChunkReader::InitColumnChunk(const HdfsFileDesc& file_desc,
    const parquet::ColumnChunk& col_chunk, int row_group_idx,
    std::vector<io::ScanRange::SubRange>&& sub_ranges) {
  if (col_chunk.meta_data.codec != parquet::CompressionCodec::UNCOMPRESSED) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(nullptr, false,
        ConvertParquetToImpalaCodec(col_chunk.meta_data.codec), &decompressor_));
  }

  RETURN_IF_ERROR(page_reader_.InitColumnChunk(file_desc, col_chunk,
        row_group_idx, move(sub_ranges)));

  return Status::OK();
}

void ParquetColumnChunkReader::Close(MemPool* mem_pool) {
  if (mem_pool != nullptr && value_mem_type_ == ValueMemoryType::VAR_LEN_STR) {
    mem_pool->AcquireData(data_page_pool_.get(), false);
  } else {
    data_page_pool_->FreeAll();
  }

  if (decompressor_ != nullptr) decompressor_->Close();
}

void ParquetColumnChunkReader::ReleaseResourcesOfLastPage(MemPool& mem_pool) {
  if (value_mem_type_ == ValueMemoryType::VAR_LEN_STR) {
    mem_pool.AcquireData(data_page_pool_.get(), false);
  } else {
    data_page_pool_->FreeAll();
  }
  // We don't hold any pointers to earlier pages in the stream - we can safely free
  // any I/O or boundary buffer.
  // TODO: is this really needed? The read in ReadPageHeader() will release these
  // resources anyway.
  stream()->ReleaseCompletedResources(false);
}

Status ParquetColumnChunkReader::StartScan() {
  DCHECK_GT(io_reservation_, 0);
  RETURN_IF_ERROR(page_reader_.StartScan(io_reservation_));
  return Status::OK();
}

Status ParquetColumnChunkReader::SkipPageData() {
  return page_reader_.SkipPageData();
}

Status ParquetColumnChunkReader::TryReadDictionaryPage(bool* is_dictionary_page,
    bool* eos, bool skip_data, ScopedBuffer* uncompressed_buffer, uint8_t** dict_values,
    int64_t* data_size, int* num_entries) {
  RETURN_IF_ERROR(page_reader_.ReadPageHeader(eos));

  *is_dictionary_page = CurrentPageHeader().__isset.dictionary_page_header;
  if (*eos || !(*is_dictionary_page)) return Status::OK();

  if (skip_data) return SkipPageData();

  RETURN_IF_ERROR(ReadDictionaryData(uncompressed_buffer, dict_values,
        data_size, num_entries));

  return Status::OK();
}

Status ParquetColumnChunkReader::ReadDictionaryData(ScopedBuffer* uncompressed_buffer,
    uint8_t** dict_values, int64_t* data_size, int* num_entries) {
  const parquet::PageHeader& current_page_header = CurrentPageHeader();
  const parquet::DictionaryPageHeader* dict_header = nullptr;
  if (current_page_header.__isset.dictionary_page_header) {
    dict_header = &current_page_header.dictionary_page_header;
  } else {
    if (!RequiresSkippedDictionaryHeaderCheck(parent_->file_version_)) {
      return Status("Dictionary page does not have dictionary header set.");
    }
  }
  // Check that the dictionary page is PLAIN encoded. PLAIN_DICTIONARY in the context of
  // a dictionary page means the same thing.
  if (dict_header != nullptr &&
      dict_header->encoding != Encoding::PLAIN &&
      dict_header->encoding != Encoding::PLAIN_DICTIONARY) {
    return Status("Only PLAIN and PLAIN_DICTIONARY encodings are supported "
                  "for dictionary pages.");
  }

  *data_size = current_page_header.compressed_page_size;
  uint8_t* data = nullptr;
  RETURN_IF_ERROR(page_reader_.ReadPageData(&data));

  if (current_page_header.uncompressed_page_size == 0) {
    // The size of dictionary can be 0, if every value is null.
    *data_size = 0;
    *num_entries = 0;
    *dict_values = nullptr;

    return Status::OK();
  }

  // There are 3 different cases from the aspect of memory management:
  // 1. If the column type is string, the dictionary will contain pointers to a buffer,
  //    so the buffer's lifetime must be as long as any row batch that references it.
  // 2. If the column type is not string, and the dictionary page is compressed, then a
  //    temporary buffer is needed for the uncompressed values.
  // 3. If the column type is not string, and the dictionary page is not compressed,
  //    then no buffer is necessary.
  const bool copy_buffer = value_mem_type_ == ValueMemoryType::FIXED_LEN_STR
      || value_mem_type_ == ValueMemoryType::VAR_LEN_STR;

  if (decompressor_.get() != nullptr || copy_buffer) {
    int buffer_size = current_page_header.uncompressed_page_size;
    if (copy_buffer) {
      *dict_values = parent_->dictionary_pool_->TryAllocate(buffer_size); // case 1.
    } else if (uncompressed_buffer->TryAllocate(buffer_size)) {
      *dict_values = uncompressed_buffer->buffer(); // case 2
    }
    if (UNLIKELY(*dict_values == nullptr)) {
      string details = Substitute(PARQUET_PAGE_MEM_LIMIT_EXCEEDED, "InitDictionary",
          buffer_size, "dictionary");
      return parent_->dictionary_pool_->mem_tracker()->MemLimitExceeded(
               parent_->state_, details, buffer_size);
    }
  } else {
    *dict_values = data; // case 3.
  }

  if (decompressor_.get() != nullptr) {
    int uncompressed_size = current_page_header.uncompressed_page_size;
    RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, *data_size, data,
                    &uncompressed_size, dict_values));
    VLOG_FILE << "Decompressed " << *data_size << " to " << uncompressed_size;
    if (current_page_header.uncompressed_page_size != uncompressed_size) {
      return Status(Substitute("Error decompressing dictionary page in file '$0'. "
               "Expected $1 uncompressed bytes but got $2", filename(),
               current_page_header.uncompressed_page_size, uncompressed_size));
    }
    *data_size = uncompressed_size;
  } else {
    if (current_page_header.uncompressed_page_size != *data_size) {
      return Status(Substitute("Error reading dictionary page in file '$0'. "
                               "Expected $1 bytes but got $2", filename(),
                               current_page_header.uncompressed_page_size, *data_size));
    }
    if (copy_buffer) memcpy(*dict_values, data, *data_size);
  }
  *num_entries = dict_header->num_values;

  return Status::OK();
}

Status ParquetColumnChunkReader::ReadNextDataPage(
    bool* eos, uint8_t** data, int* data_size, bool read_data) {
  // Read the next data page, skipping page types we don't care about. This method should
  // be called after we know that the first page is not a dictionary page. Therefore, if
  // we find a dictionary page, it is an error in the parquet file and we return a non-ok
  // status (returned by page_reader_.ReadPageHeader()).
  bool next_data_page_found = false;
  while (!next_data_page_found) {
    RETURN_IF_ERROR(page_reader_.ReadPageHeader(eos));

    const parquet::PageHeader current_page_header = CurrentPageHeader();
    DCHECK(page_reader_.PageHeadersRead() > 0
        || !current_page_header.__isset.dictionary_page_header)
        << "Should not call this method on the first page if it is a dictionary.";

    if (*eos) return Status::OK();

    if (current_page_header.type == parquet::PageType::DATA_PAGE) {
      next_data_page_found = true;
    } else {
      // We can safely skip non-data pages
      RETURN_IF_ERROR(SkipPageData());
    }
  }
  if (read_data) {
    return ReadDataPageData(data, data_size);
  } else {
    return Status::OK();
  }
}

Status ParquetColumnChunkReader::ReadDataPageData(uint8_t** data, int* data_size) {
  const parquet::PageHeader& current_page_header = CurrentPageHeader();

  int compressed_size = current_page_header.compressed_page_size;
  int uncompressed_size = current_page_header.uncompressed_page_size;
  uint8_t* compressed_data;

  RETURN_IF_ERROR(page_reader_.ReadPageData(&compressed_data));

  const bool has_slot_desc = value_mem_type_ != ValueMemoryType::NO_SLOT_DESC;

  *data_size = uncompressed_size;
  if (decompressor_.get() != nullptr) {
    SCOPED_TIMER(parent_->decompress_timer_);
    uint8_t* decompressed_buffer;
    RETURN_IF_ERROR(AllocateUncompressedDataPage(
        uncompressed_size, "decompressed data", &decompressed_buffer));
    RETURN_IF_ERROR(decompressor_->ProcessBlock32(true,
        compressed_size, compressed_data, &uncompressed_size,
        &decompressed_buffer));
    // TODO: can't we call stream_->ReleaseCompletedResources(false); at this point?
    VLOG_FILE << "Decompressed " << current_page_header.compressed_page_size
              << " to " << uncompressed_size;
    if (current_page_header.uncompressed_page_size != uncompressed_size) {
      return Status(Substitute("Error decompressing data page in file '$0'. "
          "Expected $1 uncompressed bytes but got $2", filename(),
          current_page_header.uncompressed_page_size, uncompressed_size));
    }
    *data = decompressed_buffer;

    if (has_slot_desc) {
      parent_->scan_node_->UpdateBytesRead(slot_id_, uncompressed_size, compressed_size);
      parent_->UpdateUncompressedPageSizeCounter(uncompressed_size);
      parent_->UpdateCompressedPageSizeCounter(compressed_size);
    }
  } else {
    if (compressed_size != uncompressed_size) {
      return Status(Substitute("Error reading data page in file '$0'. "
          "Expected $1 bytes but got $2", filename(),
          compressed_size, uncompressed_size));
    }

    const bool copy_buffer = value_mem_type_ == ValueMemoryType::VAR_LEN_STR;

    if (copy_buffer) {
      // In this case returned batches will have pointers into the data page itself.
      // We don't transfer disk I/O buffers out of the scanner so we need to copy
      // the page data so that it can be attached to output batches.
      uint8_t* buffer;
      RETURN_IF_ERROR(AllocateUncompressedDataPage(
          uncompressed_size, "uncompressed variable-length data", &buffer));
      memcpy(buffer, compressed_data, uncompressed_size);
      *data = buffer;
    } else {
      *data = compressed_data;
    }
    if (has_slot_desc) {
      parent_->scan_node_->UpdateBytesRead(slot_id_, uncompressed_size, 0);
      parent_->UpdateUncompressedPageSizeCounter(uncompressed_size);
    }
  }

  return Status::OK();
}

Status ParquetColumnChunkReader::AllocateUncompressedDataPage(int64_t size,
    const char* err_ctx, uint8_t** buffer) {
  *buffer = data_page_pool_->TryAllocate(size);
  if (*buffer == nullptr) {
    string details =
        Substitute(PARQUET_PAGE_MEM_LIMIT_EXCEEDED, "ReadDataPage", size, err_ctx);
    return data_page_pool_->mem_tracker()->MemLimitExceeded(
        parent_->state_, details, size);
  }
  return Status::OK();
}

}
