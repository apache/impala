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

#include "exec/parquet/parquet-level-decoder.h"
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
    string schema_name, int slot_id, ValueMemoryType value_mem_type,
    bool has_rep_level, bool has_def_level)
  : parent_(parent),
    schema_name_(schema_name),
    page_reader_(parent, schema_name),
    slot_id_(slot_id),
    data_page_pool_(new MemPool(parent->scan_node_->mem_tracker())),
    value_mem_type_(value_mem_type),
    has_rep_level_(has_rep_level),
    has_def_level_(has_def_level)
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

Status ParquetColumnChunkReader::ReadNextDataPageHeader(int* num_values) {
  // Read the next data page header, skipping page types we don't care about and empty
  // data pages. This method should be called after we know that the first page is not a
  // dictionary page. Therefore, if we find a dictionary page, it is an error in
  // the parquet file and we return a non-ok status (returned by
  // page_reader_.ReadPageHeader()).
  bool eos = false;
  *num_values = 0;
  bool next_data_page_found = false;
  while (!next_data_page_found) {
    RETURN_IF_ERROR(page_reader_.ReadPageHeader(&eos));

    const parquet::PageHeader& header = CurrentPageHeader();

    // If page_reader_.ReadPageHeader() > 0 and the page is a dictionary page then
    // ReadPageHeader would have returned an error.
    DCHECK(page_reader_.PageHeadersRead() > 0
        || !header.__isset.dictionary_page_header)
        << "Should not call this method on the first page if it is a dictionary.";

    if (eos) return Status::OK();

    if (header.type== parquet::PageType::DATA_PAGE
        || header.type == parquet::PageType::DATA_PAGE_V2) {
      bool is_v2 = header.type == parquet::PageType::DATA_PAGE_V2;
      int tmp_num_values = is_v2 ? header.data_page_header_v2.num_values
                                 : header.data_page_header.num_values;
      if (tmp_num_values < 0) {
        return Status(Substitute("Error reading data page in Parquet file '$0'. "
              "Invalid number of values in metadata: $1", filename(), tmp_num_values));
      } else if (tmp_num_values == 0) {
        // Skip pages with 0 values.
        VLOG_FILE << "Found empty page in " << filename();
        RETURN_IF_ERROR(SkipPageData());
      } else {
        *num_values = tmp_num_values;
        next_data_page_found = true;
      }
    } else {
      // We can safely skip non-data pages
      RETURN_IF_ERROR(SkipPageData());
    }
  }
  return Status::OK();
}

Status ParquetColumnChunkReader::ProcessRepDefLevelsInDataPageV1(
    const parquet::DataPageHeader* header_v1,  DataPageInfo* page_info,
    uint8_t** data, int* data_size) {
  page_info->rep_level_encoding = header_v1->repetition_level_encoding;
  page_info->def_level_encoding = header_v1->definition_level_encoding;

  int32_t rep_level_size = 0;
  if (has_rep_level_) {
    RETURN_IF_ERROR(ParquetLevelDecoder::ValidateEncoding(
        filename(), page_info->rep_level_encoding));
    RETURN_IF_ERROR(ParquetLevelDecoder::ParseRleByteSize(
        filename(), data, data_size, &rep_level_size));
  }
  page_info->rep_level_ptr = *data;
  page_info->rep_level_size = rep_level_size;
  *data += rep_level_size;
  *data_size -= rep_level_size;

  int32_t def_level_size = 0;
  if (has_def_level_) {
    RETURN_IF_ERROR(ParquetLevelDecoder::ValidateEncoding(
        filename(), page_info->def_level_encoding));
    RETURN_IF_ERROR(ParquetLevelDecoder::ParseRleByteSize(
        filename(), data, data_size, &def_level_size));
  }
  page_info->def_level_ptr = *data;
  page_info->def_level_size = def_level_size;
  *data += def_level_size;
  *data_size -= def_level_size;

  return Status::OK();
}

Status ParquetColumnChunkReader::ProcessRepDefLevelsInDataPageV2(
    const parquet::DataPageHeaderV2* header_v2,
    DataPageInfo* page_info, uint8_t* data, int max_size) {
  int rep_level_size = header_v2->repetition_levels_byte_length;
  int def_level_size = header_v2->definition_levels_byte_length;
  if (rep_level_size < 0 || def_level_size < 0
      || rep_level_size + def_level_size > max_size) {
    return Status(Substitute("Corrupt rep/def level sizes in v2 data page in file '$0'. "
        "rep level size: $1 def level size: $2 max size: $3",
        filename(), rep_level_size, def_level_size, max_size));
  }

  page_info->rep_level_size = rep_level_size;
  page_info->def_level_size = def_level_size;
  page_info->rep_level_ptr = data;
  page_info->def_level_ptr = data + rep_level_size;
  // v2 pages always use RLE for rep/def levels.
  page_info->rep_level_encoding = parquet::Encoding::RLE;
  page_info->def_level_encoding = parquet::Encoding::RLE;

  return Status::OK();
}

Status ParquetColumnChunkReader::ReadDataPageData(DataPageInfo* page_info) {
  DCHECK(page_info != nullptr);
  page_info->is_valid = false;

  const parquet::PageHeader& header = CurrentPageHeader();
  bool is_v2 = header.type == parquet::PageType::DATA_PAGE_V2;
  DCHECK(is_v2 || header.type == parquet::PageType::DATA_PAGE);

  // In v2 pages if decompressor_ == nullptr it is still possible that is_compressed
  // is true in the header (parquet-mr writes like this if compression=UNCOMPRESSED).
  bool is_compressed = decompressor_.get() != nullptr
      && (!is_v2 || header.data_page_header_v2.is_compressed);

  int orig_compressed_size = header.compressed_page_size;
  int orig_uncompressed_size = header.uncompressed_page_size;
  int compressed_size = orig_compressed_size;
  int uncompressed_size = orig_uncompressed_size;

  // Read compressed data.
  uint8_t* compressed_data;
  RETURN_IF_ERROR(page_reader_.ReadPageData(&compressed_data));

  // If v2 data page, fill rep/def level info based on header. For v1 pages this will be
  // done after decompression.
  if (is_v2) {
    RETURN_IF_ERROR(ProcessRepDefLevelsInDataPageV2(&header.data_page_header_v2,
        page_info, compressed_data, orig_uncompressed_size));
    // In v2 pages compressed_page_size size also includes the uncompressed
    // rep/def levels.
    // https://github.com/apache/parquet-format/blob/2a481fe1aad64ff770e21734533bb7ef5a057dac/src/main/thrift/parquet.thrift#L578
    int levels_size = page_info->rep_level_size + page_info->def_level_size;
    compressed_size -= levels_size;
    uncompressed_size -= levels_size;
    compressed_data += levels_size;
  }

  const bool has_slot_desc = value_mem_type_ != ValueMemoryType::NO_SLOT_DESC;

  int data_size = uncompressed_size;
  uint8_t* data = nullptr;
  if (is_compressed) {
    SCOPED_TIMER(parent_->decompress_timer_);
    uint8_t* decompressed_buffer;
    RETURN_IF_ERROR(AllocateUncompressedDataPage(
        uncompressed_size, "decompressed data", &decompressed_buffer));
    int actual_uncompressed_size = uncompressed_size;
    RETURN_IF_ERROR(decompressor_->ProcessBlock32(true,
        compressed_size, compressed_data, &actual_uncompressed_size,
        &decompressed_buffer));
    // TODO: can't we call stream_->ReleaseCompletedResources(false); at this point?
    //       (we can't in v2 data page as the original buffer contains rep/def levels)
    VLOG_FILE << "Decompressed " << compressed_size
              << " to " << actual_uncompressed_size;
    if (uncompressed_size != actual_uncompressed_size) {
      return Status(Substitute("Error decompressing data page in file '$0'. "
          "Expected $1 uncompressed bytes but got $2", filename(),
          uncompressed_size, actual_uncompressed_size));
    }
    data = decompressed_buffer;

    if (has_slot_desc) {
      // Use original sizes (includes levels in v2) in the profile.
      parent_->scan_node_->UpdateBytesRead(
          slot_id_, orig_uncompressed_size, orig_compressed_size);
      parent_->UpdateUncompressedPageSizeCounter(orig_uncompressed_size);
      parent_->UpdateCompressedPageSizeCounter(orig_compressed_size);
    }
  } else {
    if (compressed_size != uncompressed_size) {
      return Status(Substitute("Error reading data page in file '$0'. "
          "Compressed size ($1) should be the same as uncompressed size ($2) "
          "in pages without compression.", filename(),
          compressed_size, uncompressed_size));
    }

    // TODO: could skip copying when the data page is dict encoded as strings
    //       will point to the dictionary instead of the data buffer (IMPALA-12137)
    const bool copy_buffer = value_mem_type_ == ValueMemoryType::VAR_LEN_STR;

    if (copy_buffer) {
      // In this case returned batches will have pointers into the data page itself.
      // We don't transfer disk I/O buffers out of the scanner so we need to copy
      // the page data so that it can be attached to output batches.
      uint8_t* buffer;
      RETURN_IF_ERROR(AllocateUncompressedDataPage(
          uncompressed_size, "uncompressed variable-length data", &buffer));
      memcpy(buffer, compressed_data, uncompressed_size);
      data = buffer;
    } else {
      data = compressed_data;
    }
    if (has_slot_desc) {
      // Use original sizes (includes levels in v2) in the profile.
      parent_->scan_node_->UpdateBytesRead(slot_id_, orig_uncompressed_size, 0);
      parent_->UpdateUncompressedPageSizeCounter(orig_uncompressed_size);
    }

  }
  // The buffers to return are ready at this point.

  // If v1 data page, fill rep/def level info by parsing the beginning of the data. For
  // v2 pages this was done before decompression.
  if (!is_v2) {
    RETURN_IF_ERROR(ProcessRepDefLevelsInDataPageV1(&header.data_page_header, page_info,
        &data, &data_size));
  }

  page_info->data_encoding = is_v2 ? header.data_page_header_v2.encoding
                                   : header.data_page_header.encoding;
  page_info->data_ptr = data;
  page_info->data_size = data_size;
  page_info->is_valid = true;

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
