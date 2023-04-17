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

#include "exec/parquet/parquet-level-decoder.h"

#include "exec/read-write-util.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "util/bit-util.h"
#include "util/ubsan.h"

#include "common/names.h"

using parquet::Encoding;

namespace impala {

const int16_t ParquetLevel::ROW_GROUP_END;
const int16_t ParquetLevel::INVALID_LEVEL;
const int16_t ParquetLevel::INVALID_POS;

Status ParquetLevelDecoder::ValidateEncoding(const string& filename,
    const Encoding::type encoding) {
  if (Ubsan::EnumToInt(&encoding) > Encoding::MAX_ENUM_VALUE) {
    stringstream ss;
    ss << "Unsupported encoding: " << Ubsan::EnumToInt(&encoding);
    return Status(ss.str());
  }
  switch (encoding) {
    case Encoding::RLE:
      return Status::OK();
    case Encoding::BIT_PACKED:
      return Status(TErrorCode::PARQUET_BIT_PACKED_LEVELS, filename);
    default: {
      stringstream ss;
      ss << "Unsupported encoding: " << encoding;
      return Status(ss.str());
    }
  }
}

Status ParquetLevelDecoder::ParseRleByteSize(const string& filename,
    uint8_t** data, int* total_data_size, int32_t* num_bytes) {
  Status status;
  if (!ReadWriteUtil::Read(data, total_data_size, num_bytes, &status)) {
    return status;
  }
  if (*num_bytes < 0 || *num_bytes > *total_data_size) {
    return Status(TErrorCode::PARQUET_CORRUPT_RLE_BYTES, filename, *num_bytes);
  }
  return Status::OK();
}

Status ParquetLevelDecoder::Init(const string& filename, MemPool* cache_pool,
    int cache_size, int max_level, uint8_t* data, int32_t num_bytes) {
  DCHECK(data != nullptr);
  DCHECK_GE(num_bytes, 0);
  DCHECK_GT(cache_size, 0);
  cache_size = BitUtil::RoundUpToPowerOf2(cache_size, 32);
  max_level_ = max_level;
  filename_ = filename;
  RETURN_IF_ERROR(InitCache(cache_pool, cache_size));

  // Return because there is no level data to read, e.g., required field.
  if (max_level == 0) return Status::OK();

  int bit_width = BitUtil::Log2Ceiling64(max_level + 1);
  rle_decoder_.Reset(data, num_bytes, bit_width);

  return Status::OK();
}

Status ParquetLevelDecoder::InitCache(MemPool* pool, int cache_size) {
  num_cached_levels_ = 0;
  cached_level_idx_ = 0;
  // Memory has already been allocated.
  if (cached_levels_ != nullptr) {
    DCHECK_EQ(cache_size_, cache_size);
    return Status::OK();
  }

  cached_levels_ = reinterpret_cast<uint8_t*>(pool->TryAllocate(cache_size));
  if (cached_levels_ == nullptr) {
    return pool->mem_tracker()->MemLimitExceeded(
        nullptr, "Definition level cache", cache_size);
  }
  memset(cached_levels_, 0, cache_size);
  cache_size_ = cache_size;
  return Status::OK();
}

Status ParquetLevelDecoder::CacheNextBatch(int vals_remaining) {
  /// Fill the cache completely if there are enough values remaining.
  /// Otherwise don't try to read more values than are left.
  int batch_size = min(vals_remaining, cache_size_);
  if (max_level_ > 0) {
    if (UNLIKELY(!FillCache(batch_size, &num_cached_levels_)
            || num_cached_levels_ < batch_size)) {
      return Status(decoding_error_code_, vals_remaining, filename_);
    }
  } else {
    // No levels to read, e.g., because the field is required. The cache was
    // already initialized with all zeros, so we can hand out those values.
    DCHECK_EQ(max_level_, 0);
    cached_level_idx_ = 0;
    num_cached_levels_ = batch_size;
  }
  return Status::OK();
}

bool ParquetLevelDecoder::FillCache(int batch_size, int* num_cached_levels) {
  DCHECK(!CacheHasNext());
  DCHECK(num_cached_levels != nullptr);
  DCHECK_GE(max_level_, 0);
  DCHECK_GE(*num_cached_levels, 0);
  cached_level_idx_ = 0;
  if (max_level_ == 0) {
    // No levels to read, e.g., because the field is required. The cache was
    // already initialized with all zeros, so we can hand out those values.
    *num_cached_levels = batch_size;
    return true;
  }
  *num_cached_levels = rle_decoder_.GetValues(batch_size, cached_levels_);
  return *num_cached_levels > 0;
}

} // namespace impala
