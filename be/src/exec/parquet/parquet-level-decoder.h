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

#pragma once

#include <string>

#include "common/status.h"
#include "gen-cpp/parquet_types.h"
#include "util/rle-encoding.h"

namespace impala {

class MemPool;

/// Constants used instead of actual levels to indicate special conditions.
class ParquetLevel {
 public:
  /// The rep and def levels are set to this value to indicate the end of a row group.
  static const int16_t ROW_GROUP_END = numeric_limits<int16_t>::min();
  /// Indicates an invalid definition or repetition level.
  static const int16_t INVALID_LEVEL = -1;
  /// Indicates an invalid position value.
  static const int16_t INVALID_POS = -1;
};

/// Decoder for encoded Parquet levels. Only supports the RLE encoding, not the deprecated
/// BIT_PACKED encoding. Optionally reads, decodes, and caches level values in batches.
/// Level values are unsigned 8-bit integers because we support a maximum nesting
/// depth of 100, as enforced by the FE. Using a small type saves memory and speeds up
/// populating the level cache (e.g., with RLE we can memset() repeated values).
class ParquetLevelDecoder {
 public:
  ParquetLevelDecoder(bool is_def_level_decoder)
    : decoding_error_code_(is_def_level_decoder ? TErrorCode::PARQUET_DEF_LEVEL_ERROR :
                                                  TErrorCode::PARQUET_REP_LEVEL_ERROR) {}

  /// Initialize the LevelDecoder. Assumes that data is RLE encoded.
  /// 'cache_size' will be rounded up to a multiple of 32 internally.
  Status Init(const std::string& filename, MemPool* cache_pool, int cache_size,
      int max_level, uint8_t* data, int32_t num_bytes);

  /// Parses the number of bytes used for level encoding from the buffer and moves
  /// 'data' forward.
  static Status ParseRleByteSize(const string& filename,
      uint8_t** data, int* total_data_size, int32_t* num_bytes);

  // Validates that encoding is RLE.
  static Status ValidateEncoding(const string& filename,
      const parquet::Encoding::type encoding);

  /// Returns the next level or INVALID_LEVEL if there was an error. Not as efficient
  /// as batched methods.
  inline int16_t ReadLevel();

  /// Returns the next level or INVALID_LEVEL if there was an error. It doesn't move
  /// to the next level.
  inline int16_t PeekLevel();

  /// If the next value is part of a repeated run and is not cached, return the length
  /// of the repeated run. A max level of 0 is treated as an arbitrarily long run of
  /// zeroes, so this returns numeric_limits<int32_t>::max(). Otherwise return 0.
  inline int32_t NextRepeatedRunLength();

  /// Get the value of the repeated run (if NextRepeatedRunLength() > 0) and consume
  /// 'num_to_consume' items in the run. Not valid to call if there are cached levels
  /// that have not been consumed.
  inline uint8_t GetRepeatedValue(uint32_t num_to_consume);

  /// Decodes and caches the next batch of levels given that there are 'vals_remaining'
  /// values left to decode in the page. Resets members associated with the cache.
  /// Returns a non-ok status if there was a problem decoding a level, if a level was
  /// encountered with a value greater than max_level_, or if fewer than
  /// min(CacheSize(), vals_remaining) levels could be read, which indicates that the
  /// input did not have the expected number of values. Only valid to call when
  /// the cache has been exhausted, i.e. CacheHasNext() is false.
  Status CacheNextBatch(int vals_remaining);

  /// No-op if there are remaining values in the cache. Invokes 'CacheNextBatch()' if the
  /// cache is empty.
  Status CacheNextBatchIfEmpty(int vals_to_cache) {
    if (LIKELY(CacheHasNext())) return Status::OK();
    return CacheNextBatch(vals_to_cache);
  }

  /// Functions for working with the level cache.
  bool CacheHasNext() const { return cached_level_idx_ < num_cached_levels_; }
  uint8_t CacheGetNext() {
    DCHECK_LT(cached_level_idx_, num_cached_levels_);
    return cached_levels_[cached_level_idx_++];
  }
  // Retrieving the next cached level without consuming it.
  uint8_t CachePeekNext() {
    DCHECK_LT(cached_level_idx_, num_cached_levels_);
    return cached_levels_[cached_level_idx_];
  }
  void CacheSkipLevels(int num_levels) {
    DCHECK_LE(cached_level_idx_ + num_levels, num_cached_levels_);
    cached_level_idx_ += num_levels;
  }
  int CacheSize() const { return num_cached_levels_; }
  int CacheRemaining() const { return num_cached_levels_ - cached_level_idx_; }
  int CacheCurrIdx() const { return cached_level_idx_; }

 private:
  /// Initializes members associated with the level cache. Allocates memory for
  /// the cache from pool, if necessary.
  Status InitCache(MemPool* pool, int cache_size);

  // Invokes FillCache() when the cache is empty. Returns true if there are values
  // in the cache already, or filling the cache was successful, returns false otherwise.
  inline bool PrepareForRead();

  /// Decodes and writes a batch of levels into the cache. Returns true and sets
  /// the number of values written to the cache via *num_cached_levels if no errors
  /// are encountered. *num_cached_levels is < 'batch_size' in this case iff the
  /// end of input was hit without any other errors. Returns false if there was an
  /// error decoding a level or if there was an invalid level value greater than
  /// 'max_level_'. Only valid to call when the cache has been exhausted, i.e.
  /// CacheHasNext() is false.
  bool FillCache(int batch_size, int* num_cached_levels);

  /// RLE decoder, used if max_level_ > 0.
  RleBatchDecoder<uint8_t> rle_decoder_;

  /// Buffer for a batch of levels. The memory is allocated and owned by a pool passed
  /// in Init().
  uint8_t* cached_levels_ = nullptr;

  /// Number of valid level values in the cache.
  int num_cached_levels_ = 0;

  /// Current index into cached_levels_.
  int cached_level_idx_ = 0;

  /// For error checking and reporting.
  int max_level_ = 0;

  /// Number of level values cached_levels_ has memory allocated for. Always
  /// a multiple of 32 to allow reading directly from 'bit_reader_' in batches.
  int cache_size_ = 0;

  /// Name of the parquet file. Used for reporting level decoding errors.
  std::string filename_;

  /// Error code to use when reporting level decoding errors.
  TErrorCode::type decoding_error_code_;
};

inline bool ParquetLevelDecoder::PrepareForRead() {
  if (UNLIKELY(!CacheHasNext())) {
    if (UNLIKELY(!FillCache(cache_size_, &num_cached_levels_))) {
      return false;
    }
    DCHECK_GE(num_cached_levels_, 0);
    if (UNLIKELY(num_cached_levels_ == 0)) {
      return false;
    }
  }
  return true;
}

inline int16_t ParquetLevelDecoder::ReadLevel() {
  if (UNLIKELY(!PrepareForRead())) return ParquetLevel::INVALID_LEVEL;
  return CacheGetNext();
}

inline int16_t ParquetLevelDecoder::PeekLevel() {
  if (UNLIKELY(!PrepareForRead())) return ParquetLevel::INVALID_LEVEL;
  return CachePeekNext();
}

inline int32_t ParquetLevelDecoder::NextRepeatedRunLength() {
  if (CacheHasNext()) return 0;
  // Treat always-zero levels as an infinitely long run of zeroes. Return the maximum
  // run length allowed by the Parquet standard.
  if (max_level_ == 0) return numeric_limits<int32_t>::max();
  return rle_decoder_.NextNumRepeats();
}

inline uint8_t ParquetLevelDecoder::GetRepeatedValue(uint32_t num_to_consume) {
  DCHECK(!CacheHasNext());
  // Treat always-zero levels as an infinitely long run of zeroes.
  if (max_level_ == 0) return 0;
  return rle_decoder_.GetRepeatedValue(num_to_consume);
}

} // namespace impala
