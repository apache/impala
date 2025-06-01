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

#include "util/char-codec.h"

#include <boost/locale.hpp>
#include <string>

#include "exec/scanner-context.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"

using namespace impala;
using namespace strings;

CharCodec::CharCodec(MemPool* memory_pool, const std::string& encoding, char tuple_delim,
    bool reuse_buffer)
    : memory_pool_(memory_pool), encoding_(encoding), tuple_delim_(tuple_delim),
      reuse_buffer_(reuse_buffer) {
}

const int CharCodec::MAX_SYMBOL = 4;

Status CharCodec::DecodeBuffer(uint8_t** buffer, int64_t* bytes_read, MemPool* pool,
    bool eosr, bool decompress, ScannerContext* context) {
  std::string result_prefix;
  std::string result_core;
  std::string result_suffix;

  // We're about to create a new decoding buffer (if we can't reuse). Attach the
  // memory from previous decoding rounds to 'pool'. In case of streaming decompression
  // this is already done in DecompressStreamToBuffer().
  if (!decompress && !reuse_buffer_) {
    if (pool != nullptr) {
      pool->AcquireData(memory_pool_, false);
    } else {
      memory_pool_->FreeAll();
    }
    out_buffer_ = nullptr;
  }

  uint8_t* buf_start = *buffer;
  uint8_t* buf_end = buf_start + *bytes_read;

  // Allocate memory twice the size of the input buffer to handle the worst case
  ScopedMemTracker scoped_mem_tracker(memory_pool_->mem_tracker());
  RETURN_IF_ERROR(scoped_mem_tracker.TryConsume((*bytes_read) * 2));

  RETURN_IF_ERROR(HandlePrefix(&buf_start, buf_end, &result_prefix));
  RETURN_IF_ERROR(HandleCore(&buf_start, buf_end, &result_core));
  RETURN_IF_ERROR(HandleSuffix(&buf_start, buf_end, &result_suffix));

  if (eosr && !partial_symbol_.empty()) {
    return Status(TErrorCode::CHARSET_CONVERSION_ERROR,
        "End of stream reached with partial symbol.");
  }

  // In case of decompression, decompressed data can be freed up after decoding
  if (decompress) {
    memory_pool_->FreeAll();
  } else if (eosr) {
    context->ReleaseCompletedResources(false);
  }

  // Concat the results onto the output buffer
  *bytes_read = result_prefix.size() + result_core.size() + result_suffix.size();
  if (out_buffer_ == nullptr || buffer_length_ < *bytes_read) {
    buffer_length_ =  *bytes_read;
    out_buffer_ = memory_pool_->TryAllocate(buffer_length_);
    if (UNLIKELY(out_buffer_ == nullptr)) {
      string details = Substitute(
          "HdfsTextScanner::DecodeBuffer() failed to allocate $1 bytes.", *bytes_read);
      return memory_pool_->mem_tracker()->MemLimitExceeded(nullptr, details, *bytes_read);
    }
  }
  *buffer = out_buffer_;
  memcpy(*buffer, result_prefix.data(), result_prefix.size());
  memcpy(*buffer + result_prefix.size(), result_core.data(), result_core.size());
  memcpy(*buffer + result_prefix.size() + result_core.size(),
      result_suffix.data(), result_suffix.size());

  return Status::OK();
}

Status CharCodec::HandlePrefix(uint8_t** buf_start, uint8_t* buf_end,
    std::string* result_prefix) {
  if (!partial_symbol_.empty()) {
    std::vector<uint8_t> prefix;
    prefix.reserve(MAX_SYMBOL);
    prefix.assign(partial_symbol_.begin(), partial_symbol_.end());
    bool success = false;
    DCHECK_LT(partial_symbol_.size(), MAX_SYMBOL);
    for (int i = 0; partial_symbol_.size() + i < MAX_SYMBOL && *buf_start + i < buf_end;
          ++i) {
      prefix.push_back((*buf_start)[i]);
      try {
        *result_prefix =
            boost::locale::conv::to_utf<char>(reinterpret_cast<char*>(prefix.data()),
                reinterpret_cast<char*>(prefix.data()) + prefix.size(), encoding_,
                boost::locale::conv::stop);
        success = true;
        *buf_start += i + 1;
        break;
      } catch (boost::locale::conv::conversion_error&) {
        continue;
      } catch (const std::exception& e) {
        return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
      }
    }
    if (!success) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, "Unable to decode buffer");
    }
    partial_symbol_.clear();
  }
  return Status::OK();
}

Status CharCodec::HandleCore(uint8_t** buf_start, uint8_t* buf_end,
    std::string* result_core) {
  uint8_t* last_delim =
      std::find_end(*buf_start, buf_end, &tuple_delim_, &tuple_delim_ + 1);
  if (last_delim != buf_end) {
    try {
      *result_core = boost::locale::conv::to_utf<char>(
          reinterpret_cast<char*>(*buf_start), reinterpret_cast<char*>(last_delim) + 1,
          encoding_, boost::locale::conv::stop);
    } catch (boost::locale::conv::conversion_error& e) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
    } catch (const std::exception& e) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
    }

    *buf_start = last_delim + 1;
  }
  return Status::OK();
}

Status CharCodec::HandleSuffix(uint8_t** buf_start, uint8_t* buf_end,
    std::string* result_suffix) {
  if (*buf_start < buf_end) {
    bool success = false;
    uint8_t* end = buf_end;
    while (buf_end - end < MAX_SYMBOL && end > *buf_start) {
      try {
        *result_suffix =
            boost::locale::conv::to_utf<char>(reinterpret_cast<char*>(*buf_start),
                reinterpret_cast<char*>(end), encoding_, boost::locale::conv::stop);
        success = true;
        break;
      } catch (boost::locale::conv::conversion_error&) {
        --end;
      } catch (const std::exception& e) {
        return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
      }
    }

    if (!success && end > *buf_start) {
      return Status(TErrorCode::CHARSET_CONVERSION_ERROR, "Unable to decode buffer");
    }
    if (end < buf_end) {
      partial_symbol_.assign(end, buf_end);
    }
  }

  return Status::OK();
}

Status CharCodec::EncodeBuffer(const std::string& str, std::string* result) {
  try {
    *result = boost::locale::conv::from_utf<char>(
        str, encoding_, boost::locale::conv::stop);
  } catch (boost::locale::conv::conversion_error& e) {
    return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
  } catch (const std::exception& e) {
    return Status(TErrorCode::CHARSET_CONVERSION_ERROR, e.what());
  }

  return Status::OK();
}
