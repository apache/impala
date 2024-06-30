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


#ifndef IMPALA_EXEC_DELIMITED_TEXT_PARSER_INLINE_H
#define IMPALA_EXEC_DELIMITED_TEXT_PARSER_INLINE_H

#include "delimited-text-parser.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

/// Updates the values in the field and tuple masks, escaping them if necessary.
/// If the character at n is an escape character, then delimiters(tuple/field/escape
/// characters) at n+1 don't count.
inline void ProcessEscapeMask(uint16_t escape_mask, bool* last_char_is_escape,
    uint16_t* delim_mask) {
  // Escape characters can escape escape characters.
  bool first_char_is_escape = *last_char_is_escape;
  bool escape_next = first_char_is_escape;
  for (int i = 0; i < SSEUtil::CHARS_PER_128_BIT_REGISTER; ++i) {
    if (escape_next) {
      escape_mask &= ~SSEUtil::SSE_BITMASK[i];
    }
    escape_next = escape_mask & SSEUtil::SSE_BITMASK[i];
  }

  // Remember last character for the next iteration
  *last_char_is_escape = escape_mask &
      SSEUtil::SSE_BITMASK[SSEUtil::CHARS_PER_128_BIT_REGISTER - 1];

  // Shift escape mask up one so they match at the same bit index as the tuple and
  // field mask (instead of being the character before) and set the correct first bit
  escape_mask = escape_mask << 1 | (first_char_is_escape ? 1 : 0);

  // If escape_mask[n] is true, then tuple/field_mask[n] is escaped
  *delim_mask &= ~escape_mask;
}

template <bool DELIMITED_TUPLES>
template <bool PROCESS_ESCAPES>
inline Status DelimitedTextParser<DELIMITED_TUPLES>::AddColumn(int64_t len,
    char** next_column_start, int* num_fields, FieldLocation* field_locations) {
  if (UNLIKELY(!BitUtil::IsNonNegative32Bit(len))) {
    return Status(TErrorCode::TEXT_PARSER_TRUNCATED_COLUMN, len);
  }
  if (ReturnCurrentColumn()) {
    // Found a column that needs to be parsed, write the start/len to 'field_locations'
    field_locations[*num_fields].start = *next_column_start;
    int64_t field_len = len;
    if (PROCESS_ESCAPES && current_column_has_escape_) {
      field_len = -len;
    }
    field_locations[*num_fields].len = static_cast<int32_t>(field_len);
    ++(*num_fields);
  }
  if (PROCESS_ESCAPES) current_column_has_escape_ = false;
  *next_column_start += len + 1;
  // No need to keep bumping 'column_idx_' if it's already 'num_cols_'. Otherwise,
  // a large file with full of field delimiters might lead to 'column_idx_' overflow.
  if (column_idx_ < num_cols_) ++column_idx_;
  return Status::OK();
}

template <bool DELIMITED_TUPLES>
template <bool PROCESS_ESCAPES>
inline Status DelimitedTextParser<DELIMITED_TUPLES>::FillColumns(int64_t len,
    char** last_column, int* num_fields, FieldLocation* field_locations) {
  // Fill in any columns missing from the end of the tuple.
  char* dummy = NULL;
  if (last_column == NULL) last_column = &dummy;
  while (column_idx_ < num_cols_) {
    RETURN_IF_ERROR(AddColumn<PROCESS_ESCAPES>(len, last_column,
        num_fields, field_locations));
    // The rest of the columns will be null.
    last_column = &dummy;
    len = 0;
  }
  return Status::OK();
}

#ifdef __x86_64__
/// SSE optimized raw text file parsing.  SSE4_2 added an instruction (with 3 modes) for
/// text processing.  The modes mimic strchr, strstr and strcmp.  For text parsing, we can
/// leverage the strchr functionality.
//
/// The instruction operates on two sse registers:
///  - the needle (what you are searching for)
///  - the haystack (where you are searching in)
/// Both registers can contain up to 16 characters.  The result is a 16-bit mask with a bit
/// set for each character in the haystack that matched any character in the needle.
/// For example:
///  Needle   = 'abcd000000000000' (we're searching for any a's, b's, c's or d's)
///  Haystack = 'asdfghjklhjbdwwc' (the raw string)
///  Result   = '1010000000011001'
template <bool DELIMITED_TUPLES>
template <bool PROCESS_ESCAPES>
inline Status DelimitedTextParser<DELIMITED_TUPLES>::ParseSse(int max_tuples,
    int64_t* remaining_len, char** byte_buffer_ptr,
    char** row_end_locations, FieldLocation* field_locations,
    int* num_tuples, int* num_fields, char** next_column_start) {
  DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2));

  // To parse using SSE, we:
  //  1. Load into different sse registers the different characters we need to search for
  //        tuple breaks, field breaks, escape characters
  //  2. Load 16 characters at a time into the sse register
  //  3. Use the SSE instruction to do strchr on those 16 chars, the result is a bitmask
  //  4. Compute the bitmask for tuple breaks, field breaks and escape characters.
  //  5. If there are escape characters, fix up the matching masked bits in the
  //        field/tuple mask
  //  6. Go through the mask bit by bit and write the parsed data.

  // xmm registers:
  //  - xmm_buffer: the register holding the current (16 chars) we're working on from the
  //        file
  //  - xmm_delim_search_: the delim search register.  Contains field delimiter,
  //        collection_item delim_char and tuple delimiter
  //  - xmm_escape_search_: the escape search register. Only contains escape char
  //  - xmm_delim_mask: the result of doing strchr for the delimiters
  //  - xmm_escape_mask: the result of doing strchr for the escape char
  __m128i xmm_buffer, xmm_delim_mask, xmm_escape_mask;

  while (LIKELY(*remaining_len >= SSEUtil::CHARS_PER_128_BIT_REGISTER)) {
    // Load the next 16 bytes into the xmm register
    xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(*byte_buffer_ptr));

    // Do the strchr for tuple and field breaks
    // The strchr sse instruction returns the result in the lower bits of the sse
    // register.  Since we only process 16 characters at a time, only the lower 16 bits
    // can contain non-zero values.
    // _mm_extract_epi16 will extract 16 bits out of the xmm register.  The second
    // parameter specifies which 16 bits to extract (0 for the lowest 16 bits).
    xmm_delim_mask = SSE4_cmpestrm<SSEUtil::STRCHR_MODE>(xmm_delim_search_,
        num_delims_, xmm_buffer, SSEUtil::CHARS_PER_128_BIT_REGISTER);
    uint16_t delim_mask = _mm_extract_epi16(xmm_delim_mask, 0);

    uint16_t escape_mask = 0;
    // If the table does not use escape characters, skip processing for it.
    if (PROCESS_ESCAPES) {
      DCHECK(escape_char_ != '\0');
      xmm_escape_mask = SSE4_cmpestrm<SSEUtil::STRCHR_MODE>(xmm_escape_search_, 1,
          xmm_buffer, SSEUtil::CHARS_PER_128_BIT_REGISTER);
      escape_mask = _mm_extract_epi16(xmm_escape_mask, 0);
      ProcessEscapeMask(escape_mask, &last_char_is_escape_, &delim_mask);
    }

    char* last_char = *byte_buffer_ptr + 15;
    bool last_char_is_unescaped_delim = delim_mask >> 15;
    if (DELIMITED_TUPLES) {
      unfinished_tuple_ = !(last_char_is_unescaped_delim &&
          (*last_char == tuple_delim_ || (tuple_delim_ == '\n' && *last_char == '\r')));
    }

    int last_col_idx = 0;
    // Process all non-zero bits in the delim_mask from lsb->msb.  If a bit
    // is set, the character in that spot is either a field or tuple delimiter.
    while (delim_mask != 0) {
      // ffs is a libc function that returns the index of the first set bit (1-indexed)
      int n = ffs(delim_mask) - 1;
      DCHECK_GE(n, 0);
      DCHECK_LT(n, 16);
      // clear current bit
      delim_mask &= ~(SSEUtil::SSE_BITMASK[n]);

      if (PROCESS_ESCAPES) {
        // Determine if there was an escape character between [last_col_idx, n]
        bool escaped = (escape_mask & low_mask_[last_col_idx] & high_mask_[n]) != 0;
        current_column_has_escape_ |= escaped;
        last_col_idx = n;
      }

      char* delim_ptr = *byte_buffer_ptr + n;

      if (IsFieldOrCollectionItemDelimiter(*delim_ptr)) {
        RETURN_IF_ERROR(AddColumn<PROCESS_ESCAPES>(delim_ptr - *next_column_start,
            next_column_start, num_fields, field_locations));
        continue;
      }

      if (DELIMITED_TUPLES &&
          (*delim_ptr == tuple_delim_ || (tuple_delim_ == '\n' && *delim_ptr == '\r'))) {
        if (UNLIKELY(
                last_row_delim_offset_ == *remaining_len - n && *delim_ptr == '\n')) {
          // If the row ended in \r\n then move the next start past the \n
          ++*next_column_start;
          last_row_delim_offset_ = -1;
          continue;
        }
        RETURN_IF_ERROR(AddColumn<PROCESS_ESCAPES>(delim_ptr - *next_column_start,
            next_column_start, num_fields, field_locations));
        Status status = FillColumns<false>(0, NULL, num_fields, field_locations);
        DCHECK(status.ok());
        column_idx_ = num_partition_keys_;
        row_end_locations[*num_tuples] = delim_ptr;
        ++(*num_tuples);
        // Remember where we saw the last \r.
        last_row_delim_offset_ = *delim_ptr == '\r' ? *remaining_len - n - 1 : -1;
        if (UNLIKELY(*num_tuples == max_tuples)) {
          (*byte_buffer_ptr) += (n + 1);
          if (PROCESS_ESCAPES) last_char_is_escape_ = false;
          *remaining_len -= (n + 1);
          // If the last character we processed was \r then set the offset to 0
          // so that we will use it at the beginning of the next batch.
          if (last_row_delim_offset_ == *remaining_len) last_row_delim_offset_ = 0;
          return Status::OK();
        }
      }
    }

    if (PROCESS_ESCAPES) {
      // Determine if there was an escape character between (last_col_idx, 15)
      bool unprocessed_escape = escape_mask & low_mask_[last_col_idx] & high_mask_[15];
      current_column_has_escape_ |= unprocessed_escape;
    }

    *remaining_len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
    *byte_buffer_ptr += SSEUtil::CHARS_PER_128_BIT_REGISTER;
  }
  return Status::OK();
}
#endif

/// Simplified version of ParseSSE which does not handle tuple delimiters.
template<>
template <bool PROCESS_ESCAPES>
inline Status DelimitedTextParser<false>::ParseSingleTuple(int64_t remaining_len,
    char* buffer, FieldLocation* field_locations, int* num_fields) {
  char* next_column_start = buffer;

  column_idx_ = num_partition_keys_;
  current_column_has_escape_ = false;

#ifdef __x86_64__
  __m128i xmm_buffer, xmm_delim_mask, xmm_escape_mask;

  if (LIKELY(CpuInfo::IsSupported(CpuInfo::SSE4_2))) {
    while (LIKELY(remaining_len >= SSEUtil::CHARS_PER_128_BIT_REGISTER)) {
      // Load the next 16 bytes into the xmm register
      xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(buffer));

      xmm_delim_mask = SSE4_cmpestrm<SSEUtil::STRCHR_MODE>(xmm_delim_search_,
          num_delims_, xmm_buffer, SSEUtil::CHARS_PER_128_BIT_REGISTER);
      uint16_t delim_mask = _mm_extract_epi16(xmm_delim_mask, 0);

      uint16_t escape_mask = 0;
      // If the table does not use escape characters, skip processing for it.
      if (PROCESS_ESCAPES) {
        DCHECK(escape_char_ != '\0');
        xmm_escape_mask = SSE4_cmpestrm<SSEUtil::STRCHR_MODE>(xmm_escape_search_, 1,
            xmm_buffer, SSEUtil::CHARS_PER_128_BIT_REGISTER);
        escape_mask = _mm_extract_epi16(xmm_escape_mask, 0);
        ProcessEscapeMask(escape_mask, &last_char_is_escape_, &delim_mask);
      }

      int last_col_idx = 0;
      // Process all non-zero bits in the delim_mask from lsb->msb.  If a bit
      // is set, the character in that spot is a field.
      while (delim_mask != 0) {
        // ffs is a libc function that returns the index of the first set bit (1-indexed)
        int n = ffs(delim_mask) - 1;
        DCHECK_GE(n, 0);
        DCHECK_LT(n, 16);

        if (PROCESS_ESCAPES) {
          // Determine if there was an escape character between [last_col_idx, n]
          bool escaped = (escape_mask & low_mask_[last_col_idx] & high_mask_[n]) != 0;
          current_column_has_escape_ |= escaped;
          last_col_idx = n;
        }

        // clear current bit
        delim_mask &= ~(SSEUtil::SSE_BITMASK[n]);

        RETURN_IF_ERROR(AddColumn<PROCESS_ESCAPES>(buffer + n - next_column_start,
            &next_column_start, num_fields, field_locations));
      }

      if (PROCESS_ESCAPES) {
        // Determine if there was an escape character between (last_col_idx, 15)
        bool unprocessed_escape = escape_mask & low_mask_[last_col_idx] & high_mask_[15];
        current_column_has_escape_ |= unprocessed_escape;
      }

      remaining_len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      buffer += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
  }
#endif

  while (remaining_len > 0) {
    if (*buffer == escape_char_) {
      current_column_has_escape_ = true;
      last_char_is_escape_ = !last_char_is_escape_;
    } else {
      last_char_is_escape_ = false;
    }

    if (!last_char_is_escape_ && IsFieldOrCollectionItemDelimiter(*buffer)) {
      RETURN_IF_ERROR(AddColumn<PROCESS_ESCAPES>(buffer - next_column_start,
          &next_column_start, num_fields, field_locations));
    }

    --remaining_len;
    ++buffer;
  }

  // Last column does not have a delimiter after it.  Add that column and also
  // pad with empty cols if the input is ragged.
  return FillColumns<PROCESS_ESCAPES>(buffer - next_column_start,
      &next_column_start, num_fields, field_locations);
}

}

#endif
