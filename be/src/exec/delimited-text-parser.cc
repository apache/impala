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

#include "exec/delimited-text-parser.inline.h"

#include "exec/hdfs-scanner.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;

template<bool DELIMITED_TUPLES>
DelimitedTextParser<DELIMITED_TUPLES>::DelimitedTextParser(
    int num_cols, int num_partition_keys, const bool* is_materialized_col,
    char tuple_delim, char field_delim, char collection_item_delim, char escape_char)
    : is_materialized_col_(is_materialized_col),
      num_tuple_delims_(0),
      num_delims_(0),
      num_cols_(num_cols),
      num_partition_keys_(num_partition_keys),
      column_idx_(0),
      last_row_delim_offset_(-1),
      field_delim_(field_delim),
      process_escapes_(escape_char != '\0'),
      escape_char_(escape_char),
      collection_item_delim_(collection_item_delim),
      tuple_delim_(tuple_delim),
      current_column_has_escape_(false),
      last_char_is_escape_(false),
      unfinished_tuple_(false){
  // Escape character should not be the same as tuple or col delim unless it is the
  // empty delimiter.
  DCHECK(escape_char == '\0' || escape_char != tuple_delim);
  DCHECK(escape_char == '\0' || escape_char != field_delim);
  DCHECK(escape_char == '\0' || escape_char != collection_item_delim);

  // Initialize the sse search registers.
#ifdef __x86_64__
  char search_chars[SSEUtil::CHARS_PER_128_BIT_REGISTER];
  memset(search_chars, 0, sizeof(search_chars));
  if (process_escapes_) {
    search_chars[0] = escape_char_;
    xmm_escape_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));

    // To process escape characters, we need to check if there was an escape
    // character between (col_start,col_end).  The SSE instructions return
    // a bit mask for 16 bits so we need to mask off the bits below col_start
    // and after col_end.
    low_mask_[0] = 0xffff;
    high_mask_[15] = 0xffff;
    for (int i = 1; i < 16; ++i) {
      low_mask_[i] = low_mask_[i - 1] << 1;
    }
    for (int i = 14; i >= 0; --i) {
      high_mask_[i] = high_mask_[i + 1] >> 1;
    }
  } else {
    memset(high_mask_, 0, sizeof(high_mask_));
    memset(low_mask_, 0, sizeof(low_mask_));
  }

  if (DELIMITED_TUPLES) {
    search_chars[num_delims_++] = tuple_delim_;
    ++num_tuple_delims_;
    // Hive will treats \r (^M) as an alternate tuple delimiter, but \r\n is a
    // single tuple delimiter.
    if (tuple_delim_ == '\n') {
      search_chars[num_delims_++] = '\r';
      ++num_tuple_delims_;
    }
    xmm_tuple_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));
    if (field_delim_ != tuple_delim_) search_chars[num_delims_++] = field_delim_;
  } else {
    search_chars[num_delims_++] = field_delim_;
  }

  if (collection_item_delim != '\0') search_chars[num_delims_++] = collection_item_delim_;

  DCHECK_GT(num_delims_, 0);
  xmm_delim_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));
#endif

  ParserReset();
}

template
DelimitedTextParser<true>::DelimitedTextParser(
    int num_cols, int num_partition_keys, const bool* is_materialized_col,
    char tuple_delim, char field_delim, char collection_item_delim, char escape_char);

template
DelimitedTextParser<false>::DelimitedTextParser(
    int num_cols, int num_partition_keys, const bool* is_materialized_col,
    char tuple_delim, char field_delim, char collection_item_delim, char escape_char);

template<bool DELIMITED_TUPLES>
void DelimitedTextParser<DELIMITED_TUPLES>::ParserReset() {
  current_column_has_escape_ = false;
  last_char_is_escape_ = false;
  last_row_delim_offset_ = -1;
  column_idx_ = num_partition_keys_;
}

template void DelimitedTextParser<true>::ParserReset();

// Parsing raw csv data into FieldLocation descriptors.
template<bool DELIMITED_TUPLES>
Status DelimitedTextParser<DELIMITED_TUPLES>::ParseFieldLocations(int max_tuples,
    int64_t remaining_len, char** byte_buffer_ptr, char** row_end_locations,
    FieldLocation* field_locations,
    int* num_tuples, int* num_fields, char** next_column_start) {
  // Start of this batch.
  *next_column_start = *byte_buffer_ptr;
  // If there was a '\r' at the end of the last batch, set the offset to
  // just before the beginning. Otherwise make it invalid.
  if (last_row_delim_offset_ == 0) {
    last_row_delim_offset_ = remaining_len;
  } else {
    last_row_delim_offset_ = -1;
  }

#ifdef __x86_64__
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    if (process_escapes_) {
      RETURN_IF_ERROR(ParseSse<true>(max_tuples, &remaining_len, byte_buffer_ptr,
          row_end_locations, field_locations, num_tuples, num_fields, next_column_start));
    } else {
      RETURN_IF_ERROR(ParseSse<false>(max_tuples, &remaining_len, byte_buffer_ptr,
          row_end_locations, field_locations, num_tuples, num_fields, next_column_start));
    }
  }
#endif

  if (*num_tuples == max_tuples) return Status::OK();

  // Handle the remaining characters
  while (remaining_len > 0) {
    bool new_tuple = false;
    bool new_col = false;
    if (DELIMITED_TUPLES) unfinished_tuple_ = true;

    if (!last_char_is_escape_) {
      if (DELIMITED_TUPLES && (**byte_buffer_ptr == tuple_delim_ ||
           (tuple_delim_ == '\n' && **byte_buffer_ptr == '\r'))) {
        new_tuple = true;
        new_col = true;
      } else if (**byte_buffer_ptr == field_delim_
                 || **byte_buffer_ptr == collection_item_delim_) {
        new_col = true;
      }
    }

    if (process_escapes_ && **byte_buffer_ptr == escape_char_) {
      current_column_has_escape_ = true;
      last_char_is_escape_ = !last_char_is_escape_;
    } else {
      last_char_is_escape_ = false;
    }

    if (new_tuple) {
      if (last_row_delim_offset_ == remaining_len && **byte_buffer_ptr == '\n') {
        // If the row ended in \r\n then move to the \n
        ++*next_column_start;
      } else {
        RETURN_IF_ERROR(AddColumn<true>(*byte_buffer_ptr - *next_column_start,
            next_column_start, num_fields, field_locations));
        Status status = FillColumns<false>(0, NULL, num_fields, field_locations);
        DCHECK(status.ok());
        column_idx_ = num_partition_keys_;
        row_end_locations[*num_tuples] = *byte_buffer_ptr;
        ++(*num_tuples);
      }
      DCHECK(DELIMITED_TUPLES);
      unfinished_tuple_ = false;
      last_row_delim_offset_ = **byte_buffer_ptr == '\r' ? remaining_len - 1 : -1;
      if (*num_tuples == max_tuples) {
        ++*byte_buffer_ptr;
        --remaining_len;
        if (last_row_delim_offset_ == remaining_len) last_row_delim_offset_ = 0;
        return Status::OK();
      }
    } else if (new_col) {
      RETURN_IF_ERROR(AddColumn<true>(*byte_buffer_ptr - *next_column_start,
          next_column_start, num_fields, field_locations));
    }

    --remaining_len;
    ++*byte_buffer_ptr;
  }

  // For formats that store the length of the row, the row is not delimited:
  // e.g. Sequence files.
  if (!DELIMITED_TUPLES) {
    DCHECK_EQ(remaining_len, 0);
    RETURN_IF_ERROR(AddColumn<true>(*byte_buffer_ptr - *next_column_start,
        next_column_start, num_fields, field_locations));
    Status status = FillColumns<false>(0, NULL, num_fields, field_locations);
    DCHECK(status.ok());
    column_idx_ = num_partition_keys_;
    ++(*num_tuples);
  }
  return Status::OK();
}

template
Status DelimitedTextParser<true>::ParseFieldLocations(int max_tuples,
    int64_t remaining_len, char** byte_buffer_ptr, char** row_end_locations,
    FieldLocation* field_locations,
    int* num_tuples, int* num_fields, char** next_column_start);

template
Status DelimitedTextParser<false>::ParseFieldLocations(int max_tuples,
    int64_t remaining_len, char** byte_buffer_ptr, char** row_end_locations,
    FieldLocation* field_locations,
    int* num_tuples, int* num_fields, char** next_column_start);

template<bool DELIMITED_TUPLES>
int64_t DelimitedTextParser<DELIMITED_TUPLES>::FindFirstInstance(const char* buffer,
    int64_t len) {
  int64_t tuple_start = 0;
  const char* buffer_start = buffer;
  bool found = false;

  DCHECK(DELIMITED_TUPLES);
  // If the last char in the previous buffer was \r then either return the start of
  // this buffer or skip a \n at the beginning of the buffer.
  if (last_row_delim_offset_ != -1) {
    if (*buffer_start == '\n') return 1;
    return 0;
  }
restart:
  found = false;

#ifdef __x86_64__
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    __m128i xmm_buffer, xmm_tuple_mask;
    while (len - tuple_start >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      // TODO: can we parallelize this as well?  Are there multiple sse execution units?
      // Load the next 16 bytes into the xmm register and do strchr for the
      // tuple delimiter.
      xmm_buffer = _mm_loadu_si128(reinterpret_cast<const __m128i*>(buffer));
      xmm_tuple_mask = SSE4_cmpestrm<SSEUtil::STRCHR_MODE>(xmm_tuple_search_,
          num_tuple_delims_, xmm_buffer, SSEUtil::CHARS_PER_128_BIT_REGISTER);
      int tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);
      if (tuple_mask != 0) {
        found = true;
        // Find first set bit (1-based)
        int i = ffs(tuple_mask);
        tuple_start += i;
        buffer += i;
        break;
      }
      tuple_start += SSEUtil::CHARS_PER_128_BIT_REGISTER;
      buffer += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
  }
#endif
  if (!found) {
    for (; tuple_start < len; ++tuple_start) {
      char c = *buffer++;
      if (c == tuple_delim_ || (c == '\r' && tuple_delim_ == '\n')) {
        ++tuple_start;
        found = true;
        break;
      }
    }
  }

  if (!found) return -1;

  if (process_escapes_) {
    // Scan backwards for escape characters.  We do this after
    // finding the tuple break rather than during the (above)
    // forward scan to make the forward scan faster.  This will
    // perform worse if there are many characters right before the
    // tuple break that are all escape characters, but that is
    // unlikely.
    int num_escape_chars = 0;
    int64_t before_tuple_end = tuple_start - 2;
    // TODO: If scan range is split between escape character and tuple delimiter,
    // before_tuple_end will be -1. Need to scan previous range for escape characters
    // in this case.
    for (; before_tuple_end >= 0; --before_tuple_end) {
      if (buffer_start[before_tuple_end] == escape_char_) {
        ++num_escape_chars;
      } else {
        break;
      }
    }

    // TODO: This sucks.  All the preceding characters before the tuple delim were
    // escape characters.  We need to read from the previous block to see what to do.
    if (before_tuple_end < 0) {
      static bool warning_logged = false;
      if (!warning_logged) {
        LOG(WARNING) << "Unhandled code path.  This might cause a tuple to be "
                     << "skipped or repeated.";
        warning_logged = true;
      }
    }

    // An even number of escape characters means they cancel out and this tuple break
    // is *not* escaped.
    if (num_escape_chars % 2 != 0) goto restart;
  }

  if (tuple_start < len && buffer_start[tuple_start] == '\n' &&
      buffer_start[tuple_start - 1] == '\r') {
    // We have \r\n, move to the next character.
    ++tuple_start;
  }
  return tuple_start;
}

template
int64_t DelimitedTextParser<true>::FindFirstInstance(const char* buffer, int64_t len);
