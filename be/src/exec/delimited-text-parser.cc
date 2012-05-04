// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/cpu-info.h"
#include "exec/delimited-text-parser.h"

using namespace impala;
using namespace std;

void DelimitedTextParser::ParserReset() {
  current_column_has_escape_ = false;
  last_char_is_escape_ = false;
  column_idx_ = start_column_;
}

DelimitedTextParser::DelimitedTextParser(const vector<int>& map_column_to_slot,
                                         int start_column,
                                         char tuple_delim,
                                         char field_delim,
                                         char collection_item_delim,
                                         char escape_char)
    : map_column_to_slot_(map_column_to_slot),
      start_column_(start_column),
      field_delim_(field_delim),
      escape_char_(escape_char),
      collection_item_delim_(collection_item_delim),
      tuple_delim_(tuple_delim) {

  // Initialize the sse search registers.
  // TODO: is this safe to do in here?  Not sure if the compiler/system
  // will manage these registers for us.
  char tmp[SSEUtil::CHARS_PER_128_BIT_REGISTER];
  memset(tmp, 0, sizeof(tmp));
  if (tuple_delim_ != '\0') {
    tmp[0] = tuple_delim_;
    xmm_tuple_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(tmp));
  }
  if (escape_char_ != '\0') {
    tmp[0] = escape_char_;
    xmm_escape_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(tmp));
  }
  tmp[0] = field_delim_;
  tmp[1] = collection_item_delim_;
  xmm_field_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(tmp));

  column_idx_ = start_column_;
  current_column_has_escape_ = false;
  last_char_is_escape_ = false;
}


// Updates the values in the field and tuple masks, escaping them if necessary.
// If the character at n is an escape character, then delimiters(tuple/field/escape
// characters) at n+1 don't count.
inline void ProcessEscapeMask(int escape_mask, bool* last_char_is_escape,
                              int* field_mask, int* tuple_mask) {
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
  escape_mask = escape_mask << 1 | first_char_is_escape;

  // If escape_mask[n] is true, then tuple/field_mask[n] is escaped
  *tuple_mask &= ~escape_mask;
  *field_mask &= ~escape_mask;
}

inline void DelimitedTextParser::AddColumn(int len,
    char** next_column_start, int* num_fields,
    vector<DelimitedTextParser::FieldLocation>* field_locations) {
  if (ReturnCurrentColumn()) {
    DCHECK_LT(*num_fields, field_locations->size());
    // Found a column that needs to be parsed, write the start/len to 'parsed_data_'
    (*field_locations)[*num_fields].start = *next_column_start;
    (*field_locations)[*num_fields].len = len;
    if (current_column_has_escape_) (*field_locations)[*num_fields].len *= -1;
    ++(*num_fields);
  }
  current_column_has_escape_ = false;
  *next_column_start += len + 1;
  ++column_idx_;
}

// SSE optimized raw text file parsing.  SSE4_2 added an instruction (with 3 modes) for
// text processing.  The modes mimic strchr, strstr and strcmp.  For text parsing, we can
// leverage the strchr functionality.
//
// The instruction operates on two sse registers:
//  - the needle (what you are searching for)
//  - the haystack (where you are searching in)
// Both registers can contain up to 16 characters.  The result is a 16-bit mask with a bit
// set for each character in the haystack that matched any character in the needle.
// For example:
//  Needle   = 'abcd000000000000' (we're searching for any a's, b's, c's d's)
//  Haystack = 'asdfghjklhjbdwwc' (the raw string)
//  Result   = '101000000001101'
Status DelimitedTextParser::ParseFieldLocations(int max_tuples, int64_t remaining_len,
    char** byte_buffer_ptr, std::vector<FieldLocation>* field_locations,
    int* num_tuples, int* num_fields, char** next_column_start) {
  // Start of this batch.
  *next_column_start = *byte_buffer_ptr;

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
  //  - xmm_tuple_search_: the tuple search register.  Only contains the tuple_delim char.
  //  - xmm_field_search_: the field search register.  Contains field delim and
  //        collection_item delim_char
  //  - xmm_escape_search_: the escape search register. Only contains escape char
  //  - xmm_tuple_mask: the result of doing strchr for the tuple delim
  //  - xmm_field_mask: the result of doing strchr for the field delim
  //  - xmm_escape_mask: the result of doing strchr for the escape char
  __m128i xmm_buffer, xmm_tuple_mask, xmm_field_mask, xmm_escape_mask;

  if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
    while (remaining_len >= SSEUtil::CHARS_PER_128_BIT_REGISTER) {
      // Load the next 16 bytes into the xmm register
      xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(*byte_buffer_ptr));

      // Do the strchr for tuple and field breaks
      // TODO: can we parallelize this as well?  Are there multiple sse execution units?
      // The strchr sse instruction returns the result in the lower bits of the sse
      // register.  Since we only process 16 characters at a time, only the lower 16 bits
      // can contain non-zero values.
      // _mm_extract_epi16 will extract 16 bits out of the xmm register.  The second
      // parameter specifies which 16 bits to extract (0 for the lowest 16 bits).
      int32_t tuple_mask = 0;
      if (tuple_delim_ != '\0') {
        xmm_tuple_mask = 
          _mm_cmpistrm(xmm_tuple_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
        tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);
      }
      int32_t field_mask = 0;
      if (field_delim_ != '\0' || collection_item_delim_ != 0) {
        xmm_field_mask = 
          _mm_cmpistrm(xmm_field_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
        field_mask = _mm_extract_epi16(xmm_field_mask, 0);
      }

      int escape_mask = 0;

      // If the table does not use escape characters, skip processing for it.
      if (escape_char_ != '\0') {
        xmm_escape_mask = _mm_cmpistrm(xmm_escape_search_, xmm_buffer,
                                       SSEUtil::STRCHR_MODE);
        escape_mask = _mm_extract_epi16(xmm_escape_mask, 0);
        ProcessEscapeMask(escape_mask, &last_char_is_escape_, &field_mask, &tuple_mask);
      }

      // Tuple delims are automatically field delims
      field_mask |= tuple_mask;

      if (field_mask != 0) {
        // Loop through the mask and find the tuple/column offsets
        for (int n = 0; n < SSEUtil::CHARS_PER_128_BIT_REGISTER; ++n) {
          if (escape_mask != 0) {
            current_column_has_escape_ =
                current_column_has_escape_ || (escape_mask & SSEUtil::SSE_BITMASK[n]);
          }

          if (field_mask & SSEUtil::SSE_BITMASK[n]) {
            AddColumn((*byte_buffer_ptr + n) - *next_column_start,
                next_column_start, num_fields, field_locations);
          }

          if (tuple_mask & SSEUtil::SSE_BITMASK[n]) {
            column_idx_ = start_column_;
            ++(*num_tuples);
            if (*num_tuples == max_tuples) {
              (*byte_buffer_ptr) += (n + 1);
              last_char_is_escape_ = false;
              return Status::OK;
            }
          }
        }
      } else {
        current_column_has_escape_ = (current_column_has_escape_ || escape_mask);
      }

      remaining_len -= SSEUtil::CHARS_PER_128_BIT_REGISTER;
      *byte_buffer_ptr += SSEUtil::CHARS_PER_128_BIT_REGISTER;
    }
  }

  // Handle the remaining characters
  while (remaining_len > 0) {
    bool new_tuple = false;
    bool new_col = false;

    if (!last_char_is_escape_) {
      if (tuple_delim_ != '\0' && **byte_buffer_ptr == tuple_delim_) {
        new_tuple = true;
        new_col = true;
      } else if (**byte_buffer_ptr == field_delim_
                 || **byte_buffer_ptr == collection_item_delim_) {
        new_col = true;
      }
    }
    if (**byte_buffer_ptr == escape_char_) {
      current_column_has_escape_ = true;
      last_char_is_escape_ = !last_char_is_escape_;
    } else {
      last_char_is_escape_ = false;
    }

    if (new_col) {
      AddColumn(*byte_buffer_ptr - *next_column_start,
          next_column_start, num_fields, field_locations);
    }

    if (new_tuple) {
      column_idx_ = start_column_;
      ++(*num_tuples);
      if (*num_tuples == max_tuples) {
        ++*byte_buffer_ptr;
        return Status::OK;
      }
    }

    --remaining_len;
    ++*byte_buffer_ptr;

  }

  // For formats that store the length of the row the row is not delimited:
  // e.g. Sequene files.
  if (tuple_delim_ == '\0') {
    DCHECK(remaining_len == 0);
    AddColumn(*byte_buffer_ptr - *next_column_start,
        next_column_start, num_fields, field_locations);
    column_idx_ = start_column_;
    ++(*num_tuples);
  }

  return Status::OK;
}

// Find the start of the first full tuple in buffer by looking for the end of
// the previous tuple.
// TODO: most of this is not tested.  We need some tailored data to exercise the boundary
// cases
int DelimitedTextParser::FindFirstTupleStart(char* buffer, int len) {
  int tuple_start = 0;
  char* buffer_start = buffer;
restart:
  if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
    __m128i xmm_buffer, xmm_tuple_mask;
    while (tuple_start < len) {
      // TODO: can we parallelize this as well?  Are there multiple sse execution units?
      // Load the next 16 bytes into the xmm register and do strchr for the
      // tuple delimiter.
      int chr_count = len - tuple_start;
      if (chr_count > SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        chr_count = SSEUtil::CHARS_PER_128_BIT_REGISTER;
      }
      xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(buffer));
      xmm_tuple_mask =
          _mm_cmpestrm(xmm_tuple_search_, 1, xmm_buffer, chr_count, SSEUtil::STRCHR_MODE);
      int tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);
      if (tuple_mask != 0) {
        for (int i = 0; i < SSEUtil::CHARS_PER_128_BIT_REGISTER; ++i) {
          if ((tuple_mask & SSEUtil::SSE_BITMASK[i]) != 0) {
            tuple_start += i + 1;
            buffer += i + 1;
            break;
          }
        }
        break;
      }
      tuple_start += chr_count;
      buffer += chr_count;
    }
  } else {
    for (int i = tuple_start; i < len; ++i) {
      char c = *buffer++;
      if (c == tuple_delim_) {
        tuple_start = i + 1;
        break;
      }
    }
  }

  if (escape_char_ != '\0') {
    // Scan backwards for escape characters.  We do this after
    // finding the tuple break rather than during the (above)
    // forward scan to make the forward scan faster.  This will
    // perform worse if there are many characters right before the
    // tuple break that are all escape characters, but that is
    // unlikely.
    int num_escape_chars = 0;
    int before_tuple_end = tuple_start - 2;
    for (; before_tuple_end >= 0; --before_tuple_end) {
      if (buffer_start[before_tuple_end] == escape_char_) {
        ++num_escape_chars;
      } else {
        break;
      }
    }
    // TODO: This sucks.  All the preceding characters before the tuple delim were
    // escape characters.  We need to read from the previous block to see what to do.
    DCHECK_GT(before_tuple_end, 0);

    // An even number of escape characters means they cancel out and this tuple break
    // is *not* escaped.
    if (num_escape_chars % 2 != 0) {
      goto restart;
    }
  }

  return tuple_start;
}
