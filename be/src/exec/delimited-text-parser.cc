// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "util/cpu-info.h"
#include "exec/hdfs-scanner.h"
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
  char search_chars[SSEUtil::CHARS_PER_128_BIT_REGISTER];
  memset(search_chars, 0, sizeof(search_chars));
  if (escape_char_ != '\0') {
    search_chars[0] = escape_char_;
    xmm_escape_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));

    // To process escape characters, we need to check if there was an escape
    // character between (col_start,col_end).  The SSE instructions return
    // a bit mask for 16 bits so we need to mask off the bits below col_start
    // and after col_end.
    // TODO: processing escapes still takes a while (up to 20%).  For tables that
    // don't have an escape character set, this can be avoided. Consider duplicating
    // ParseFieldLocations to have a version that doesn't deal with escapes.
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

  int num_delims = 0;
  if (tuple_delim != '\0') {
    search_chars[num_delims++] = tuple_delim_;
    xmm_tuple_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));
  }

  if (field_delim != '\0' || collection_item_delim != '\0') {
    search_chars[num_delims++] = field_delim_;
    search_chars[num_delims++] = collection_item_delim_;
  }

  DCHECK_GT(num_delims, 0);
  xmm_delim_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));

  column_idx_ = start_column_;
  current_column_has_escape_ = false;
  last_char_is_escape_ = false;
}


// Updates the values in the field and tuple masks, escaping them if necessary.
// If the character at n is an escape character, then delimiters(tuple/field/escape
// characters) at n+1 don't count.
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
//  Needle   = 'abcd000000000000' (we're searching for any a's, b's, c's or d's)
//  Haystack = 'asdfghjklhjbdwwc' (the raw string)
//  Result   = '101000000001101'
// TODO: can we parallelize this as well?  Are there multiple sse execution units?
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
  //  - xmm_delim_search_: the delim search register.  Contains field delimiter,
  //        collection_item delim_char and tuple delimiter
  //  - xmm_escape_search_: the escape search register. Only contains escape char
  //  - xmm_delim_mask: the result of doing strchr for the delimiters
  //  - xmm_escape_mask: the result of doing strchr for the escape char
  __m128i xmm_buffer, xmm_delim_mask, xmm_escape_mask;

  if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
    while (LIKELY(remaining_len >= SSEUtil::CHARS_PER_128_BIT_REGISTER)) {
      // Load the next 16 bytes into the xmm register
      xmm_buffer = _mm_loadu_si128(reinterpret_cast<__m128i*>(*byte_buffer_ptr));

      // Do the strchr for tuple and field breaks
      // The strchr sse instruction returns the result in the lower bits of the sse
      // register.  Since we only process 16 characters at a time, only the lower 16 bits
      // can contain non-zero values.
      // _mm_extract_epi16 will extract 16 bits out of the xmm register.  The second
      // parameter specifies which 16 bits to extract (0 for the lowest 16 bits).
      xmm_delim_mask =
          _mm_cmpistrm(xmm_delim_search_, xmm_buffer, SSEUtil::STRCHR_MODE);
      uint16_t delim_mask = _mm_extract_epi16(xmm_delim_mask, 0);

      uint16_t escape_mask = 0;
      // If the table does not use escape characters, skip processing for it.
      if (escape_char_ != '\0') {
        xmm_escape_mask = _mm_cmpistrm(xmm_escape_search_, xmm_buffer,
                                       SSEUtil::STRCHR_MODE);
        escape_mask = _mm_extract_epi16(xmm_escape_mask, 0);
        ProcessEscapeMask(escape_mask, &last_char_is_escape_, &delim_mask);
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

        // Determine if there was an escape character between [last_col_idx, n]
        bool escaped = (escape_mask & low_mask_[last_col_idx] & high_mask_[n]) != 0;
        current_column_has_escape_ |= escaped;
        last_col_idx = n;

        AddColumn((*byte_buffer_ptr + n) - *next_column_start,
            next_column_start, num_fields, field_locations);

        if ((*byte_buffer_ptr)[n] == tuple_delim_) {
          column_idx_ = start_column_;
          ++(*num_tuples);
          if (UNLIKELY(*num_tuples == max_tuples)) {
            (*byte_buffer_ptr) += (n + 1);
            last_char_is_escape_ = false;
            return Status::OK;
          }
        }
      }

      // Determine if there was an escape character between (last_col_idx, 15)
      bool unprocessed_escape = escape_mask & low_mask_[last_col_idx] & high_mask_[15];
      current_column_has_escape_ |= unprocessed_escape;

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

  // For formats that store the length of the row, the row is not delimited:
  // e.g. Sequence files.
  if (tuple_delim_ == '\0') {
    DCHECK(remaining_len == 0);
    AddColumn(*byte_buffer_ptr - *next_column_start,
        next_column_start, num_fields, field_locations);
    column_idx_ = start_column_;
    ++(*num_tuples);
  }

  return Status::OK;
}

// Find the first intance of the tuple delimiter.  This will
// find the start of the first full tuple in buffer by looking for the end of
// the previous tuple.
// TODO: most of this is not tested.  We need some tailored data to exercise the boundary
// cases
int DelimitedTextParser::FindFirstInstance(char* buffer, int len) {
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

// The start of the sync block is specified by an integer of -1.
// By setting the tuple deimiter to 0xff we can do a fast search of the
// bytes till we find a -1 and then look for 3 more -1 bytes which will make up
// the integer.  This is followed by the 16 byte sync block which was specified in
// the file header.
// TODO: Can we user strstr see mode?
Status DelimitedTextParser::FindSyncBlock(int end_of_range, int sync_size,
                                          uint8_t* sync, ByteStream*  byte_stream) {
  // A sync block is preceeded by 4 bytes of -1 (tuple_delim_).
  int sync_flag_counter = 0;
  // Starting offset of the buffer we are scanning
  int64_t buf_start = 0;
  // Number of bytes read from stream
  int64_t num_bytes_read = 0;
  // Current offset into buffer.
  int64_t off = 0;
  // Bytes left to process in buffer.
  int64_t bytes_left = 0;
  // Size of buffer to read.
  int64_t read_size = HdfsScanner::FILE_BLOCK_SIZE;
  // Buffer to scan.
  uint8_t buf[read_size];

  // Loop until we find a Sync block or get to the end of the range.
  while (buf_start + off < end_of_range && sync_flag_counter == 0) {
    // If there are no bytes left to process in the buffer get some more.
    if (bytes_left == 0) {
      if (buf_start == 0) {
        RETURN_IF_ERROR(byte_stream->GetPosition(&buf_start));
      } else {
        // Seek to the next buffer, in case we read the byte stream below.
        buf_start += num_bytes_read;
        RETURN_IF_ERROR(byte_stream->Seek(buf_start));
      }
      // Do not to read past the end of range, unless we stopped at a -1 byte.
      // This could be the start of a sync marker and we need to process the data
      // after this point.
      if (buf_start + read_size >= end_of_range) {
        read_size = (end_of_range - buf_start);
        if (sync_flag_counter != 0 && read_size < 4 - sync_flag_counter) {
          read_size = 4 - sync_flag_counter;
        }
      }
      if (read_size == 0) {
        return Status::OK;
      }
      RETURN_IF_ERROR(byte_stream->Read(buf, read_size, &num_bytes_read));
      off = 0;
      if (num_bytes_read == 0) {
        return Status::OK;
      }
      bytes_left = num_bytes_read;
    }

    if (sync_flag_counter == 0) {
      off += FindFirstInstance(reinterpret_cast<char*>(buf + off), bytes_left);
      bytes_left = num_bytes_read - off;

      // If we read to the end of the buffer, we did not find a -1.
      if (bytes_left == 0) continue;

      sync_flag_counter = 1;
    }

    // We found a -1 see if there are 3 more
    while (bytes_left != 0) {
      --bytes_left;
      if (buf[off++] != static_cast<uint8_t>(tuple_delim_)) {
        sync_flag_counter = 0;
        break;
      }
      if (++sync_flag_counter == 4) {
        if (bytes_left < sync_size) {
          // Reset the buffer to contain the whole sync block.
          buf_start = buf_start + off;
          RETURN_IF_ERROR(byte_stream->Seek(buf_start));
          RETURN_IF_ERROR(byte_stream->Read(buf, sync_size, &num_bytes_read));
          off = 0;
        }
        if (!memcmp(static_cast<void*>(&buf[off]), static_cast<void*>(sync), sync_size)) {
          // Seek to the beginning of the sync so the protocol readers are right.
          RETURN_IF_ERROR(byte_stream->Seek(buf_start + off - 4));
          return Status::OK;
        }
        sync_flag_counter = 0;
        break;
      }
    }
  }
  return Status::OK;

}
