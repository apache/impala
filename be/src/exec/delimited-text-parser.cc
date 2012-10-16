// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/delimited-text-parser.inline.h"

#include "exec/byte-stream.h"
#include "exec/hdfs-scanner.h"
#include "util/cpu-info.h"

using namespace impala;
using namespace std;

DelimitedTextParser::DelimitedTextParser(HdfsScanNode* scan_node,
                                         char tuple_delim,
                                         char field_delim,
                                         char collection_item_delim,
                                         char escape_char)
    : scan_node_(scan_node),
      field_delim_(field_delim),
      escape_char_(escape_char),
      collection_item_delim_(collection_item_delim),
      tuple_delim_(tuple_delim),
      current_column_has_escape_(false),
      last_char_is_escape_(false),
      last_row_delim_offset_(-1),
      column_idx_(0) {

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
    // Hive will treats \r (^M) as an alternate tuple delimiter, but \r\n is a
    // single tuple delimiter.
    if (tuple_delim_ == '\n') search_chars[num_delims++] = '\r';
    xmm_tuple_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));
  }

  if (field_delim != '\0' || collection_item_delim != '\0') {
    search_chars[num_delims++] = field_delim_;
    search_chars[num_delims++] = collection_item_delim_;
  }

  DCHECK_GT(num_delims, 0);
  xmm_delim_search_ = _mm_loadu_si128(reinterpret_cast<__m128i*>(search_chars));

  // scan_node_ can only be NULL in test setups
  if (scan_node_ != NULL) ParserReset();
}

void DelimitedTextParser::ParserReset() {
  current_column_has_escape_ = false;
  last_char_is_escape_ = false;
  last_row_delim_offset_ = -1;
  column_idx_ = scan_node_->num_partition_keys();
}

// Parsing raw csv data into FieldLocation descriptors.
Status DelimitedTextParser::ParseFieldLocations(int max_tuples, int64_t remaining_len,
    char** byte_buffer_ptr, char** row_end_locations, 
    vector<FieldLocation>* field_locations,
    int* num_tuples, int* num_fields, char** next_column_start) {
  // Start of this batch.
  *next_column_start = *byte_buffer_ptr;
  // If there was a '\r' at the end of the last batch, set the offset to
  // just before the begining. Otherwise make it invalid.
  if (last_row_delim_offset_ == 0) {
    last_row_delim_offset_ = remaining_len;
  } else {
    last_row_delim_offset_ = -1;
  }

  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    if (escape_char_ == '\0') {
      ParseSse<false>(max_tuples, &remaining_len, byte_buffer_ptr, row_end_locations,
          field_locations, num_tuples, num_fields, next_column_start);
    } else {
      ParseSse<true>(max_tuples, &remaining_len, byte_buffer_ptr, row_end_locations,
          field_locations, num_tuples, num_fields, next_column_start);
    }
  }

  if (*num_tuples == max_tuples) return Status::OK;

  // Handle the remaining characters
  while (remaining_len > 0) {
    bool new_tuple = false;
    bool new_col = false;

    if (!last_char_is_escape_) {
      if (tuple_delim_ != '\0' && (**byte_buffer_ptr == tuple_delim_ ||
           (tuple_delim_ == '\n' && **byte_buffer_ptr == '\r'))) {
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


    if (new_tuple) {
      if (last_row_delim_offset_ == remaining_len && **byte_buffer_ptr == '\n') {
        // If the row ended in \r\n then move to the \n
        ++*next_column_start;
      } else {
        AddColumn<true>(*byte_buffer_ptr - *next_column_start,
            next_column_start, num_fields, field_locations);
        FillColumns<false>(0, NULL, num_fields, field_locations);
        column_idx_ = scan_node_->num_partition_keys();
        row_end_locations[*num_tuples] = *byte_buffer_ptr;
        ++(*num_tuples);
      }
      last_row_delim_offset_ = **byte_buffer_ptr == '\r' ? remaining_len - 1 : -1;
      if (*num_tuples == max_tuples) {
        ++*byte_buffer_ptr;
        --remaining_len;
        if (last_row_delim_offset_ == remaining_len) last_row_delim_offset_ = 0;
        return Status::OK;
      }
    } else if (new_col) {
      AddColumn<true>(*byte_buffer_ptr - *next_column_start,
          next_column_start, num_fields, field_locations);
    }

    --remaining_len;
    ++*byte_buffer_ptr;
  }

  // For formats that store the length of the row, the row is not delimited:
  // e.g. Sequence files.
  if (tuple_delim_ == '\0') {
    DCHECK(remaining_len == 0);
    AddColumn<true>(*byte_buffer_ptr - *next_column_start,
        next_column_start, num_fields, field_locations);
    FillColumns<false>(0, NULL, num_fields, field_locations);
    column_idx_ = scan_node_->num_partition_keys();
    ++(*num_tuples);
  }

  return Status::OK;
}

// Find the first instance of the tuple delimiter.  This will
// find the start of the first full tuple in buffer by looking for the end of
// the previous tuple.
int DelimitedTextParser::FindFirstInstance(const char* buffer, int len) {
  int tuple_start = 0;
  const char* buffer_start = buffer;
  bool found = false;

  // If the last char in the previous buffer was \r then either return the start of 
  // this buffer or skip a \n at the beginning of the buffer.
  if (last_row_delim_offset_ != -1) {
    if (*buffer_start == '\n') return 1;
    return 0;
  }
restart:
  if (CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    __m128i xmm_buffer, xmm_tuple_mask;
    while (tuple_start < len) {
      // TODO: can we parallelize this as well?  Are there multiple sse execution units?
      // Load the next 16 bytes into the xmm register and do strchr for the
      // tuple delimiter.
      int chr_count = len - tuple_start;
      if (chr_count > SSEUtil::CHARS_PER_128_BIT_REGISTER) {
        chr_count = SSEUtil::CHARS_PER_128_BIT_REGISTER;
      }
      xmm_buffer = _mm_loadu_si128(reinterpret_cast<const __m128i*>(buffer));
      // This differs from ParseSse by using the slower cmpestrm instruction which
      // takes a chr_count and can search less than 16 bytes at a time.
      xmm_tuple_mask =
          _mm_cmpestrm(xmm_tuple_search_, 1, xmm_buffer, chr_count, SSEUtil::STRCHR_MODE);
      int tuple_mask = _mm_extract_epi16(xmm_tuple_mask, 0);
      if (tuple_mask != 0) {
        found = true;
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
      if (c == tuple_delim_ || (c == '\r' && tuple_delim_ == '\n')) {
        tuple_start = i + 1;
        found = true;
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
    if (before_tuple_end < 0) {
      static bool warning_logged = false;
      if (!warning_logged) {
        LOG(WARNING) << "Unhandled code path.  This might cause a tuple to be "
                     << "skipped or repeated.";
        warning_logged = true;
        return tuple_start;
      }
    }

    // An even number of escape characters means they cancel out and this tuple break
    // is *not* escaped.
    if (num_escape_chars % 2 != 0) {
      goto restart;
    }
  }

  if (!found) return -1;
  if (tuple_start == len - 1 && buffer_start[tuple_start] == '\r') {
    // If \r is the last char we need to wait to see if the next one is \n or not.
    last_row_delim_offset_ = 0;
    return -1;
  }
  if (buffer_start[tuple_start] == '\n' && buffer_start[tuple_start - 1] == '\r') {
    // We have \r\n, move to the next character.
    ++tuple_start;
  }
  return tuple_start;
}

// The start of the sync block is specified by an integer of -1.
// By setting the tuple delimiter to 0xff we can do a fast search of the
// bytes till we find a -1 and then look for 3 more -1 bytes which will make up
// the integer.  This is followed by the 16 byte sync block which was specified in
// the file header.
// TODO: Can we user strstr see mode?
Status DelimitedTextParser::FindSyncBlock(int end_of_range, int sync_size,
                                          uint8_t* sync, ByteStream*  byte_stream) {
  // A sync block is preceded by 4 bytes of -1 (tuple_delim_).
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
