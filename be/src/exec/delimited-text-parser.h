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


#ifndef IMPALA_EXEC_DELIMITED_TEXT_PARSER_H
#define IMPALA_EXEC_DELIMITED_TEXT_PARSER_H

#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "util/sse-util.h"

namespace impala {

template <bool DELIMITED_TUPLES>
class DelimitedTextParser {
 public:

  /// The Delimited Text Parser parses text rows that are delimited by specific
  /// characters:
  ///   tuple_delim: delimits tuples.  Only used if DELIMITED_TUPLES is true.
  ///   field_delim: delimits fields
  ///   collection_item_delim: delimits collection items
  ///   escape_char: escape delimiters, make them part of the data.
  ///
  /// If the template parameter DELIMITED_TUPLES is false there is no support
  /// for tuple delimiters and we do not need to search for them.  Any value
  /// may be passed for tuple_delim, as it is ignored.
  ///
  /// 'num_cols' is the total number of columns including partition keys.
  ///
  /// 'is_materialized_col' should be initialized to an array of length 'num_cols', with
  /// is_materialized_col[i] = <true if column i should be materialized, false otherwise>
  /// Owned by caller.
  ///
  /// The main method is ParseData which fills in a vector of pointers and lengths to the
  /// fields.  It also can handle an escape character which masks a tuple or field
  /// delimiter that occurs in the data.
  DelimitedTextParser(
      int num_cols, int num_partition_keys, const bool* is_materialized_col,
      char tuple_delim, char field_delim_ = '\0', char collection_item_delim = '^',
      char escape_char = '\0');

  /// Called to initialize parser at beginning of scan range.
  void ParserReset();

  /// Check if we are at the start of a tuple.
  bool AtTupleStart() { return column_idx_ == num_partition_keys_; }

  char escape_char() const { return escape_char_; }

  /// Parses a byte buffer for the field and tuple breaks.
  /// This function will write the field start & len to field_locations
  /// which can then be written out to tuples.
  /// This function uses SSE ("Intel x86 instruction set extension
  /// 'Streaming Simd Extension') if the hardware supports SSE4.2
  /// instructions.  SSE4.2 added string processing instructions that
  /// allow for processing 16 characters at a time.  Otherwise, this
  /// function walks the file_buffer_ character by character.
  /// Input Parameters:
  ///   max_tuples: The maximum number of tuples that should be parsed.
  ///               This is used to control how the batching works.
  ///   remaining_len: Length of data remaining in the byte_buffer_pointer.
  ///   byte_buffer_pointer: Pointer to the buffer containing the data to be parsed.
  /// Output Parameters:
  ///   field_locations: array of pointers to data fields and their lengths
  ///   num_tuples: Number of tuples parsed
  ///   num_fields: Number of materialized fields parsed
  ///   next_column_start: pointer within file_buffer_ where the next field starts
  ///                      after the return from the call to ParseData
  /// Returns an error status if any column exceeds the size limit.
  /// See AddColumn() for details.
  Status ParseFieldLocations(int max_tuples, int64_t remaining_len,
      char** byte_buffer_ptr, char** row_end_locations,
      FieldLocation* field_locations,
      int* num_tuples, int* num_fields, char** next_column_start);

  /// Parse a single tuple from buffer.
  /// - buffer/len are input parameters for the entire record.
  /// - on return field_locations will contain the start/len for each materialized
  ///   col.
  /// - *num_fields returns the number of fields processed.
  /// This function is used to parse sequence file records which do not need to
  /// parse for tuple delimiters. Returns an error status if any column exceeds the
  /// size limit. See AddColumn() for details.
  /// This function is disabled for non-sequence file parsing.
  template <bool PROCESS_ESCAPES>
  Status ParseSingleTuple(int64_t len, char* buffer, FieldLocation* field_locations,
      int* num_fields);

  /// FindFirstInstance returns the position after the first non-escaped tuple
  /// delimiter from the starting offset.
  /// Used to find the start of a tuple if jumping into the middle of a text file.
  /// If no tuple delimiter is found within the buffer, return -1;
  int64_t FindFirstInstance(const char* buffer, int64_t len);

  /// Will we return the current column to the query?
  /// Hive allows cols at the end of the table that are not in the schema.  We'll
  /// just ignore those columns
  bool ReturnCurrentColumn() const {
    return column_idx_ < num_cols_ && is_materialized_col_[column_idx_];
  }

  /// Fill in columns missing at the end of the tuple.
  /// 'len' and 'last_column' may contain the length and the pointer to the
  /// last column on which the file ended without a delimiter.
  /// Fills in the offsets and lengths in field_locations.
  /// If parsing stopped on a delimiter and there is no last column then length will be 0.
  /// Other columns beyond that are filled with 0 length fields.
  /// 'num_fields' points to an initialized count of fields and will incremented
  /// by the number fields added.
  /// 'field_locations' will be updated with the start and length of the fields.
  /// Returns an error status if 'len' exceeds the size limit specified in AddColumn().
  template <bool PROCESS_ESCAPES>
  Status FillColumns(int64_t len, char** last_column, int* num_fields,
      impala::FieldLocation* field_locations);

  /// Return true if we have not seen a tuple delimiter for the current tuple being
  /// parsed (i.e., the last byte read was not a tuple delimiter).
  bool HasUnfinishedTuple() {
    DCHECK(DELIMITED_TUPLES);
    return unfinished_tuple_;
  }

 private:
  /// Initialize the parser state.
  void ParserInit(HdfsScanNode* scan_node);

  /// Helper routine to add a column to the field_locations vector.
  /// Template parameter:
  ///   PROCESS_ESCAPES -- if true the the column may have escape characters
  ///                      and the negative of the len will be stored.
  ///   len: length of the current column. The length of a column must fit in a 32-bit
  ///        signed integer (i.e. <= 2147483647 bytes). If a column is larger than that,
  ///        it will be treated as an error.
  /// Input/Output:
  ///   next_column_start: Start of the current column, moved to the start of the next.
  ///   num_fields: current number of fields processed, updated to next field.
  /// Output:
  ///   field_locations: updated with start and length of current field.
  /// Return an error status if 'len' exceeds the size limit specified above.
  template <bool PROCESS_ESCAPES>
  Status AddColumn(int64_t len, char** next_column_start, int* num_fields,
      FieldLocation* field_locations);

  /// Helper routine to parse delimited text using SSE instructions.
  /// Identical arguments as ParseFieldLocations.
  /// If the template argument, 'PROCESS_ESCAPES' is true, this function will handle
  /// escapes, otherwise, it will assume the text is unescaped.  By using templates,
  /// we can special case the un-escaped path for better performance.  The unescaped
  /// path is optimized away by the compiler. Returns an error status if the length
  /// of any column exceeds the size limit. See AddColumn() for details.
  template <bool PROCESS_ESCAPES>
  Status ParseSse(int max_tuples, int64_t* remaining_len,
      char** byte_buffer_ptr, char** row_end_locations_,
      FieldLocation* field_locations,
      int* num_tuples, int* num_fields, char** next_column_start);

  bool IsFieldOrCollectionItemDelimiter(char c) {
    return (!DELIMITED_TUPLES && c == field_delim_) ||
      (DELIMITED_TUPLES && field_delim_ != tuple_delim_ && c == field_delim_) ||
      (collection_item_delim_ != '\0' && c == collection_item_delim_);
  }
#ifdef __x86_64__
  /// SSE(xmm) register containing the tuple search character(s).
  __m128i xmm_tuple_search_;

  /// SSE(xmm) register containing the delimiter search character(s).
  __m128i xmm_delim_search_;

  /// SSE(xmm) register containing the escape search character.
  __m128i xmm_escape_search_;
#endif
  /// For each col index [0, num_cols_), true if the column should be materialized.
  /// Not owned.
  const bool* is_materialized_col_;

  /// The number of delimiters contained in xmm_tuple_search_, i.e. its length.
  int num_tuple_delims_;

  /// The number of delimiters contained in xmm_delim_search_, i.e. its length.
  int num_delims_;

  /// Number of columns in the table (including partition columns)
  int num_cols_;

  /// Number of partition columns in the table.
  int num_partition_keys_;

  /// Index to keep track of the current column in the current file
  int column_idx_;

  /// Used for special processing of \r.
  /// This will be the offset of the last instance of \r from the end of the
  /// current buffer being searched unless the last row delimiter was not a \r in which
  /// case it will be -1.  If the last character in a buffer is \r then the value
  /// will be 0.  At the start of processing a new buffer if last_row_delim_offset_ is 0
  /// then it is set to be one more than the size of the buffer so that if the buffer
  /// starts with \n it is processed as \r\n.
  int32_t last_row_delim_offset_;

  /// Precomputed masks to process escape characters
  uint16_t low_mask_[16];
  uint16_t high_mask_[16];

  /// Character delimiting fields (to become slots).
  char field_delim_;

  /// True if this parser should handle escape characters.
  bool process_escapes_;

  /// Escape character. Only used if process_escapes_ is true.
  char escape_char_;

  /// Character delimiting collection items (to become slots).
  char collection_item_delim_;

  /// Character delimiting tuples.  Only used if DELIMITED_TUPLES is true.
  char tuple_delim_;

  /// Whether or not the current column has an escape character in it
  /// (and needs to be unescaped)
  bool current_column_has_escape_;

  /// Whether or not the previous character was the escape character
  bool last_char_is_escape_;

  /// True if the last tuple is unfinished (not ended with tuple delimiter).
  bool unfinished_tuple_;
};

using TupleDelimitedTextParser = DelimitedTextParser<true>;
using SequenceDelimitedTextParser = DelimitedTextParser<false>;

}// namespace impala
#endif// IMPALA_EXEC_DELIMITED_TEXT_PARSER_H
