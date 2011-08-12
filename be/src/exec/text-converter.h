// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_TEXT_CONVERTER_H
#define IMPALA_EXEC_TEXT_CONVERTER_H

namespace impala {

class Tuple;
class SlotDescriptor;
class MemPool;

// Helper class for dealing with text data, e.g., converting text data to numeric types, etc.
class TextConverter {
 public:
  TextConverter(bool strings_are_quoted, char escape_char, MemPool* var_len_pool);

  // Converts slot data (begin, end) into type of slot_desc,
  // and writes the result into the tuples's slot.
  // copy_string indicates whether we need to make a separate copy of the string data:
  // For regular unescaped strings, we point to the original data in the file_buf_.
  // For regular escaped strings,
  // we copy an its unescaped string into a separate buffer and point to it.
  // Unsuccessful conversions are turned into NULLs.
  // Returns true if value was converted and written successfully, false otherwise.
  bool ConvertAndWriteSlotBytes(const char* begin,
      const char* end, Tuple* tuple, const SlotDescriptor* slot_desc,
      bool copy_string, bool unescape_string);

  // Removes escape characters from len characters of the null-terminated string src,
  // and copies the unescaped string into dest, changing *len to the unescaped length.
  // No null-terminator is added to dest.
  void UnescapeString(const char* src, char* dest, int* len);

 private:
  bool strings_are_quoted_;
  char escape_char_;
  // Pool used to allocate memory for string copies.
  MemPool* var_len_pool_;
};

}

#endif
