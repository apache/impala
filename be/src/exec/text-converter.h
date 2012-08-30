// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_TEXT_CONVERTER_H
#define IMPALA_EXEC_TEXT_CONVERTER_H

#include "runtime/runtime-state.h"

namespace llvm {
  class Function;
}

namespace impala {

class LlvmCodeGen;
class MemPool;
class SlotDescriptor;
class Status;
class StringValue;
class Tuple;
class TupleDescriptor;

// Helper class for dealing with text data, e.g., converting text data to
// numeric types, etc.
class TextConverter {
 public:
  TextConverter(char escape_char);

  // Converts slot data, of length 'len',  into type of slot_desc,
  // and writes the result into the tuples's slot.
  // copy_string indicates whether we need to make a separate copy of the string data:
  // For regular unescaped strings, we point to the original data in the file_buf_.
  // For regular escaped strings, we copy an its unescaped string into a separate buffer 
  // and point to it.
  // If the string needs to be copied, the memory is allocated from 'pool', otherwise
  // 'pool' is unused.
  // Unsuccessful conversions are turned into NULLs.
  // Returns true if the value was written successfully.
  bool WriteSlot(const SlotDescriptor* slot_desc, Tuple* tuple, 
      const char* data, int len, bool copy_string, bool need_escape, MemPool* pool);

  // Removes escape characters from len characters of the null-terminated string src,
  // and copies the unescaped string into dest, changing *len to the unescaped length.
  // No null-terminator is added to dest.
  void UnescapeString(const char* src, char* dest, int* len);

  // Removes escape characters from 'str', allocating a new string from pool.
  // 'str' is updated with the new ptr and length.
  void UnescapeString(StringValue* str, MemPool* pool);

  // Codegen the function to write a slot for slot_desc.
  // Returns NULL if codegen was not succesful. 
  // The signature of the generated function is:
  // bool WriteSlot(Tuple* tuple, const char* data, int len);
  // The codegen function returns true if the slot could be written and false
  // otherwise.
  llvm::Function* CodegenWriteSlot(LlvmCodeGen*, 
      TupleDescriptor* tuple_desc, SlotDescriptor* slot_desc);

 private:
  char escape_char_;
};

}

#endif
