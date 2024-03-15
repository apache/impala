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


#ifndef IMPALA_EXEC_TEXT_CONVERTER_H
#define IMPALA_EXEC_TEXT_CONVERTER_H

#include "runtime/runtime-state.h"

#include <string>

namespace llvm {
  class Function;
}

namespace impala {

struct ColumnType;
class LlvmCodeGen;
class MemPool;
class SlotDescriptor;
class Status;
class StringValue;
class Tuple;
class TupleDescriptor;

/// Helper class for dealing with text data, e.g., converting text data to
/// numeric types, etc.
class TextConverter {
 public:
  /// escape_char: Character to indicate escape sequences.
  /// null_col_val: Special string to indicate NULL column values.
  /// check_null: If set, then the WriteSlot() functions set the target slot to NULL
  /// if their input string matches null_vol_val.
  /// strict_mode: If set, numerical overflow/underflow are considered to be parse
  /// errors.
  TextConverter(char escape_char, const std::string& null_col_val,
      bool check_null = true, bool strict_mode = false);

  /// Converts slot data, of length 'len',  into type of slot_desc,
  /// and writes the result into the tuples's slot.
  /// copy_string indicates whether we need to make a separate copy of the string data:
  /// For regular unescaped strings, we point to the original data in the file_buf_.
  /// For regular escaped strings, we copy its unescaped string into a separate buffer
  /// and point to it.
  /// If the string needs to be copied, the memory is allocated from 'pool', otherwise
  /// 'pool' is unused.
  /// Unsuccessful conversions are turned into NULLs.
  /// Returns true if the value was written successfully.
  bool WriteSlot(const SlotDescriptor* slot_desc, Tuple* tuple, const char* data, int len,
      bool copy_string, bool need_escape, MemPool* pool);

  /// Removes escape characters from len characters of the null-terminated string src,
  /// and copies the unescaped string into dest, changing *len to the unescaped length.
  /// No null-terminator is added to dest. If maxlen > 0, will only copy at most
  /// maxlen bytes into dest.
  void UnescapeString(const char* src, char* dest, int* len, int64_t maxlen = -1);

  /// Codegen the function to write a slot for slot_desc.
  /// Should only be called if the column type is supported, i.e.
  /// SupportsCodegenWriteSlot(slot_desc->type()) returns true.
  /// Returns Status::OK() if codegen was successful. If codegen was successful
  /// llvm::Function** fn points to the codegen'd function.
  /// The signature of the generated function is:
  /// bool WriteSlot(Tuple* tuple, const char* data, int len);
  /// The codegen function returns true if the slot could be written and false
  /// otherwise.
  /// If check_null is set, then the codegen'd function sets the target slot to NULL
  /// if its input string matches null_vol_val.
  /// The codegenerated function does not support escape characters and should not
  /// be used for partitions that contain escapes.
  /// strict_mode: If set, numerical overflow/underflow are considered to be parse
  /// errors.
  static Status CodegenWriteSlot(LlvmCodeGen* codegen, TupleDescriptor* tuple_desc,
      SlotDescriptor* slot_desc, llvm::Function** fn, const char* null_col_val, int len,
      bool check_null, bool strict_mode = false);

  /// Returns whether codegen is supported for the given type.
  static bool SupportsCodegenWriteSlot(const ColumnType& col_type);

 private:
  char escape_char_;
  /// Special string to indicate NULL column values.
  std::string null_col_val_;
  /// Indicates whether we should check for null_col_val_ and set slots to NULL.
  bool check_null_;
  /// Indicates whether numerical overflow/underflow are considered to be parse
  /// errors.
  bool strict_mode_;
};

}

#endif
