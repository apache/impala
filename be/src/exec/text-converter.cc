// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <boost/algorithm/string.hpp>

#include "codegen/llvm-codegen.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/tuple.h"
#include "text-converter.h"
#include "util/string-parser.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;

TextConverter::TextConverter(char escape_char, const string& null_col_val,
    bool check_null)
  : escape_char_(escape_char),
    null_col_val_(null_col_val),
    check_null_(check_null) {
}

void TextConverter::UnescapeString(StringValue* value, MemPool* pool) {
  char* new_data = reinterpret_cast<char*>(pool->Allocate(value->len));
  UnescapeString(value->ptr, new_data, &value->len);
  value->ptr = new_data;
}

void TextConverter::UnescapeString(const char* src, char* dest, int* len,
    int64_t maxlen) {
  const char* src_end = src + *len;
  char* dest_end = dest + *len;
  if (maxlen > 0) dest_end = dest + maxlen;
  char* dest_ptr = dest;
  bool escape_next_char = false;

  while ((src < src_end) && (dest_ptr < dest_end)) {
    if (*src == escape_char_) {
      escape_next_char = !escape_next_char;
    } else {
      escape_next_char = false;
    }
    if (escape_next_char) {
      ++src;
    } else {
      *dest_ptr++ = *src++;
    }
  }
  char* dest_start = reinterpret_cast<char*>(dest);
  *len = dest_ptr - dest_start;
}

// Codegen for a function to parse one slot.  The IR for a int slot looks like:
// define i1 @WriteSlot({ i8, i32 }* %tuple_arg, i8* %data, i32 %len) {
// entry:
//   %parse_result = alloca i32
//   %0 = call i1 @IsNullString(i8* %data, i32 %len)
//   br i1 %0, label %set_null, label %check_zero
//
// set_null:                                         ; preds = %check_zero, %entry
//   call void @SetNull({ i8, i32 }* %tuple_arg)
//   ret i1 true
//
// parse_slot:                                       ; preds = %check_zero
//   %slot = getelementptr inbounds { i8, i32 }* %tuple_arg, i32 0, i32 1
//   %1 = call i32 @IrStringToInt32(i8* %data, i32 %len, i32* %parse_result)
//   %parse_result1 = load i32* %parse_result
//   %failed = icmp eq i32 %parse_result1, 1
//   br i1 %failed, label %parse_fail, label %parse_success
//
// check_zero:                                       ; preds = %entry
//   %2 = icmp eq i32 %len, 0
//   br i1 %2, label %set_null, label %parse_slot
//
// parse_success:                                    ; preds = %parse_slot
//   store i32 %1, i32* %slot
//   ret i1 true
//
// parse_fail:                                       ; preds = %parse_slot
//   call void @SetNull({ i8, i32 }* %tuple_arg)
//   ret i1 false
// }
Function* TextConverter::CodegenWriteSlot(LlvmCodeGen* codegen,
    TupleDescriptor* tuple_desc, SlotDescriptor* slot_desc,
    const char* null_col_val, int len, bool check_null) {
  if (slot_desc->type().type == TYPE_CHAR) {
    LOG(INFO) << "Char isn't supported for CodegenWriteSlot";
    return NULL;
  }
  SCOPED_TIMER(codegen->codegen_timer());

  // Codegen is_null_string
  bool is_default_null = (len == 2 && null_col_val[0] == '\\' && null_col_val[1] == 'N');
  Function* is_null_string_fn;
  if (is_default_null) {
    is_null_string_fn = codegen->GetFunction(IRFunction::IS_NULL_STRING);
  } else {
    is_null_string_fn = codegen->GetFunction(IRFunction::GENERIC_IS_NULL_STRING);
  }
  if (is_null_string_fn == NULL) return NULL;

  StructType* tuple_type = tuple_desc->GenerateLlvmStruct(codegen);
  if (tuple_type == NULL) return NULL;
  PointerType* tuple_ptr_type = PointerType::get(tuple_type, 0);

  Function* set_null_fn = slot_desc->CodegenUpdateNull(codegen, tuple_type, true);
  if (set_null_fn == NULL) {
    LOG(ERROR) << "Could not codegen WriteSlot because slot update codegen failed.";
    return NULL;
  }

  LlvmCodeGen::FnPrototype prototype(
      codegen, "WriteSlot", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_arg", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("data", codegen->ptr_type()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("len", codegen->GetType(TYPE_INT)));

  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  BasicBlock* set_null_block, *parse_slot_block, *check_zero_block = NULL;
  codegen->CreateIfElseBlocks(fn, "set_null", "parse_slot",
      &set_null_block, &parse_slot_block);

  if (!slot_desc->type().IsVarLenStringType()) {
    check_zero_block = BasicBlock::Create(codegen->context(), "check_zero", fn);
  }

  // Check if the data matches the configured NULL string.
  Value* is_null;
  if (check_null) {
    if (is_default_null) {
      is_null = builder.CreateCall2(is_null_string_fn, args[1], args[2]);
    } else {
      is_null = builder.CreateCall4(is_null_string_fn, args[1], args[2],
          codegen->CastPtrToLlvmPtr(codegen->ptr_type(),
              const_cast<char*>(null_col_val)),
          codegen->GetIntConstant(TYPE_INT, len));
    }
  } else {
    // Constant FALSE as branch condition. We rely on later optimization passes
    // to remove the branch and THEN block.
    is_null = codegen->false_value();
  }
  builder.CreateCondBr(is_null, set_null_block,
      (slot_desc->type().IsVarLenStringType()) ? parse_slot_block : check_zero_block);

  if (!slot_desc->type().IsVarLenStringType()) {
    builder.SetInsertPoint(check_zero_block);
    // If len <= 0 and it is not a string col, set slot to NULL
    // The len can be less than 0 if the field contained an escape character which
    // is only valid for string cols.
    Value* null_len = builder.CreateICmpSLE(
        args[2], codegen->GetIntConstant(TYPE_INT, 0));
    builder.CreateCondBr(null_len, set_null_block, parse_slot_block);
  }

  // Codegen parse slot block
  builder.SetInsertPoint(parse_slot_block);
  Value* slot = builder.CreateStructGEP(args[0], slot_desc->field_idx(), "slot");

  if (slot_desc->type().IsVarLenStringType()) {
    Value* ptr = builder.CreateStructGEP(slot, 0, "string_ptr");
    Value* len = builder.CreateStructGEP(slot, 1, "string_len");

    builder.CreateStore(args[1], ptr);
    // TODO codegen memory allocation for CHAR
    DCHECK(slot_desc->type().type != TYPE_CHAR);
    if (slot_desc->type().type == TYPE_VARCHAR) {
      // determine if we need to truncate the string
      Value* maxlen = codegen->GetIntConstant(TYPE_INT, slot_desc->type().len);
      Value* len_lt_maxlen = builder.CreateICmpSLT(args[2], maxlen, "len_lt_maxlen");
      Value* minlen = builder.CreateSelect(len_lt_maxlen, args[2], maxlen,
                                           "select_min_len");
      builder.CreateStore(minlen, len);
    } else {
      builder.CreateStore(args[2], len);
    }
    builder.CreateRet(codegen->true_value());
  } else {
    IRFunction::Type parse_fn_enum;
    Function* parse_fn = NULL;
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN:
        parse_fn_enum = IRFunction::STRING_TO_BOOL;
        break;
      case TYPE_TINYINT:
        parse_fn_enum = IRFunction::STRING_TO_INT8;
        break;
      case TYPE_SMALLINT:
        parse_fn_enum = IRFunction::STRING_TO_INT16;
        break;
      case TYPE_INT:
        parse_fn_enum = IRFunction::STRING_TO_INT32;
        break;
      case TYPE_BIGINT:
        parse_fn_enum = IRFunction::STRING_TO_INT64;
        break;
      case TYPE_FLOAT:
        parse_fn_enum = IRFunction::STRING_TO_FLOAT;
        break;
      case TYPE_DOUBLE:
        parse_fn_enum = IRFunction::STRING_TO_DOUBLE;
        break;
      default:
        DCHECK(false);
        return NULL;
    }
    parse_fn = codegen->GetFunction(parse_fn_enum);
    DCHECK(parse_fn != NULL);

    // Set up trying to parse the string to the slot type
    BasicBlock* parse_success_block, *parse_failed_block;
    codegen->CreateIfElseBlocks(fn, "parse_success", "parse_fail",
        &parse_success_block, &parse_failed_block);
    LlvmCodeGen::NamedVariable parse_result("parse_result", codegen->GetType(TYPE_INT));
    Value* parse_result_ptr = codegen->CreateEntryBlockAlloca(fn, parse_result);
    Value* failed_value = codegen->GetIntConstant(TYPE_INT, StringParser::PARSE_FAILURE);

    // Call Impala's StringTo* function
    Value* result = builder.CreateCall3(parse_fn, args[1], args[2], parse_result_ptr);
    Value* parse_result_val = builder.CreateLoad(parse_result_ptr, "parse_result");

    // Check for parse error.  TODO: handle overflow
    Value* parse_failed = builder.CreateICmpEQ(parse_result_val, failed_value, "failed");
    builder.CreateCondBr(parse_failed, parse_failed_block, parse_success_block);

    // Parse succeeded
    builder.SetInsertPoint(parse_success_block);
    builder.CreateStore(result, slot);
    builder.CreateRet(codegen->true_value());

    // Parse failed, set slot to null and return false
    builder.SetInsertPoint(parse_failed_block);
    builder.CreateCall(set_null_fn, args[0]);
    builder.CreateRet(codegen->false_value());
  }

  // Case where data is \N or len == 0 and it is not a string col
  builder.SetInsertPoint(set_null_block);
  builder.CreateCall(set_null_fn, args[0]);
  builder.CreateRet(codegen->true_value());

  return codegen->FinalizeFunction(fn);
}
