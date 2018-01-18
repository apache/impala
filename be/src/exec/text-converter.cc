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
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using namespace impala;

TextConverter::TextConverter(char escape_char, const string& null_col_val,
    bool check_null, bool strict_mode)
  : escape_char_(escape_char),
    null_col_val_(null_col_val),
    check_null_(check_null),
    strict_mode_(strict_mode) {
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
// define i1 @WriteSlot(<{ i32, i8 }>* %tuple_arg, i8* %data, i32 %len) #38 {
// entry:
//   %parse_result = alloca i32
//   %0 = call i1 @IrIsNullString(i8* %data, i32 %len)
//   br i1 %0, label %set_null, label %check_zero
//
// set_null:                                         ; preds = %check_zero, %entry
//   %1 = bitcast <{ i32, i8 }>* %tuple_arg to i8*
//   %null_byte_ptr2 = getelementptr inbounds i8, i8* %1, i32 4
//   %null_byte3 = load i8, i8* %null_byte_ptr2
//   %null_bit_set4 = or i8 %null_byte3, 1
//   store i8 %null_bit_set4, i8* %null_byte_ptr2
//   ret i1 true
//
// parse_slot:                                       ; preds = %check_zero
//   %slot = getelementptr inbounds <{ i32, i8 }>, <{ i32, i8 }>* %tuple_arg, i32 0, i32 0
//   %2 = call i32 @IrStringToInt32(i8* %data, i32 %len, i32* %parse_result)
//   %parse_result1 = load i32, i32* %parse_result
//   %failed = icmp eq i32 %parse_result1, 1
//   br i1 %failed, label %parse_fail, label %parse_success
//
// check_zero:                                       ; preds = %entry
//   %3 = icmp eq i32 %len, 0
//   br i1 %3, label %set_null, label %parse_slot
//
// parse_success:                                    ; preds = %parse_slot
//   store i32 %2, i32* %slot
//   ret i1 true
//
// parse_fail:                                       ; preds = %parse_slot
//   %4 = bitcast <{ i32, i8 }>* %tuple_arg to i8*
//   %null_byte_ptr = getelementptr inbounds i8, i8* %4, i32 4
//   %null_byte = load i8, i8* %null_byte_ptr
//   %null_bit_set = or i8 %null_byte, 1
//   store i8 %null_bit_set, i8* %null_byte_ptr
//   ret i1 false
//}
// TODO: convert this function to use cross-compilation + constant substitution in whole
// or part. It is currently too complex and doesn't implement the full functionality.
Status TextConverter::CodegenWriteSlot(LlvmCodeGen* codegen,
    TupleDescriptor* tuple_desc, SlotDescriptor* slot_desc, llvm::Function** fn,
    const char* null_col_val, int len, bool check_null, bool strict_mode) {
  DCHECK(fn != nullptr);
  if (slot_desc->type().type == TYPE_CHAR) {
    return Status("TextConverter::CodegenWriteSlot(): Char isn't supported for"
        " CodegenWriteSlot");
  }
  SCOPED_TIMER(codegen->codegen_timer());

  // Codegen is_null_string
  bool is_default_null = (len == 2 && null_col_val[0] == '\\' && null_col_val[1] == 'N');
  llvm::Function* is_null_string_fn;
  if (is_default_null) {
    is_null_string_fn = codegen->GetFunction(IRFunction::IS_NULL_STRING, false);
  } else {
    is_null_string_fn = codegen->GetFunction(IRFunction::GENERIC_IS_NULL_STRING, false);
  }

  DCHECK(is_null_string_fn != NULL);

  llvm::StructType* tuple_type = tuple_desc->GetLlvmStruct(codegen);
  if (tuple_type == NULL) {
    return Status("TextConverter::CodegenWriteSlot(): Failed to generate "
        "tuple type");
  }
  llvm::PointerType* tuple_ptr_type = tuple_type->getPointerTo();

  LlvmCodeGen::FnPrototype prototype(
      codegen, "WriteSlot", codegen->bool_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_arg", tuple_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("data", codegen->ptr_type()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("len", codegen->i32_type()));

  LlvmBuilder builder(codegen->context());
  llvm::Value* args[3];
  *fn = prototype.GeneratePrototype(&builder, &args[0]);

  llvm::BasicBlock *set_null_block, *parse_slot_block, *check_zero_block = NULL;
  codegen->CreateIfElseBlocks(*fn, "set_null", "parse_slot",
      &set_null_block, &parse_slot_block);

  if (!slot_desc->type().IsVarLenStringType()) {
    check_zero_block = llvm::BasicBlock::Create(codegen->context(), "check_zero", *fn);
  }

  // Check if the data matches the configured NULL string.
  llvm::Value* is_null;
  if (check_null) {
    if (is_default_null) {
      is_null = builder.CreateCall(
          is_null_string_fn, llvm::ArrayRef<llvm::Value*>({args[1], args[2]}));
    } else {
      is_null = builder.CreateCall(is_null_string_fn,
          llvm::ArrayRef<llvm::Value*>(
              {args[1], args[2], codegen->CastPtrToLlvmPtr(codegen->ptr_type(),
                                     const_cast<char*>(null_col_val)),
                  codegen->GetI32Constant(len)}));
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
    // If len == 0 and it is not a string col, set slot to NULL
    llvm::Value* null_len =
        builder.CreateICmpEQ(args[2], codegen->GetI32Constant(0));
    builder.CreateCondBr(null_len, set_null_block, parse_slot_block);
  }

  // Codegen parse slot block
  builder.SetInsertPoint(parse_slot_block);
  llvm::Value* slot =
      builder.CreateStructGEP(NULL, args[0], slot_desc->llvm_field_idx(), "slot");

  if (slot_desc->type().IsVarLenStringType()) {
    llvm::Value* ptr = builder.CreateStructGEP(NULL, slot, 0, "string_ptr");
    llvm::Value* len = builder.CreateStructGEP(NULL, slot, 1, "string_len");

    builder.CreateStore(args[1], ptr);
    // TODO codegen memory allocation for CHAR
    DCHECK(slot_desc->type().type != TYPE_CHAR);
    if (slot_desc->type().type == TYPE_VARCHAR) {
      // determine if we need to truncate the string
      llvm::Value* maxlen = codegen->GetI32Constant(slot_desc->type().len);
      llvm::Value* len_lt_maxlen =
          builder.CreateICmpSLT(args[2], maxlen, "len_lt_maxlen");
      llvm::Value* minlen =
          builder.CreateSelect(len_lt_maxlen, args[2], maxlen, "select_min_len");
      builder.CreateStore(minlen, len);
    } else {
      builder.CreateStore(args[2], len);
    }
    builder.CreateRet(codegen->true_value());
  } else {
    IRFunction::Type parse_fn_enum;
    llvm::Function* parse_fn = NULL;
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
      case TYPE_TIMESTAMP:
        parse_fn_enum = IRFunction::STRING_TO_TIMESTAMP;
        break;
      case TYPE_DECIMAL:
        switch (slot_desc->slot_size()) {
          case 4:
            parse_fn_enum = IRFunction::STRING_TO_DECIMAL4;
            break;
          case 8:
            parse_fn_enum = IRFunction::STRING_TO_DECIMAL8;
            break;
          case 16:
            parse_fn_enum = IRFunction::STRING_TO_DECIMAL16;
            break;
          default:
            DCHECK(false);
            return Status("TextConverter::CodegenWriteSlot(): "
                "Decimal slots can't be this size.");
        }
        break;
      default:
        DCHECK(false);
        return Status("TextConverter::CodegenWriteSlot(): Codegen'd parser NYI for the"
            "slot_desc type");
    }
    parse_fn = codegen->GetFunction(parse_fn_enum, false);
    DCHECK(parse_fn != NULL);

    // Set up trying to parse the string to the slot type
    llvm::BasicBlock *parse_success_block, *parse_failed_block;
    codegen->CreateIfElseBlocks(*fn, "parse_success", "parse_fail",
        &parse_success_block, &parse_failed_block);
    LlvmCodeGen::NamedVariable parse_result("parse_result", codegen->i32_type());
    llvm::Value* parse_result_ptr = codegen->CreateEntryBlockAlloca(*fn, parse_result);

    llvm::CallInst* parse_return;
    // Call Impala's StringTo* function
    // Function implementations in exec/hdfs-scanner-ir.cc
    if (slot_desc->type().type == TYPE_DECIMAL) {
      // Special case for decimal since it has additional precision/scale parameters
      parse_return = builder.CreateCall(parse_fn, {args[1], args[2],
          codegen->GetI32Constant(slot_desc->type().precision),
          codegen->GetI32Constant(slot_desc->type().scale), parse_result_ptr});
    } else if (slot_desc->type().type == TYPE_TIMESTAMP) {
      // If the return value is large (more than 16 bytes in our toolchain) the first
      // parameter would be a pointer to value parsed and the return value of callee
      // should be ignored
      parse_return =
          builder.CreateCall(parse_fn, {slot, args[1], args[2], parse_result_ptr});
    } else {
      parse_return = builder.CreateCall(parse_fn, {args[1], args[2], parse_result_ptr});
    }
    llvm::Value* parse_result_val = builder.CreateLoad(parse_result_ptr, "parse_result");
    llvm::Value* failed_value =
        codegen->GetI32Constant(StringParser::PARSE_FAILURE);

    // Check for parse error.
    llvm::Value* parse_failed =
        builder.CreateICmpEQ(parse_result_val, failed_value, "failed");
    if (strict_mode) {
      // In strict_mode, also check if parse_result is PARSE_OVERFLOW.
      llvm::Value* overflow_value =
          codegen->GetI32Constant(StringParser::PARSE_OVERFLOW);
      llvm::Value* parse_overflow =
          builder.CreateICmpEQ(parse_result_val, overflow_value, "overflowed");
      parse_failed = builder.CreateOr(parse_failed, parse_overflow, "failed_or");
    }
    builder.CreateCondBr(parse_failed, parse_failed_block, parse_success_block);

    // Parse succeeded
    builder.SetInsertPoint(parse_success_block);
    // If the parsed value is in parse_return, move it into slot
    if (slot_desc->type().type == TYPE_DECIMAL) {
      // For Decimal values, the return type generated by Clang is struct type rather than
      // integer so casting is necessary
      llvm::Value* cast_slot =
          builder.CreateBitCast(slot, parse_return->getType()->getPointerTo());
      builder.CreateStore(parse_return, cast_slot);
    } else if (slot_desc->type().type != TYPE_TIMESTAMP) {
      builder.CreateStore(parse_return, slot);
    }
    builder.CreateRet(codegen->true_value());

    // Parse failed, set slot to null and return false
    builder.SetInsertPoint(parse_failed_block);
    slot_desc->CodegenSetNullIndicator(codegen, &builder, args[0], codegen->true_value());
    builder.CreateRet(codegen->false_value());
  }

  // Case where data is \N or len == 0 and it is not a string col
  builder.SetInsertPoint(set_null_block);
  slot_desc->CodegenSetNullIndicator(codegen, &builder, args[0], codegen->true_value());
  builder.CreateRet(codegen->true_value());

  if (codegen->FinalizeFunction(*fn) == NULL) {
    return Status("TextConverter::CodegenWriteSlot(): codegen'd "
        "WriteSlot function failed verification, see log");
  }
  return Status::OK();
}
