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

#include "runtime/outbound-row-batch.h"
#include "runtime/outbound-row-batch.inline.h"

#include "codegen/llvm-codegen.h"
#include "util/compress.h"
#include "util/scope-exit-trigger.h"

namespace impala {

const char* OutboundRowBatch::LLVM_CLASS_NAME = "class.impala::OutboundRowBatch";
const char* OutboundRowBatch::DedupMap::LLVM_CLASS_NAME =
    "class.impala::OutboundRowBatch::DedupMap";

Status OutboundRowBatch::PrepareForSend(int num_tuples_per_row,
    TrackedString* compression_scratch, bool used_append_row) {
  if (used_append_row) {
    DCHECK_GE(tuple_data_.size(), tuple_data_offset_);
    tuple_data_.resize(tuple_data_offset_);
  } else {
    DCHECK_EQ(tuple_data_offset_, 0);
  }
  bool is_compressed = false;
  int64_t uncompressed_size = tuple_data_.size();
  if (uncompressed_size > 0 && compression_scratch != nullptr) {
    RETURN_IF_ERROR(TryCompress(compression_scratch, &is_compressed));
  }
  int num_tuples = tuple_offsets_.size();
  DCHECK_EQ(num_tuples % num_tuples_per_row, 0);
  int num_rows = num_tuples / num_tuples_per_row;
  SetHeader(num_rows, num_tuples_per_row, uncompressed_size, is_compressed);
  return Status::OK();
}

Status OutboundRowBatch::TryCompress(TrackedString* compression_scratch,
    bool* is_compressed) {
  DCHECK(compression_scratch != nullptr);
  Lz4Compressor compressor(nullptr, false);
  RETURN_IF_ERROR(compressor.Init());
  auto compressor_cleanup =
      MakeScopeExitTrigger([&compressor]() { compressor.Close(); });

  *is_compressed = false;
  int64_t uncompressed_size = tuple_data_.size();
  // If the input size is too large for LZ4 to compress, MaxOutputLen() will return 0.
  int64_t compressed_size = compressor.MaxOutputLen(uncompressed_size);
  if (compressed_size == 0) {
      return Status(TErrorCode::LZ4_COMPRESSION_INPUT_TOO_LARGE, uncompressed_size);
  }
  DCHECK_GT(compressed_size, 0);
  if (compression_scratch->size() < compressed_size) {
      compression_scratch->resize(compressed_size);
  }

  uint8_t* input = reinterpret_cast<uint8_t*>(tuple_data_.data());
  uint8_t* compressed_output = const_cast<uint8_t*>(
      reinterpret_cast<const uint8_t*>(compression_scratch->data()));
  RETURN_IF_ERROR(compressor.ProcessBlock(
      true, uncompressed_size, input, &compressed_size, &compressed_output));
  if (LIKELY(compressed_size < uncompressed_size)) {
      compression_scratch->resize(compressed_size);
      tuple_data_.swap(*compression_scratch);
      *is_compressed = true;
      // TODO: could copy to a smaller buffer if compressed data is much smaller to
      //       save memory
  }
  VLOG_ROW << "uncompressed size: " << uncompressed_size << ", compressed size: "
      << compressed_size;
  return Status::OK();
}

void OutboundRowBatch::SetHeader(int num_rows, int num_tuples_per_row,
    int64_t uncompressed_size, bool is_compressed) {
  header_.Clear();
  header_.set_num_rows(num_rows);
  header_.set_num_tuples_per_row(num_tuples_per_row);
  header_.set_uncompressed_size(uncompressed_size);
  header_.set_compression_type(
      is_compressed ? CompressionTypePB::LZ4 : CompressionTypePB::NONE);
}

void OutboundRowBatch::Reset() {
  header_.Clear();
  tuple_offsets_.clear();
  tuple_data_offset_ = 0;
  // Do not clear tuple_data_ to avoid unnecessary delete + allocate.
}

Status OutboundRowBatch::CodegenAppendRowWithDedup(LlvmCodeGen* codegen,
    const RowDescriptor* row_desc, llvm::Function** fn) {
  // For each Tuple in the Row:
  //  Make a copy of AppendTupleWithDedup, with calls to Tuple::TryDeepCopy replaced
  //  with a codegen'd TryDeepCopy generated from the Tuple's Descriptor

  llvm::Type* this_ptr_type = codegen->GetStructPtrType<OutboundRowBatch>();
  llvm::Type* status_ptr_type = codegen->GetStructPtrType<Status>();
  llvm::Type* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  llvm::Type* dedup_map_ptr_type = codegen->GetStructPtrType<DedupMap>();
  llvm::Type* row_descriptor_ptr_type = codegen->GetStructPtrType<RowDescriptor>();

  LlvmCodeGen::FnPrototype prototype(codegen, "AppendRowWithDedup", codegen->void_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("status", status_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("prev_row", tuple_row_ptr_type));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("distinct_tuples", dedup_map_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row_desc", row_descriptor_ptr_type));

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[6];
  *fn = prototype.GeneratePrototype(&builder, args);
  // Mark the first argument explicitly as sret, so the generated function's prototype
  // matches that of OutboundRowBatch::AppendRowWithDedup (returning Status as a struct).
  (*fn)->addAttribute(1, llvm::Attribute::StructRet);
  llvm::Value* status = args[0];
  llvm::Value* this_ptr = args[1];
  llvm::Value* row = args[2];
  llvm::Value* prev_row = args[3];
  llvm::Value* distinct_tuples = args[4];
  llvm::Value* row_desc_arg = args[5];

  llvm::BasicBlock* return_block = llvm::BasicBlock::Create(context, "return", *fn);

  int num_tuples_base = row_desc->num_tuples_no_inline();
  llvm::Constant* num_tuples = codegen->GetI32Constant(num_tuples_base);
  for (int idx = 0; idx < num_tuples_base; ++idx) {
    // Call AppendTupleWithDedup
    // Fetch values first for function call
    llvm::Constant* tuple_idx = codegen->GetI32Constant(idx);

    // This is passed through for collections as they call the interpreted functions,
    // requiring the tuple desc.
    // TODO: once collection type deepcopy is codegen'd, remove this!
    llvm::Value* tuple_desc = codegen->CodegenCallFunction(&builder,
        IRFunction::ROW_DESCRIPTOR_GET_TUPLE_DESC, {row_desc_arg, tuple_idx},
        "tuple_desc");

    TupleDescriptor* desc = row_desc->tuple_descriptors()[idx];
    llvm::Constant* byte_size = codegen->GetI32Constant(desc->byte_size());

    llvm::Function* append_tuple_fn = codegen->GetFunction(
        IRFunction::OUTBOUND_ROW_BATCH_APPEND_TUPLE_WITH_DEDUP, true);

    // Replace calls to TryDeepCopy with codegen'd function based on tuple_desc
    llvm::Function* try_deep_copy_fn = nullptr;
    RETURN_IF_ERROR(Tuple::CodegenTryDeepCopy(codegen, desc, &try_deep_copy_fn));

    int replaced =
        codegen->ReplaceCallSites(append_tuple_fn, try_deep_copy_fn, "TryDeepCopy");
    DCHECK_REPLACE_COUNT(replaced, 2);

    builder.CreateCall(append_tuple_fn, {status, this_ptr, row, prev_row, tuple_idx,
        distinct_tuples, tuple_desc, byte_size, num_tuples});

    // return if status not OK
    llvm::Value* status_ok = codegen->CodegenCallFunction(&builder,
        IRFunction::STATUS_OK, {status}, "status_ok");

    llvm::BasicBlock* continue_block = llvm::BasicBlock::Create(context, "continue", *fn);
    builder.CreateCondBr(status_ok, continue_block, return_block);

    builder.SetInsertPoint(continue_block);
  }

  builder.CreateBr(return_block);

  builder.SetInsertPoint(return_block);
  builder.CreateRetVoid();

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == nullptr) {
    return Status("Codegen'd OutboundRowBatch::AppendRowWithDedup() function"
      " failed verification, see log");
  }

  return Status::OK();
}

}
