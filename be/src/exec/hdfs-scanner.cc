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

#include "exec/hdfs-scanner.h"

#include <sstream>
#include <boost/algorithm/string.hpp>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "exec/text-converter.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exec/text-converter.inline.h"
#include "exprs/expr-context.h"
#include "runtime/collection-value-builder.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "util/bitmap.h"
#include "util/codec.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/sse-util.h"
#include "util/string-parser.h"
#include "util/test-info.h"
#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala;
using namespace llvm;
using namespace strings;

const char* FieldLocation::LLVM_CLASS_NAME = "struct.impala::FieldLocation";
const char* HdfsScanner::LLVM_CLASS_NAME = "class.impala::HdfsScanner";

HdfsScanner::HdfsScanner(HdfsScanNode* scan_node, RuntimeState* state,
    bool add_batches_to_queue)
    : scan_node_(scan_node),
      state_(state),
      add_batches_to_queue_(add_batches_to_queue),
      context_(NULL),
      stream_(NULL),
      scanner_conjunct_ctxs_(NULL),
      template_tuple_(NULL),
      tuple_byte_size_(scan_node->tuple_desc()->byte_size()),
      tuple_(NULL),
      batch_(NULL),
      tuple_mem_(NULL),
      num_null_bytes_(scan_node->tuple_desc()->num_null_bytes()),
      parse_status_(Status::OK()),
      decompression_type_(THdfsCompression::NONE),
      data_buffer_pool_(new MemPool(scan_node->mem_tracker())),
      decompress_timer_(NULL),
      write_tuples_fn_(NULL) {
}

HdfsScanner::HdfsScanner()
    : scan_node_(NULL),
      state_(NULL),
      add_batches_to_queue_(true),
      context_(NULL),
      stream_(NULL),
      scanner_conjunct_ctxs_(NULL),
      template_tuple_(NULL),
      tuple_byte_size_(-1),
      tuple_(NULL),
      batch_(NULL),
      tuple_mem_(NULL),
      num_null_bytes_(-1),
      parse_status_(Status::OK()),
      decompression_type_(THdfsCompression::NONE),
      data_buffer_pool_(NULL),
      decompress_timer_(NULL),
      write_tuples_fn_(NULL) {
  DCHECK(TestInfo::is_test());
}

HdfsScanner::~HdfsScanner() {
}

Status HdfsScanner::Open(ScannerContext* context) {
  context_ = context;
  stream_ = context->GetStream();

  // Clone the scan node's conjuncts map. The cloned contexts must be closed by the
  // caller.
  HdfsScanNode::ConjunctsMap::const_iterator iter = scan_node_->conjuncts_map().begin();
  for (; iter != scan_node_->conjuncts_map().end(); ++iter) {
    RETURN_IF_ERROR(Expr::CloneIfNotExists(iter->second,
        scan_node_->runtime_state(), &scanner_conjuncts_map_[iter->first]));
  }
  DCHECK(scanner_conjuncts_map_.find(scan_node_->tuple_desc()->id()) !=
         scanner_conjuncts_map_.end());
  scanner_conjunct_ctxs_ = &scanner_conjuncts_map_[scan_node_->tuple_desc()->id()];

  // Initialize the template_tuple_.
  template_tuple_ = scan_node_->InitTemplateTuple(
      state_, context_->partition_descriptor()->partition_key_value_ctxs());
  template_tuple_map_[scan_node_->tuple_desc()] = template_tuple_;

  decompress_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "DecompressionTime");
  return Status::OK();
}

void HdfsScanner::Close(RowBatch* row_batch) {
  if (decompressor_.get() != NULL) decompressor_->Close();
  HdfsScanNode::ConjunctsMap::const_iterator iter = scanner_conjuncts_map_.begin();
  for (; iter != scanner_conjuncts_map_.end(); ++iter) {
    Expr::Close(iter->second, state_);
  }
  obj_pool_.Clear();
  stream_ = NULL;
  context_->ClearStreams();
}

Status HdfsScanner::InitializeWriteTuplesFn(HdfsPartitionDescriptor* partition,
    THdfsFileFormat::type type, const string& scanner_name) {
  if (!scan_node_->tuple_desc()->string_slots().empty()
      && partition->escape_char() != '\0') {
    // Codegen currently doesn't emit call to MemPool::TryAllocate() so skip codegen if
    // there are strings slots and we need to compact (i.e. copy) the data.
    scan_node_->IncNumScannersCodegenDisabled();
    return Status::OK();
  }

  write_tuples_fn_ = reinterpret_cast<WriteTuplesFn>(scan_node_->GetCodegenFn(type));
  if (write_tuples_fn_ == NULL) {
    scan_node_->IncNumScannersCodegenDisabled();
    return Status::OK();
  }
  VLOG(2) << scanner_name << "(node_id=" << scan_node_->id()
          << ") using llvm codegend functions.";
  scan_node_->IncNumScannersCodegenEnabled();
  return Status::OK();
}

Status HdfsScanner::StartNewRowBatch() {
  DCHECK(add_batches_to_queue_);
  batch_ = new RowBatch(scan_node_->row_desc(), state_->batch_size(),
      scan_node_->mem_tracker());
  int64_t tuple_buffer_size;
  RETURN_IF_ERROR(
      batch_->ResizeAndAllocateTupleBuffer(state_, &tuple_buffer_size, &tuple_mem_));
  return Status::OK();
}

int HdfsScanner::GetMemory(MemPool** pool, Tuple** tuple_mem, TupleRow** tuple_row_mem) {
  DCHECK(add_batches_to_queue_);
  DCHECK(batch_ != NULL);
  DCHECK_GT(batch_->capacity(), batch_->num_rows());
  *pool = batch_->tuple_data_pool();
  *tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
  *tuple_row_mem = batch_->GetRow(batch_->AddRow());
  return batch_->capacity() - batch_->num_rows();
}

Status HdfsScanner::GetCollectionMemory(CollectionValueBuilder* builder, MemPool** pool,
    Tuple** tuple_mem, TupleRow** tuple_row_mem, int64_t* num_rows) {
  int num_tuples;
  *pool = builder->pool();
  RETURN_IF_ERROR(builder->GetFreeMemory(tuple_mem, &num_tuples));
  // Treat tuple as a single-tuple row
  *tuple_row_mem = reinterpret_cast<TupleRow*>(tuple_mem);
  *num_rows = num_tuples;
  return Status::OK();
}

// TODO(skye): have this check scan_node_->ReachedLimit() and get rid of manual check?
Status HdfsScanner::CommitRows(int num_rows) {
  DCHECK(add_batches_to_queue_);
  DCHECK(batch_ != NULL);
  DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
  batch_->CommitRows(num_rows);
  tuple_mem_ += static_cast<int64_t>(scan_node_->tuple_desc()->byte_size()) * num_rows;

  // We need to pass the row batch to the scan node if there is too much memory attached,
  // which can happen if the query is very selective. We need to release memory even
  // if no rows passed predicates.
  if (batch_->AtCapacity() || context_->num_completed_io_buffers() > 0) {
    context_->ReleaseCompletedResources(batch_, /* done */ false);
    scan_node_->AddMaterializedRowBatch(batch_);
    RETURN_IF_ERROR(StartNewRowBatch());
  }
  if (context_->cancelled()) return Status::CANCELLED;
  // Check for UDF errors.
  RETURN_IF_ERROR(state_->GetQueryStatus());
  // Free local expr allocations for this thread
  HdfsScanNode::ConjunctsMap::const_iterator iter = scanner_conjuncts_map_.begin();
  for (; iter != scanner_conjuncts_map_.end(); ++iter) {
    ExprContext::FreeLocalAllocations(iter->second);
  }
  return Status::OK();
}

// In this code path, no slots were materialized from the input files.  The only
// slots are from partition keys.  This lets us simplify writing out the batches.
//   1. template_tuple_ is the complete tuple.
//   2. Eval conjuncts against the tuple.
//   3. If it passes, stamp out 'num_tuples' copies of it into the row_batch.
int HdfsScanner::WriteEmptyTuples(RowBatch* row_batch, int num_tuples) {
  DCHECK_GT(num_tuples, 0);

  if (template_tuple_ == NULL) {
    // No slots from partitions keys or slots.  This is count(*).  Just add the
    // number of rows to the batch.
    row_batch->AddRows(num_tuples);
    row_batch->CommitRows(num_tuples);
  } else {
    // Make a row and evaluate the row
    int row_idx = row_batch->AddRow();

    TupleRow* current_row = row_batch->GetRow(row_idx);
    current_row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
    if (!EvalConjuncts(current_row)) return 0;
    // Add first tuple
    row_batch->CommitLastRow();
    --num_tuples;

    DCHECK_LE(num_tuples, row_batch->capacity() - row_batch->num_rows());

    for (int n = 0; n < num_tuples; ++n) {
      DCHECK(!row_batch->AtCapacity());
      TupleRow* current_row = row_batch->GetRow(row_batch->AddRow());
      current_row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
      row_batch->CommitLastRow();
    }
  }
  return num_tuples;
}

// In this code path, no slots were materialized from the input files.  The only
// slots are from partition keys.  This lets us simplify writing out the batches.
//   1. template_tuple_ is the complete tuple.
//   2. Eval conjuncts against the tuple.
//   3. If it passes, stamp out 'num_tuples' copies of it into the row_batch.
int HdfsScanner::WriteEmptyTuples(ScannerContext* context,
    TupleRow* row, int num_tuples) {
  DCHECK_GE(num_tuples, 0);
  if (num_tuples == 0) return 0;

  if (template_tuple_ == NULL) {
    // Must be conjuncts on constant exprs.
    if (!EvalConjuncts(row)) return 0;
    return num_tuples;
  } else {
    row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
    if (!EvalConjuncts(row)) return 0;
    row = next_row(row);

    for (int n = 1; n < num_tuples; ++n) {
      row->SetTuple(scan_node_->tuple_idx(), template_tuple_);
      row = next_row(row);
    }
  }
  return num_tuples;
}

bool HdfsScanner::WriteCompleteTuple(MemPool* pool, FieldLocation* fields,
    Tuple* tuple, TupleRow* tuple_row, Tuple* template_tuple,
    uint8_t* error_fields, uint8_t* error_in_row) {
  *error_in_row = false;
  // Initialize tuple before materializing slots
  InitTuple(template_tuple, tuple);

  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    int need_escape = false;
    int len = fields[i].len;
    if (UNLIKELY(len < 0)) {
      len = -len;
      need_escape = true;
    }

    SlotDescriptor* desc = scan_node_->materialized_slots()[i];
    bool error = !text_converter_->WriteSlot(desc, tuple,
        fields[i].start, len, false, need_escape, pool);
    error_fields[i] = error;
    *error_in_row |= error;
  }

  tuple_row->SetTuple(scan_node_->tuple_idx(), tuple);
  return EvalConjuncts(tuple_row);
}

// Codegen for WriteTuple(above).  The signature matches WriteTuple (except for the
// this* first argument).  For writing out and evaluating a single string slot:
// define i1 @WriteCompleteTuple(%"class.impala::HdfsScanner"* %this,
//                               %"class.impala::MemPool"* %pool,
//                               %"struct.impala::FieldLocation"* %fields,
//                               %"class.impala::Tuple"* %tuple,
//                               %"class.impala::TupleRow"* %tuple_row,
//                               %"class.impala::Tuple"* %template,
//                               i8* %error_fields, i8* %error_in_row) #20 {
// entry:
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple
//                to { i8, %"struct.impala::StringValue" }*
//   %tuple_ptr1 = bitcast %"class.impala::Tuple"* %template
//                 to { i8, %"struct.impala::StringValue" }*
//   %null_byte = getelementptr inbounds
//                { i8, %"struct.impala::StringValue" }* %tuple_ptr, i32 0, i32 0
//   store i8 0, i8* %null_byte
//   %0 = bitcast %"class.impala::TupleRow"* %tuple_row
//        to { i8, %"struct.impala::StringValue" }**
//   %1 = getelementptr { i8, %"struct.impala::StringValue" }** %0, i32 0
//   store { i8, %"struct.impala::StringValue" }* %tuple_ptr,
//         { i8, %"struct.impala::StringValue" }** %1
//   br label %parse
//
// parse:                                            ; preds = %entry
//   %data_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 0
//   %len_ptr = getelementptr %"struct.impala::FieldLocation"* %fields, i32 0, i32 1
//   %slot_error_ptr = getelementptr i8* %error_fields, i32 0
//   %data = load i8** %data_ptr
//   %len = load i32* %len_ptr
//   %2 = call i1 @WriteSlot({ i8, %"struct.impala::StringValue" }* %tuple_ptr,
//                           i8* %data, i32 %len)
//   %slot_parse_error = xor i1 %2, true
//   %error_in_row2 = or i1 false, %slot_parse_error
//   %3 = zext i1 %slot_parse_error to i8
//   store i8 %3, i8* %slot_error_ptr
//   %4 = call %"class.impala::ExprContext"* @GetConjunctCtx(
//       %"class.impala::HdfsScanner"* %this, i32 0)
//   %conjunct_eval = call i16 @Eq_StringVal_StringValWrapper1(
//       %"class.impala::ExprContext"* %4, %"class.impala::TupleRow"* %tuple_row)
//   %5 = ashr i16 %conjunct_eval, 8
//   %6 = trunc i16 %5 to i8
//   %val = trunc i8 %6 to i1
//   br i1 %val, label %parse3, label %eval_fail
//
// parse3:                                           ; preds = %parse
//   %7 = zext i1 %error_in_row2 to i8
//   store i8 %7, i8* %error_in_row
//   ret i1 true
//
// eval_fail:                                        ; preds = %parse
//   ret i1 false
// }
Function* HdfsScanner::CodegenWriteCompleteTuple(
    HdfsScanNode* node, LlvmCodeGen* codegen, const vector<ExprContext*>& conjunct_ctxs) {
  SCOPED_TIMER(codegen->codegen_timer());
  RuntimeState* state = node->runtime_state();

  // TODO: Timestamp is not yet supported
  for (int i = 0; i < node->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = node->materialized_slots()[i];
    if (slot_desc->type().type == TYPE_TIMESTAMP) return NULL;
    if (slot_desc->type().type == TYPE_DECIMAL) return NULL;
  }

  // Cast away const-ness.  The codegen only sets the cached typed llvm struct.
  TupleDescriptor* tuple_desc = const_cast<TupleDescriptor*>(node->tuple_desc());
  vector<Function*> slot_fns;
  for (int i = 0; i < node->materialized_slots().size(); ++i) {
    SlotDescriptor* slot_desc = node->materialized_slots()[i];
    Function* fn = TextConverter::CodegenWriteSlot(codegen, tuple_desc, slot_desc,
        node->hdfs_table()->null_column_value().data(),
        node->hdfs_table()->null_column_value().size(), true, state->strict_mode());
    if (fn == NULL) return NULL;
    slot_fns.push_back(fn);
  }

  // Compute order to materialize slots.  BE assumes that conjuncts should
  // be evaluated in the order specified (optimization is already done by FE)
  vector<int> materialize_order;
  node->ComputeSlotMaterializationOrder(&materialize_order);

  // Get types to construct matching function signature to WriteCompleteTuple
  PointerType* uint8_ptr_type = PointerType::get(codegen->GetType(TYPE_TINYINT), 0);

  StructType* field_loc_type = reinterpret_cast<StructType*>(
      codegen->GetType(FieldLocation::LLVM_CLASS_NAME));
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  Type* tuple_opaque_type = codegen->GetType(Tuple::LLVM_CLASS_NAME);
  Type* mem_pool_type = codegen->GetType(MemPool::LLVM_CLASS_NAME);
  Type* hdfs_scanner_type = codegen->GetType(HdfsScanner::LLVM_CLASS_NAME);

  DCHECK(tuple_opaque_type != NULL);
  DCHECK(tuple_row_type != NULL);
  DCHECK(field_loc_type != NULL);
  DCHECK(hdfs_scanner_type != NULL);

  PointerType* field_loc_ptr_type = PointerType::get(field_loc_type, 0);
  PointerType* tuple_opaque_ptr_type = PointerType::get(tuple_opaque_type, 0);
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
  PointerType* mem_pool_ptr_type = PointerType::get(mem_pool_type, 0);
  PointerType* hdfs_scanner_ptr_type = PointerType::get(hdfs_scanner_type, 0);

  // Generate the typed llvm struct for the output tuple
  StructType* tuple_type = tuple_desc->GetLlvmStruct(codegen);
  if (tuple_type == NULL) return NULL;
  PointerType* tuple_ptr_type = PointerType::get(tuple_type, 0);

  // Initialize the function prototype.  This needs to match
  // HdfsScanner::WriteCompleteTuple's signature identically.
  LlvmCodeGen::FnPrototype prototype(
      codegen, "WriteCompleteTuple", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", hdfs_scanner_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("pool", mem_pool_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("fields", field_loc_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_opaque_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("template", tuple_opaque_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_fields", uint8_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_in_row", uint8_ptr_type));

  LLVMContext& context = codegen->context();
  LlvmCodeGen::LlvmBuilder builder(context);
  Value* args[8];
  Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  BasicBlock* parse_block = BasicBlock::Create(context, "parse", fn);
  BasicBlock* eval_fail_block = BasicBlock::Create(context, "eval_fail", fn);

  // Extract the input args
  Value* this_arg = args[0];
  Value* fields_arg = args[2];
  Value* tuple_arg = builder.CreateBitCast(args[3], tuple_ptr_type, "tuple_ptr");
  Value* tuple_row_arg = args[4];
  Value* template_arg = builder.CreateBitCast(args[5], tuple_ptr_type, "tuple_ptr");
  Value* errors_arg = args[6];
  Value* error_in_row_arg = args[7];

  // Codegen for function body
  Value* error_in_row = codegen->false_value();
  // Initialize tuple
  if (node->num_materialized_partition_keys() == 0) {
    // No partition key slots, just zero the NULL bytes.
    for (int i = 0; i < tuple_desc->num_null_bytes(); ++i) {
      Value* null_byte = builder.CreateStructGEP(NULL, tuple_arg, i, "null_byte");
      builder.CreateStore(codegen->GetIntConstant(TYPE_TINYINT, 0), null_byte);
    }
  } else {
    // Copy template tuple.
    // TODO: only copy what's necessary from the template tuple.
    codegen->CodegenMemcpy(&builder, tuple_arg, template_arg, tuple_desc->byte_size());
  }

  // Put tuple in tuple_row
  Value* tuple_row_typed =
      builder.CreateBitCast(tuple_row_arg, PointerType::get(tuple_ptr_type, 0));
  Value* tuple_row_idxs[] = { codegen->GetIntConstant(TYPE_INT, node->tuple_idx()) };
  Value* tuple_in_row_addr = builder.CreateGEP(tuple_row_typed, tuple_row_idxs);
  builder.CreateStore(tuple_arg, tuple_in_row_addr);
  builder.CreateBr(parse_block);

  // Loop through all the conjuncts in order and materialize slots as necessary to
  // evaluate the conjuncts (e.g. conjunct_ctxs[0] will have the slots it references
  // first).
  // materialized_order[slot_idx] represents the first conjunct which needs that slot.
  // Slots are only materialized if its order matches the current conjunct being
  // processed.  This guarantees that each slot is materialized once when it is first
  // needed and that at the end of the materialize loop, the conjunct has everything
  // it needs (either from this iteration or previous iterations).
  builder.SetInsertPoint(parse_block);
  for (int conjunct_idx = 0; conjunct_idx <= conjunct_ctxs.size(); ++conjunct_idx) {
    for (int slot_idx = 0; slot_idx < materialize_order.size(); ++slot_idx) {
      // If they don't match, it means either the slot has already been
      // materialized for a previous conjunct or will be materialized later for
      // another conjunct.  Either case, the slot does not need to be materialized
      // yet.
      if (materialize_order[slot_idx] != conjunct_idx) continue;

      // Materialize slots[slot_idx] to evaluate conjunct_ctxs[conjunct_idx]
      // All slots[i] with materialized_order[i] < conjunct_idx have already been
      // materialized by prior iterations through the outer loop

      // Extract ptr/len from fields
      Value* data_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
        codegen->GetIntConstant(TYPE_INT, 0),
      };
      Value* len_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
        codegen->GetIntConstant(TYPE_INT, 1),
      };
      Value* error_idxs[] = {
        codegen->GetIntConstant(TYPE_INT, slot_idx),
      };
      Value* data_ptr = builder.CreateGEP(fields_arg, data_idxs, "data_ptr");
      Value* len_ptr = builder.CreateGEP(fields_arg, len_idxs, "len_ptr");
      Value* error_ptr = builder.CreateGEP(errors_arg, error_idxs, "slot_error_ptr");
      Value* data = builder.CreateLoad(data_ptr, "data");
      Value* len = builder.CreateLoad(len_ptr, "len");

      // Call slot parse function
      Function* slot_fn = slot_fns[slot_idx];
      Value* slot_parsed = builder.CreateCall(slot_fn,
          ArrayRef<Value*>({tuple_arg, data, len}));
      Value* slot_error = builder.CreateNot(slot_parsed, "slot_parse_error");
      error_in_row = builder.CreateOr(error_in_row, slot_error, "error_in_row");
      slot_error = builder.CreateZExt(slot_error, codegen->GetType(TYPE_TINYINT));
      builder.CreateStore(slot_error, error_ptr);
    }

    if (conjunct_idx == conjunct_ctxs.size()) {
      // In this branch, we've just materialized slots not referenced by any conjunct.
      // This slots are the last to get materialized.  If we are in this branch, the
      // tuple passed all conjuncts and should be added to the row batch.
      Value* error_ret = builder.CreateZExt(error_in_row, codegen->GetType(TYPE_TINYINT));
      builder.CreateStore(error_ret, error_in_row_arg);
      builder.CreateRet(codegen->true_value());
    } else {
      // All slots for conjunct_ctxs[conjunct_idx] are materialized, evaluate the partial
      // tuple against that conjunct and start a new parse_block for the next conjunct
      parse_block = BasicBlock::Create(context, "parse", fn, eval_fail_block);
      Function* conjunct_fn;
      Status status =
          conjunct_ctxs[conjunct_idx]->root()->GetCodegendComputeFn(state, &conjunct_fn);
      if (!status.ok()) {
        stringstream ss;
        ss << "Failed to codegen conjunct: " << status.GetDetail();
        state->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
        fn->eraseFromParent();
        return NULL;
      }

      Function* get_ctx_fn =
          codegen->GetFunction(IRFunction::HDFS_SCANNER_GET_CONJUNCT_CTX, false);
      Value* ctx = builder.CreateCall(get_ctx_fn,
          ArrayRef<Value*>({this_arg, codegen->GetIntConstant(TYPE_INT, conjunct_idx)}));

      Value* conjunct_args[] = {ctx, tuple_row_arg};
      CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
          codegen, &builder, TYPE_BOOLEAN, conjunct_fn, conjunct_args, "conjunct_eval");
      builder.CreateCondBr(result.GetVal(), parse_block, eval_fail_block);
      builder.SetInsertPoint(parse_block);
    }
  }

  // Block if eval failed.
  builder.SetInsertPoint(eval_fail_block);
  builder.CreateRet(codegen->false_value());

  codegen->FinalizeFunction(fn);
  return codegen->FinalizeFunction(fn);
}

Function* HdfsScanner::CodegenWriteAlignedTuples(HdfsScanNode* node,
    LlvmCodeGen* codegen, Function* write_complete_tuple_fn) {
  SCOPED_TIMER(codegen->codegen_timer());
  DCHECK(write_complete_tuple_fn != NULL);

  Function* write_tuples_fn =
      codegen->GetFunction(IRFunction::HDFS_SCANNER_WRITE_ALIGNED_TUPLES, true);
  DCHECK(write_tuples_fn != NULL);

  int replaced = codegen->ReplaceCallSites(write_tuples_fn, write_complete_tuple_fn,
      "WriteCompleteTuple");
  DCHECK_EQ(replaced, 1);

  return codegen->FinalizeFunction(write_tuples_fn);
}

Status HdfsScanner::UpdateDecompressor(const THdfsCompression::type& compression) {
  // Check whether the file in the stream has different compression from the last one.
  if (compression != decompression_type_) {
    if (decompression_type_ != THdfsCompression::NONE) {
      // Close the previous decompressor before creating a new one.
      DCHECK(decompressor_.get() != NULL);
      decompressor_->Close();
      decompressor_.reset(NULL);
    }
    // The LZO-compression scanner is implemented in a dynamically linked library and it
    // is not created at Codec::CreateDecompressor().
    if (compression != THdfsCompression::NONE && compression != THdfsCompression::LZO) {
      RETURN_IF_ERROR(Codec::CreateDecompressor(data_buffer_pool_.get(),
        scan_node_->tuple_desc()->string_slots().empty(), compression, &decompressor_));
    }
    decompression_type_ = compression;
  }
  return Status::OK();
}

Status HdfsScanner::UpdateDecompressor(const string& codec) {
  map<const string, const THdfsCompression::type>::const_iterator
    type = Codec::CODEC_MAP.find(codec);

  if (type == Codec::CODEC_MAP.end()) {
    stringstream ss;
    ss << Codec::UNKNOWN_CODEC_ERROR << codec;
    return Status(ss.str());
  }
  RETURN_IF_ERROR(UpdateDecompressor(type->second));
  return Status::OK();
}

bool HdfsScanner::ReportTupleParseError(FieldLocation* fields, uint8_t* errors,
    int row_idx) {
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    if (errors[i]) {
      const SlotDescriptor* desc = scan_node_->materialized_slots()[i];
      ReportColumnParseError(desc, fields[i].start, fields[i].len);
      errors[i] = false;
    }
  }

  // Call into subclass to log a more accurate error message.
  if (state_->LogHasSpace()) {
    stringstream ss;
    ss << "file: " << stream_->filename() << endl << "record: ";
    LogRowParseError(row_idx, &ss);
    state_->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()), 2);
  }

  if (state_->abort_on_error()) DCHECK(!parse_status_.ok());
  return parse_status_.ok();
}

void HdfsScanner::LogRowParseError(int row_idx, stringstream* ss) {
  // This is only called for text and seq files which should override this function.
  DCHECK(false);
}

void HdfsScanner::ReportColumnParseError(const SlotDescriptor* desc,
    const char* data, int len) {
  // len < 0 is used to indicate the data contains escape characters.  We don't care
  // about that here and can just output the raw string.
  if (len < 0) len = -len;

  if (state_->LogHasSpace() || state_->abort_on_error()) {
    stringstream ss;
    ss << "Error converting column: "
       << desc->col_pos() - scan_node_->num_partition_keys()
       << " TO " << desc->type()
       << " (Data is: " << string(data,len) << ")";

    // When skipping multiple header lines we only try to skip them in the first scan
    // range. For subsequent scan ranges, it's impossible to determine how many lines
    // precede it and whether any header lines should be skipped. If the header does not
    // fit into the first scan range and spills into subsequent scan ranges, then we will
    // try to parse header data here and fail. The scanner of the first scan range will
    // fail the query if it cannot fully skip the header. However, if abort_on_error is
    // set, then a race happens between the first scanner to detect the condition and any
    // other scanner that tries to parse an invalid value. Therefore a possible mitigation
    // is to increase the max_scan_range_length so the header is fully contained in the
    // first scan range.
    if (scan_node_->skip_header_line_count() > 1) {
      ss << "\n" << "Table has skip.header.line.count set to a value > 1. If the data "
         << "that could not be parsed looks like it's part of the file's header, then "
         << "try increasing max_scan_range_length to a value larger than the size of the "
         << "file's header.";
    }
    if (state_->LogHasSpace()) {
      state_->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()), 2);
    }

    if (state_->abort_on_error() && parse_status_.ok()) parse_status_ = Status(ss.str());
  }
}
