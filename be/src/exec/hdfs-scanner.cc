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

#include "codegen/codegen-anyval.h"
#include "exec/base-sequence-scanner.h"
#include "exec/exec-node.inline.h"
#include "exec/file-metadata-utils.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-scan-node-mt.h"
#include "exec/read-write-util.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.h"
#include "exec/text-converter.inline.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/collection-value-builder.h"
#include "runtime/fragment-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-filter.inline.h"
#include "runtime/tuple-row.h"
#include "util/bitmap.h"
#include "util/codec.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace impala;
using namespace impala::io;
using namespace strings;

DEFINE_double(min_filter_reject_ratio, 0.1, "(Advanced) If the percentage of "
    "rows rejected by a runtime filter drops below this value, the filter is disabled.");

const char* FieldLocation::LLVM_CLASS_NAME = "struct.impala::FieldLocation";
const char* HdfsScanner::LLVM_CLASS_NAME = "class.impala::HdfsScanner";

HdfsScanner::HdfsScanner(HdfsScanNodeBase* scan_node, RuntimeState* state)
    : scan_node_(scan_node),
      state_(state),
      file_metadata_utils_(scan_node),
      expr_perm_pool_(new MemPool(scan_node->expr_mem_tracker())),
      template_tuple_pool_(new MemPool(scan_node->mem_tracker())),
      tuple_byte_size_(scan_node->tuple_desc()->byte_size()),
      data_buffer_pool_(new MemPool(scan_node->mem_tracker())) {
  DCHECK_EQ(1, scan_node->row_desc()->tuple_descriptors().size())
      << "All HDFS scanners assume one tuple per row";
  for (SlotDescriptor* string_slot : scan_node_->tuple_desc()->string_slots()) {
    string_slot_offsets_.push_back(
        {string_slot->null_indicator_offset(), string_slot->tuple_offset()});
  }
}

HdfsScanner::HdfsScanner()
    : scan_node_(nullptr),
      state_(nullptr),
      file_metadata_utils_(nullptr),
      tuple_byte_size_(0) {
  DCHECK(TestInfo::is_test());
}

HdfsScanner::~HdfsScanner() {
}

Status HdfsScanner::ValidateSlotDescriptors() const {
  if (file_format() != THdfsFileFormat::PARQUET &&
      file_format() != THdfsFileFormat::ORC) {
    // Virtual column FILE__POSITION is only supported for PARQUET files.
    for (SlotDescriptor* sd : scan_node_->virtual_column_slots()) {
      if (sd->virtual_column_type() == TVirtualColumnType::FILE_POSITION) {
        return Status(Substitute(
            "Virtual column FILE__POSITION is not supported for $0 files.",
            _THdfsFileFormat_VALUES_TO_NAMES.find(file_format())->second));
      }
    }
  }
  return Status::OK();
}

Status HdfsScanner::Open(ScannerContext* context) {
  context_ = context;
  RETURN_IF_ERROR(ValidateSlotDescriptors());
  file_metadata_utils_.SetFile(state_, scan_node_->GetFileDesc(
      context->partition_descriptor()->id(), context->GetStream()->filename()));
  stream_ = context->GetStream();

  // Clone the scan node's conjuncts map. The cloned evaluators must be closed by the
  // caller.
  for (const auto& entry: scan_node_->conjuncts_map()) {
    RETURN_IF_ERROR(ScalarExprEvaluator::Clone(&obj_pool_, scan_node_->runtime_state(),
        expr_perm_pool_.get(), context_->expr_results_pool(), entry.second,
        &conjunct_evals_map_[entry.first]));
  }
  DCHECK(conjunct_evals_map_.find(scan_node_->tuple_desc()->id()) !=
         conjunct_evals_map_.end());
  conjunct_evals_ = &conjunct_evals_map_[scan_node_->tuple_desc()->id()];

  // Set up the scan node's dictionary filtering conjuncts map.
  if (scan_node_->thrift_dict_filter_conjuncts_map() != nullptr) {
    for (auto& entry : *(scan_node_->thrift_dict_filter_conjuncts_map())) {
      SlotDescriptor* slot_desc = state_->desc_tbl().GetSlotDescriptor(entry.first);
      TupleId tuple_id = (slot_desc->type().IsCollectionType() ?
          slot_desc->children_tuple_descriptor()->id() :
          slot_desc->parent()->id());
      auto conjunct_evals_it = conjunct_evals_map_.find(tuple_id);
      DCHECK(conjunct_evals_it != conjunct_evals_map_.end());
      const vector<ScalarExprEvaluator*>& conjunct_evals = conjunct_evals_it->second;

      // Convert this slot's list of conjunct indices into a list of pointers
      // into conjunct_evals_.
      for (int conjunct_idx : entry.second) {
        DCHECK_LT(conjunct_idx, conjunct_evals.size());
        DCHECK((conjunct_evals)[conjunct_idx] != nullptr);
        dict_filter_map_[entry.first].push_back((conjunct_evals)[conjunct_idx]);
      }
    }
  }

  std::map<const SlotId, const SlotDescriptor*> slot_descs_written;
  template_tuple_ = file_metadata_utils_.CreateTemplateTuple(
      context_->partition_descriptor()->id(), template_tuple_pool_.get(),
      &slot_descs_written);
  template_tuple_map_[scan_node_->tuple_desc()] = template_tuple_;

  decompress_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "DecompressionTime");
  return Status::OK();
}

Status HdfsScanner::ProcessSplit() {
  DCHECK(scan_node_->HasRowBatchQueue());
  HdfsScanNode* scan_node = static_cast<HdfsScanNode*>(scan_node_);
  bool returned_rows = false;
  do {
    // IMPALA-3798, IMPALA-3804: For sequence-based files, the filters are only
    // applied in HdfsScanNode::ProcessSplit()
    bool is_sequence_based = BaseSequenceScanner::FileFormatIsSequenceBased(
        context_->partition_descriptor()->file_format());
    if (!is_sequence_based && FilterContext::CheckForAlwaysFalse(FilterStats::SPLITS_KEY,
        context_->filter_ctxs())) {
      eos_ = true;
      break;
    }
    unique_ptr<RowBatch> batch = std::make_unique<RowBatch>(scan_node_->row_desc(),
        state_->batch_size(), scan_node_->mem_tracker());
    if (scan_node_->is_partition_key_scan()) batch->limit_capacity(1);
    Status status = GetNextInternal(batch.get());
    if (batch->num_rows() > 0) returned_rows = true;
    // Always add batch to the queue if any rows were returned because it may contain
    // data referenced by previously appended batches.
    if (returned_rows) scan_node->AddMaterializedRowBatch(move(batch));
    RETURN_IF_ERROR(status);
    // Only need to return one row per partition for partition key scans.
    if (returned_rows && scan_node_->is_partition_key_scan()) eos_ = true;
  } while (!eos_ && !scan_node_->ReachedLimitShared());
  return Status::OK();
}

void HdfsScanner::Close() {
  DCHECK(scan_node_->HasRowBatchQueue());
  RowBatch* final_batch = new RowBatch(scan_node_->row_desc(), state_->batch_size(),
      scan_node_->mem_tracker());
  Close(final_batch);
}

void HdfsScanner::CloseInternal() {
  DCHECK(!is_closed_);
  if (decompressor_.get() != nullptr) {
    decompressor_->Close();
    decompressor_.reset();
  }
  for (auto& entry : conjunct_evals_map_) {
    ScalarExprEvaluator::Close(entry.second, state_);
  }
  expr_perm_pool_->FreeAll();
  context_->expr_results_pool()->FreeAll();
  obj_pool_.Clear();
  stream_ = nullptr;
  context_->ClearStreams();
  is_closed_ = true;
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

  write_tuples_fn_ = scan_node_->GetCodegenFn(type);
  if (write_tuples_fn_ == NULL) {
    scan_node_->IncNumScannersCodegenDisabled();
    return Status::OK();
  }
  VLOG(2) << scanner_name << "(node_id=" << scan_node_->id()
          << ") using llvm codegend functions.";
  scan_node_->IncNumScannersCodegenEnabled();
  return Status::OK();
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

Status HdfsScanner::CommitRows(int num_rows, RowBatch* row_batch) {
  DCHECK_LE(num_rows, row_batch->capacity() - row_batch->num_rows());
  row_batch->CommitRows(num_rows);
  tuple_mem_ += static_cast<int64_t>(scan_node_->tuple_desc()->byte_size()) * num_rows;
  tuple_ = reinterpret_cast<Tuple*>(tuple_mem_);
  if (context_->cancelled()) return Status::CancelledInternal("HDFS scanner");
  // Check for UDF errors.
  RETURN_IF_ERROR(state_->GetQueryStatus());
  // Clear expr result allocations for this thread to avoid accumulating too much
  // memory from evaluating the scanner conjuncts.
  context_->expr_results_pool()->Clear();
  return Status::OK();
}

int HdfsScanner::WriteAlignedTuplesCodegenOrInterpret(MemPool* pool,
    TupleRow* tuple_row_mem, FieldLocation* fields, int num_tuples,
    int max_added_tuples, int slots_per_tuple, int row_idx_start, bool copy_strings) {
  return CallCodegendOrInterpreted<WriteTuplesFn>::invoke(this, write_tuples_fn_,
      &HdfsScanner::WriteAlignedTuples, pool, tuple_row_mem, fields, num_tuples,
      max_added_tuples, scan_node_->materialized_slots().size(), row_idx_start,
      copy_strings);
}

int HdfsScanner::WriteTemplateTuples(TupleRow* row, int num_tuples) {
  DCHECK_GE(num_tuples, 0);
  DCHECK_EQ(scan_node_->materialized_slots().size(), 0);
  int num_to_commit = 0;
  if (LIKELY(conjunct_evals_->size() == 0)) {
    num_to_commit = num_tuples;
  } else {
    TupleRow template_tuple_row;
    template_tuple_row.SetTuple(0, template_tuple_);
    // Evaluate any conjuncts which may reference the partition columns.
    for (int i = 0; i < num_tuples; ++i) {
      if (EvalConjuncts(&template_tuple_row)) ++num_to_commit;
    }
  }
  Tuple** row_tuple = reinterpret_cast<Tuple**>(row);
  if (template_tuple_ != nullptr) {
    for (int i = 0; i < num_to_commit; ++i) row_tuple[i] = template_tuple_;
  } else {
    DCHECK_EQ(scan_node_->tuple_desc()->byte_size(), 0);
    // IMPALA-6258: Initialize tuple ptrs to non-null value
    for (int i = 0; i < num_to_commit; ++i) row_tuple[i] = Tuple::POISON;
  }
  return num_to_commit;
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

    const SlotDescriptor* slot_desc = scan_node_->materialized_slots()[i];
    bool error = !text_converter_->WriteSlot(slot_desc, tuple, fields[i].start, len,
        false, need_escape, pool);
    error_fields[i] = error;
    *error_in_row |= error;
  }

  tuple_row->SetTuple(0, tuple);
  return EvalConjuncts(tuple_row);
}

// Codegen for WriteTuple(above) for writing out single nullable string slot and
// evaluating a <slot> = <constantexpr> conjunct. The signature matches WriteTuple()
// except for the first this* argument.
// define i1 @WriteCompleteTuple(%"class.impala::HdfsScanner"* %this,
//                               %"class.impala::MemPool"* %pool,
//                               %"struct.impala::FieldLocation"* %fields,
//                               %"class.impala::Tuple"* %tuple,
//                               %"class.impala::TupleRow"* %tuple_row,
//                               %"class.impala::Tuple"* %template,
//                               i8* %error_fields, i8* %error_in_row) {
// entry:
//   %tuple_ptr = bitcast %"class.impala::Tuple"* %tuple
//                to <{ %"struct.impala::StringValue", i8 }>*
//   %tuple_ptr1 = bitcast %"class.impala::Tuple"* %template
//                 to <{ %"struct.impala::StringValue", i8 }>*
//   %int8_ptr = bitcast <{ %"struct.impala::StringValue", i8 }>* %tuple_ptr to i8*
//   %null_bytes_ptr = getelementptr i8, i8* %int8_ptr, i32 16
//   call void @llvm.memset.p0i8.i64(i8* %null_bytes_ptr, i8 0, i64 1, i32 0, i1 false)
//   %0 = bitcast %"class.impala::TupleRow"* %tuple_row
//        to <{ %"struct.impala::StringValue", i8 }>**
//   %1 = getelementptr <{ %"struct.impala::StringValue", i8 }>*,
//                      <{ %"struct.impala::StringValue", i8 }>** %0, i32 0
//   store <{ %"struct.impala::StringValue", i8 }>* %tuple_ptr,
//         <{ %"struct.impala::StringValue", i8 }>** %1
//   br label %parse
//
// parse:                                            ; preds = %entry
//  %data_ptr = getelementptr %"struct.impala::FieldLocation",
//                            %"struct.impala::FieldLocation"* %fields, i32 0, i32 0
//  %len_ptr = getelementptr %"struct.impala::FieldLocation",
//                           %"struct.impala::FieldLocation"* %fields, i32 0, i32 1
//  %slot_error_ptr = getelementptr i8, i8* %error_fields, i32 0
//  %data = load i8*, i8** %data_ptr
//  %len = load i32, i32* %len_ptr
//  %2 = call i1 @WriteSlot(<{ %"struct.impala::StringValue", i8 }>* %tuple_ptr,
//                          i8* %data, i32 %len)
//  %slot_parse_error = xor i1 %2, true
//  %error_in_row2 = or i1 false, %slot_parse_error
//  %3 = zext i1 %slot_parse_error to i8
//  store i8 %3, i8* %slot_error_ptr
//  %4 = call %"class.impala::ScalarExprEvaluator"* @GetConjunctCtx(
//    %"class.impala::HdfsScanner"* %this, i32 0)
//  %conjunct_eval = call i16 @"impala::Operators::Eq_StringVal_StringValWrapper"(
//    %"class.impala::ScalarExprEvaluator"* %4, %"class.impala::TupleRow"* %tuple_row)
//  %5 = ashr i16 %conjunct_eval, 8
//  %6 = trunc i16 %5 to i8
//  %val = trunc i8 %6 to i1
//  br i1 %val, label %parse3, label %eval_fail
//
// parse3:                                           ; preds = %parse
//   %7 = zext i1 %error_in_row2 to i8
//   store i8 %7, i8* %error_in_row
//   ret i1 true
//
// eval_fail:                                        ; preds = %parse
//   ret i1 false
// }
Status HdfsScanner::CodegenWriteCompleteTuple(const HdfsScanPlanNode* node,
    FragmentState* state, llvm::Function** write_complete_tuple_fn) {
  const vector<ScalarExpr*>& conjuncts = node->conjuncts_;
  LlvmCodeGen* codegen = state->codegen();
  *write_complete_tuple_fn = NULL;

  // Cast away const-ness.  The codegen only sets the cached typed llvm struct.
  TupleDescriptor* tuple_desc = const_cast<TupleDescriptor*>(node->tuple_desc_);
  vector<llvm::Function*> slot_fns;
  for (int i = 0; i < node->materialized_slots_.size(); ++i) {
    llvm::Function* fn = nullptr;
    SlotDescriptor* slot_desc = node->materialized_slots_[i];

    // If the type is CHAR, WriteSlot for this slot cannot be codegen'd. To keep codegen
    // for other things, we call the interpreted code for this slot from the codegen'd
    // code instead of failing codegen. See IMPALA-9747.
    if (TextConverter::SupportsCodegenWriteSlot(slot_desc->type())) {
      RETURN_IF_ERROR(TextConverter::CodegenWriteSlot(codegen, tuple_desc, slot_desc,
          &fn, node->hdfs_table_->null_column_value().data(),
          node->hdfs_table_->null_column_value().size(), true,
          state->query_options().strict_mode));
      if (i >= LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) codegen->SetNoInline(fn);
    }
    slot_fns.push_back(fn);
  }

  // Compute order to materialize slots.  BE assumes that conjuncts should
  // be evaluated in the order specified (optimization is already done by FE)
  vector<int> materialize_order;
  node->ComputeSlotMaterializationOrder(state->desc_tbl(), &materialize_order);

  // Get types to construct matching function signature to WriteCompleteTuple
  llvm::PointerType* uint8_ptr_type = codegen->i8_ptr_type();

  llvm::PointerType* field_loc_ptr_type = codegen->GetStructPtrType<FieldLocation>();
  llvm::PointerType* tuple_opaque_ptr_type = codegen->GetStructPtrType<Tuple>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  llvm::PointerType* mem_pool_ptr_type = codegen->GetStructPtrType<MemPool>();
  llvm::PointerType* hdfs_scanner_ptr_type = codegen->GetStructPtrType<HdfsScanner>();

  // Generate the typed llvm struct for the output tuple
  llvm::StructType* tuple_type = tuple_desc->GetLlvmStruct(codegen);
  if (tuple_type == NULL) return Status("Could not generate tuple struct.");
  llvm::PointerType* tuple_ptr_type = llvm::PointerType::get(tuple_type, 0);

  // Initialize the function prototype.  This needs to match
  // HdfsScanner::WriteCompleteTuple's signature identically.
  LlvmCodeGen::FnPrototype prototype(
      codegen, "WriteCompleteTuple", codegen->bool_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", hdfs_scanner_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("pool", mem_pool_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("fields", field_loc_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple", tuple_opaque_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("tuple_row", tuple_row_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("template", tuple_opaque_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_fields", uint8_ptr_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("error_in_row", uint8_ptr_type));

  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);
  llvm::Value* args[8];
  llvm::Function* fn = prototype.GeneratePrototype(&builder, &args[0]);

  llvm::BasicBlock* parse_block = llvm::BasicBlock::Create(context, "parse", fn);
  llvm::BasicBlock* eval_fail_block = llvm::BasicBlock::Create(context, "eval_fail", fn);

  // Extract the input args
  llvm::Value* this_arg = args[0];
  llvm::Value* mem_pool_arg = args[1];
  llvm::Value* fields_arg = args[2];
  llvm::Value* opaque_tuple_arg = args[3];
  llvm::Value* tuple_arg =
      builder.CreateBitCast(opaque_tuple_arg, tuple_ptr_type, "tuple_ptr");
  llvm::Value* tuple_row_arg = args[4];
  llvm::Value* opaque_template_arg = args[5];
  llvm::Value* errors_arg = args[6];
  llvm::Value* error_in_row_arg = args[7];

  // Codegen for function body
  llvm::Value* error_in_row = codegen->false_value();

  llvm::Function* init_tuple_fn;
  RETURN_IF_ERROR(CodegenInitTuple(node, codegen, &init_tuple_fn));
  builder.CreateCall(init_tuple_fn, {this_arg, opaque_template_arg, opaque_tuple_arg});

  // Put tuple in tuple_row
  llvm::Value* tuple_row_typed =
      builder.CreateBitCast(tuple_row_arg, llvm::PointerType::get(tuple_ptr_type, 0));
  llvm::Value* tuple_row_idxs[] = {codegen->GetI32Constant(0)};
  llvm::Value* tuple_in_row_addr =
      builder.CreateInBoundsGEP(tuple_row_typed, tuple_row_idxs);
  builder.CreateStore(tuple_arg, tuple_in_row_addr);
  builder.CreateBr(parse_block);

  // Loop through all the conjuncts in order and materialize slots as necessary to
  // evaluate the conjuncts (e.g. conjuncts[0] will have the slots it references
  // first).
  // materialized_order[slot_idx] represents the first conjunct which needs that slot.
  // Slots are only materialized if its order matches the current conjunct being
  // processed.  This guarantees that each slot is materialized once when it is first
  // needed and that at the end of the materialize loop, the conjunct has everything
  // it needs (either from this iteration or previous iterations).
  builder.SetInsertPoint(parse_block);
  for (int conjunct_idx = 0; conjunct_idx <= conjuncts.size(); ++conjunct_idx) {
    for (int slot_idx = 0; slot_idx < materialize_order.size(); ++slot_idx) {
      // If they don't match, it means either the slot has already been
      // materialized for a previous conjunct or will be materialized later for
      // another conjunct.  Either case, the slot does not need to be materialized
      // yet.
      if (materialize_order[slot_idx] != conjunct_idx) continue;

      // Materialize slots[slot_idx] to evaluate conjuncts[conjunct_idx]
      // All slots[i] with materialized_order[i] < conjunct_idx have already been
      // materialized by prior iterations through the outer loop

      // Extract ptr/len from fields
      llvm::Value* data_idxs[] = {
          codegen->GetI32Constant(slot_idx),
          codegen->GetI32Constant(0),
      };
      llvm::Value* len_idxs[] = {
          codegen->GetI32Constant(slot_idx),
          codegen->GetI32Constant(1),
      };
      llvm::Value* error_idxs[] = {
          codegen->GetI32Constant(slot_idx),
      };
      llvm::Value* data_ptr =
          builder.CreateInBoundsGEP(fields_arg, data_idxs, "data_ptr");
      llvm::Value* len_ptr = builder.CreateInBoundsGEP(fields_arg, len_idxs, "len_ptr");
      llvm::Value* error_ptr =
          builder.CreateInBoundsGEP(errors_arg, error_idxs, "slot_error_ptr");
      llvm::Value* data = builder.CreateLoad(data_ptr, "data");
      llvm::Value* len = builder.CreateLoad(len_ptr, "len");

      // Convert length to positive if it is negative. Negative lengths are assigned to
      // slots that contain escape characters.
      // TODO: CodegenWriteSlot() currently does not handle text that requres unescaping.
      // However, if it is modified to handle that case, we need to detect it here and
      // send a 'need_escape' bool to CodegenWriteSlot(), since we are making the length
      // positive here.
      llvm::Value* len_lt_zero =
          builder.CreateICmpSLT(len, codegen->GetI32Constant(0), "len_lt_zero");
      llvm::Value* ones_compliment_len = builder.CreateNot(len, "ones_compliment_len");
      llvm::Value* positive_len = builder.CreateAdd(
          ones_compliment_len, codegen->GetI32Constant(1), "positive_len");
      len = builder.CreateSelect(len_lt_zero, positive_len, len,
          "select_positive_len");

      // Call slot parse function
      llvm::Function* slot_fn = slot_fns[slot_idx];

      llvm::Value* slot_parsed = nullptr;
      if (LIKELY(slot_fn != nullptr)) {
        slot_parsed = builder.CreateCall(
            slot_fn, llvm::ArrayRef<llvm::Value*>({tuple_arg, data, len}));
      } else {
        llvm::Value* slot_idx_value = codegen->GetI32Constant(slot_idx);
        llvm::Function* interpreted_slot_fn = codegen->GetFunction(
            IRFunction::HDFS_SCANNER_TEXT_CONVERTER_WRITE_SLOT_INTERPRETED_IR, false);
        DCHECK(interpreted_slot_fn != nullptr);

        slot_parsed = builder.CreateCall(interpreted_slot_fn,
            {this_arg, slot_idx_value, opaque_tuple_arg, data, len, mem_pool_arg});
      }

      llvm::Value* slot_error = builder.CreateNot(slot_parsed, "slot_parse_error");
      error_in_row = builder.CreateOr(error_in_row, slot_error, "error_in_row");
      slot_error = builder.CreateZExt(slot_error, codegen->i8_type());
      builder.CreateStore(slot_error, error_ptr);
    }

    if (conjunct_idx == conjuncts.size()) {
      // In this branch, we've just materialized slots not referenced by any conjunct.
      // This slots are the last to get materialized.  If we are in this branch, the
      // tuple passed all conjuncts and should be added to the row batch.
      llvm::Value* error_ret =
          builder.CreateZExt(error_in_row, codegen->i8_type());
      builder.CreateStore(error_ret, error_in_row_arg);
      builder.CreateRet(codegen->true_value());
    } else {
      // All slots for conjuncts[conjunct_idx] are materialized, evaluate the partial
      // tuple against that conjunct and start a new parse_block for the next conjunct
      parse_block = llvm::BasicBlock::Create(context, "parse", fn, eval_fail_block);
      llvm::Function* conjunct_fn;
      RETURN_IF_ERROR(
          conjuncts[conjunct_idx]->GetCodegendComputeFn(codegen, false, &conjunct_fn));
      if (node->materialized_slots_.size() + conjunct_idx
          >= LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
        codegen->SetNoInline(conjunct_fn);
      }

      llvm::Function* get_eval_fn =
          codegen->GetFunction(IRFunction::HDFS_SCANNER_GET_CONJUNCT_EVALUATOR, false);
      llvm::Value* eval = builder.CreateCall(
          get_eval_fn, llvm::ArrayRef<llvm::Value*>(
                           {this_arg, codegen->GetI32Constant(conjunct_idx)}));

      llvm::Value* conjunct_args[] = {eval, tuple_row_arg};
      CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(
          codegen, &builder, ColumnType(TYPE_BOOLEAN), conjunct_fn, conjunct_args,
          "conjunct_eval");
      builder.CreateCondBr(result.GetVal(), parse_block, eval_fail_block);
      builder.SetInsertPoint(parse_block);
    }
  }

  // Block if eval failed.
  builder.SetInsertPoint(eval_fail_block);
  builder.CreateRet(codegen->false_value());

  if (node->materialized_slots_.size() + conjuncts.size()
      > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(fn);
  }
  *write_complete_tuple_fn = codegen->FinalizeFunction(fn);
  if (*write_complete_tuple_fn == NULL) {
    return Status("Failed to finalize write_complete_tuple_fn.");
  }
  return Status::OK();
}

Status HdfsScanner::CodegenWriteAlignedTuples(const HdfsScanPlanNode* node,
    FragmentState* state, llvm::Function* write_complete_tuple_fn,
    llvm::Function** write_aligned_tuples_fn) {
  LlvmCodeGen* codegen = state->codegen();
  *write_aligned_tuples_fn = NULL;
  DCHECK(write_complete_tuple_fn != NULL);

  llvm::Function* write_tuples_fn =
      codegen->GetFunction(IRFunction::HDFS_SCANNER_WRITE_ALIGNED_TUPLES, true);
  DCHECK(write_tuples_fn != NULL);

  int replaced = codegen->ReplaceCallSites(write_tuples_fn, write_complete_tuple_fn,
      "WriteCompleteTuple");
  DCHECK_REPLACE_COUNT(replaced, 1);

  llvm::Function* copy_strings_fn;
  RETURN_IF_ERROR(Tuple::CodegenCopyStrings(
      codegen, *node->tuple_desc_, &copy_strings_fn));
  replaced = codegen->ReplaceCallSites(
      write_tuples_fn, copy_strings_fn, "CopyStrings");
  DCHECK_REPLACE_COUNT(replaced, 1);

  int tuple_byte_size = node->tuple_desc_->byte_size();
  replaced = codegen->ReplaceCallSitesWithValue(write_tuples_fn,
      codegen->GetI32Constant(tuple_byte_size), "tuple_byte_size");
  DCHECK_REPLACE_COUNT(replaced, 1);

  *write_aligned_tuples_fn = codegen->FinalizeFunction(write_tuples_fn);
  if (*write_aligned_tuples_fn == NULL) {
    return Status("Failed to finalize write_aligned_tuples_fn.");
  }
  return Status::OK();
}

Status HdfsScanner::CodegenInitTuple(
    const HdfsScanPlanNode* node, LlvmCodeGen* codegen, llvm::Function** init_tuple_fn) {
  *init_tuple_fn = codegen->GetFunction(IRFunction::HDFS_SCANNER_INIT_TUPLE, true);
  DCHECK(*init_tuple_fn != nullptr);

  // Replace all of the constants in InitTuple() to specialize the code.
  bool has_template_tuple = !node->partition_key_slots_.empty();
  if (!has_template_tuple) {
    has_template_tuple = node->HasVirtualColumnInTemplateTuple();
  }
  int replaced = 0;
  // If has_template_tuple is true, then we can certainly replace the callsites.
  // If has_template_tuple is false, then we should only replace the callsites for
  // non-Iceberg tables, as for Iceberg tables we might still create a template
  // tuple based on the partitioning in the data files.
  if (has_template_tuple || !node->hdfs_table_->IsIcebergTable()) {
    replaced = codegen->ReplaceCallSitesWithBoolConst(
      *init_tuple_fn, has_template_tuple, "has_template_tuple");
    DCHECK_REPLACE_COUNT(replaced, 1);
  }

  const TupleDescriptor* tuple_desc = node->tuple_desc_;
  replaced = codegen->ReplaceCallSitesWithValue(*init_tuple_fn,
      codegen->GetI32Constant(tuple_desc->byte_size()), "tuple_byte_size");
  DCHECK_REPLACE_COUNT(replaced, 1);

  replaced = codegen->ReplaceCallSitesWithValue(*init_tuple_fn,
      codegen->GetI32Constant(tuple_desc->null_bytes_offset()),
      "null_bytes_offset");
  DCHECK_REPLACE_COUNT(replaced, 1);

  replaced = codegen->ReplaceCallSitesWithValue(*init_tuple_fn,
      codegen->GetI32Constant(tuple_desc->num_null_bytes()), "num_null_bytes");
  DCHECK_REPLACE_COUNT(replaced, 1);

  *init_tuple_fn = codegen->FinalizeFunction(*init_tuple_fn);
  if (*init_tuple_fn == nullptr) {
    return Status("Failed to finalize codegen'd InitTuple().");
  }
  return Status::OK();
}

// ; Function Attrs: noinline
// define i1 @EvalRuntimeFilters(%"class.impala::HdfsScanner"* %this,
//                               %"class.impala::TupleRow"* %row) #34 {
// entry:
//   %0 = call i1 @_ZN6impala11HdfsScanner17EvalRuntimeFilterEiPNS_8TupleRowE.2(
//       %"class.impala::HdfsScanner"* %this, i32 0, %"class.impala::TupleRow"*
//       %row)
//   br i1 %0, label %continue, label %bail_out
//
// bail_out:                                         ; preds = %entry
//   ret i1 false
//
// continue:                                         ; preds = %entry
//   ret i1 true
// }
//
// EvalRuntimeFilter() is the same as the cross-compiled version except EvalOneFilter()
// is replaced with the one generated by FilterContext::CodegenEval().
Status HdfsScanner::CodegenEvalRuntimeFilters(
    LlvmCodeGen* codegen, const vector<ScalarExpr*>& filter_exprs, llvm::Function** fn) {
  llvm::LLVMContext& context = codegen->context();
  LlvmBuilder builder(context);

  *fn = nullptr;
  llvm::Type* this_type = codegen->GetStructPtrType<HdfsScanner>();
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(codegen, "EvalRuntimeFilters",
                                     codegen->bool_type());
  prototype.AddArgument(LlvmCodeGen::NamedVariable("this", this_type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  llvm::Value* args[2];
  llvm::Function* eval_runtime_filters_fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* this_arg = args[0];
  llvm::Value* row_arg = args[1];

  int num_filters = filter_exprs.size();
  if (num_filters == 0) {
    builder.CreateRet(codegen->true_value());
  } else {
    // row_rejected_block: jump target for when a filter is evaluated to false.
    llvm::BasicBlock* row_rejected_block =
        llvm::BasicBlock::Create(context, "row_rejected", eval_runtime_filters_fn);

    DCHECK_GT(num_filters, 0);
    for (int i = 0; i < num_filters; ++i) {
      // Make a clone of HdfsScanner::EvalRuntimeFilter(int i, TupleRow* row)
      llvm::Function* eval_runtime_filter_fn =
          codegen->GetFunction(IRFunction::HDFS_SCANNER_EVAL_RUNTIME_FILTER, true);
      DCHECK(eval_runtime_filter_fn != nullptr);

      // Codegen function for inlining filter's expression evaluation and constant fold
      // the type of the expression into the hashing function to avoid branches.
      llvm::Function* eval_one_filter_fn;
      DCHECK(filter_exprs[i] != nullptr);
      RETURN_IF_ERROR(FilterContext::CodegenEval(codegen, filter_exprs[i],
          &eval_one_filter_fn));
      DCHECK(eval_one_filter_fn != nullptr);

      // "FilterContext4Eval" is FilterContext::Eval(impala::TupleRow*).
      // Replace ctx->Eval(row) in the above cloned function with its LLVM version
      // which is 'eval_one_filter_fn'.
      int replaced = codegen->ReplaceCallSites(eval_runtime_filter_fn, eval_one_filter_fn,
          "FilterContext4Eval");
      DCHECK_REPLACE_COUNT(replaced, 1);

      // Create a call to the above LLVM optimized eval_runtime_filter_fn.
      llvm::Value* idx = codegen->GetI32Constant(i);
      llvm::Value* passed_filter = builder.CreateCall(
          eval_runtime_filter_fn, llvm::ArrayRef<llvm::Value*>({this_arg, idx, row_arg}));

      // Create a new block "continue" at the end of eval_runtime_filters_fn block.
      llvm::BasicBlock* continue_block =
          llvm::BasicBlock::Create(context, "continue", eval_runtime_filters_fn);
      // Create a condition on top of eval_runtime_filter_fn.
      builder.CreateCondBr(passed_filter, continue_block, row_rejected_block);
      // Mark 'continue_block' the location to accumulate any additional LLVM code
      // for the next filter (if any).
      builder.SetInsertPoint(continue_block);
    }
    builder.CreateRet(codegen->true_value());

    builder.SetInsertPoint(row_rejected_block);
    builder.CreateRet(codegen->false_value());

    // Don't inline this function to avoid code bloat in ProcessScratchBatch().
    // If there is any filter, EvalRuntimeFilters() is large enough to not benefit
    // much from inlining.
    eval_runtime_filters_fn->addFnAttr(llvm::Attribute::NoInline);
  }

  *fn = codegen->FinalizeFunction(eval_runtime_filters_fn);
  if (*fn == nullptr) {
    return Status("Codegen'd HdfsScanner::EvalRuntimeFilters() failed "
        "verification, see log");
  }
  return Status::OK();
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

Status HdfsScanner::DecompressFileToBuffer(uint8_t** buffer, int64_t* bytes_read) {
  // For other compressed file: attempt to read and decompress the entire file, point
  // to the decompressed buffer, and then continue normal processing.
  DCHECK(decompression_type_ != THdfsCompression::SNAPPY);
  const HdfsFileDesc* desc = scan_node_->GetFileDesc(
      context_->partition_descriptor()->id(), stream_->filename());
  int64_t file_size = desc->file_length;
  DCHECK_GT(file_size, 0);

  Status status;
  if (!stream_->GetBytes(file_size, buffer, bytes_read, &status)) {
    DCHECK(!status.ok());
    return status;
  }

  // If didn't read anything, return.
  if (*bytes_read == 0) return Status::OK();

  // Need to read the entire file.
  if (file_size > *bytes_read) {
    return Status(Substitute("Expected to read a compressed text file of size $0 bytes. "
        "But only read $1 bytes. This may indicate data file corruption. (file: $3).",
        file_size, *bytes_read, stream_->filename()));
  }

  // Decompress and adjust the buffer and bytes_read accordingly.
  int64_t decompressed_len = 0;
  uint8_t* decompressed_buffer = nullptr;
  SCOPED_TIMER(decompress_timer_);
  // TODO: Once the writers are in, add tests with very large compressed files (4GB)
  // that could overflow.
  RETURN_IF_ERROR(decompressor_->ProcessBlock(false, *bytes_read, *buffer,
      &decompressed_len, &decompressed_buffer));

  // Inform 'stream_' that the buffer with the compressed text can be released.
  context_->ReleaseCompletedResources(true);

  VLOG_FILE << "Decompressed " << *bytes_read << " to " << decompressed_len;
  *buffer = decompressed_buffer;
  *bytes_read = decompressed_len;
  return Status::OK();
}

Status HdfsScanner::DecompressStreamToBuffer(uint8_t** buffer, int64_t* bytes_read,
    MemPool* pool, bool* eosr) {
  // We're about to create a new decompression buffer (if we can't reuse). Attach the
  // memory from previous decompression rounds to 'pool'.
  if (!decompressor_->reuse_output_buffer()) {
    if (pool != nullptr) {
      pool->AcquireData(data_buffer_pool_.get(), false);
    } else {
      data_buffer_pool_->FreeAll();
    }
  }

  uint8_t* decompressed_buffer = nullptr;
  int64_t decompressed_len = 0;
  // Set bytes_to_read = -1 because we don't know how much data decompressor need.
  // Just read the first available buffer within the scan range.
  Status status = DecompressStream(-1, &decompressed_buffer, &decompressed_len,
      eosr);
  if (status.code() == TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_NO_PROGRESS) {
    // It's possible (but very unlikely) that ProcessBlockStreaming() wasn't able to
    // make progress if the compressed buffer returned by GetBytes() is too small.
    // (Note that this did not even occur in simple experiments where the input buffer
    // is always 1 byte, but we need to handle this case to be defensive.) In this
    // case, try again with a reasonably large fixed size buffer. If we still did not
    // make progress, then return an error.
    LOG(INFO) << status.GetDetail();
    // Number of bytes to read when the previous attempt to streaming decompress did not
    // make progress.
    constexpr int64_t COMPRESSED_DATA_FIXED_READ_SIZE = 1 * 1024 * 1024;
    status = DecompressStream(COMPRESSED_DATA_FIXED_READ_SIZE, &decompressed_buffer,
        &decompressed_len, eosr);
  }
  RETURN_IF_ERROR(status);
  *buffer = decompressed_buffer;
  *bytes_read = decompressed_len;

  if (*eosr) {
    DCHECK(stream_->eosr());
    context_->ReleaseCompletedResources(true);
  }

  return Status::OK();
}

Status HdfsScanner::DecompressStream(int64_t bytes_to_read,
    uint8_t** decompressed_buffer, int64_t* decompressed_len, bool *eosr) {
  // Some decompressors, such as Bzip2 API (version 0.9 and later) and Gzip can
  // decompress buffers that are read from stream_, so we don't need to read the
  // whole file in once. A compressed buffer is passed to ProcessBlockStreaming
  // but it may not consume all of the input.
  uint8_t* compressed_buffer_ptr = nullptr;
  int64_t compressed_buffer_size = 0;
  // We don't know how many bytes ProcessBlockStreaming() will consume so we set
  // peek=true and then later advance the stream using SkipBytes().
  if (bytes_to_read == -1) {
    RETURN_IF_ERROR(stream_->GetBuffer(true, &compressed_buffer_ptr,
        &compressed_buffer_size));
  } else {
    DCHECK_GT(bytes_to_read, 0);
    Status status;
    if (!stream_->GetBytes(bytes_to_read, &compressed_buffer_ptr, &compressed_buffer_size,
        &status, true)) {
      DCHECK(!status.ok());
      return status;
    }
  }
  int64_t compressed_buffer_bytes_read = 0;
  bool stream_end = false;
  {
    SCOPED_TIMER(decompress_timer_);
    Status status = decompressor_->ProcessBlockStreaming(compressed_buffer_size,
        compressed_buffer_ptr, &compressed_buffer_bytes_read, decompressed_len,
        decompressed_buffer, &stream_end);
    if (!status.ok()) {
      status.AddDetail(Substitute("$0file=$1, offset=$2", status.GetDetail(),
          stream_->filename(), stream_->file_offset()));
      return status;
    }
    DCHECK_GE(compressed_buffer_size, compressed_buffer_bytes_read);
  }
  // Skip the bytes in stream_ that were decompressed.
  Status status;
  if (!stream_->SkipBytes(compressed_buffer_bytes_read, &status)) {
    DCHECK(!status.ok());
    return status;
  }

  if (stream_->eosr()) {
    if (stream_end) {
      *eosr = true;
    } else {
      return Status(TErrorCode::COMPRESSED_FILE_TRUNCATED, stream_->filename());
    }
  } else if (*decompressed_len == 0) {
    return Status(TErrorCode::COMPRESSED_FILE_DECOMPRESSOR_NO_PROGRESS,
        stream_->filename());
  }

  return Status::OK();
}

bool HdfsScanner::ReportTupleParseError(FieldLocation* fields, uint8_t* errors) {
  for (int i = 0; i < scan_node_->materialized_slots().size(); ++i) {
    if (errors[i]) {
      const SlotDescriptor* desc = scan_node_->materialized_slots()[i];
      ReportColumnParseError(desc, fields[i].start, fields[i].len);
      errors[i] = false;
    }
  }
  LogRowParseError();

  if (state_->abort_on_error()) DCHECK(!parse_status_.ok());
  return parse_status_.ok();
}

void HdfsScanner::LogRowParseError() {
  const string& s = Substitute("Error parsing row: file: $0, before offset: $1",
      stream_->filename(), stream_->file_offset());
  state_->LogError(ErrorMsg(TErrorCode::GENERAL, s));
}

void HdfsScanner::ReportColumnParseError(const SlotDescriptor* desc,
    const char* data, int len) {
  if (state_->LogHasSpace() || state_->abort_on_error()) {
    stringstream ss;
    ss << "Error converting column: "
       << desc->col_pos() - scan_node_->num_partition_keys()
       << " to " << desc->type();

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

void HdfsScanner::CheckFiltersEffectiveness() {
  for (int i = 0; i < filter_stats_.size(); ++i) {
    LocalFilterStats* stats = &filter_stats_[i];
    const RuntimeFilter* filter = filter_ctxs_[i]->filter;
    double reject_ratio = stats->rejected / static_cast<double>(stats->considered);
    if (filter->AlwaysTrue() ||
        reject_ratio < FLAGS_min_filter_reject_ratio) {
      stats->enabled_for_row = 0;
    }
  }
}

Status HdfsScanner::IssueFooterRanges(HdfsScanNodeBase* scan_node,
    const THdfsFileFormat::type& file_type, const vector<HdfsFileDesc*>& files,
    int64_t footer_size_estimate) {
  DCHECK(!files.empty());
  DCHECK_LE(footer_size_estimate, READ_SIZE_MIN_VALUE);
  vector<ScanRange*> footer_ranges;
  for (int i = 0; i < files.size(); ++i) {
    // Compute the offset of the file footer.
    int64_t footer_size = min(footer_size_estimate, files[i]->file_length);
    int64_t footer_start = files[i]->file_length - footer_size;
    DCHECK_GE(footer_start, 0);

    // Try to find the split with the footer.
    ScanRange* footer_split = FindFooterSplit(files[i]);
    bool footer_scanner =
        scan_node->IsZeroSlotTableScan() || scan_node->optimize_count_star();

    for (int j = 0; j < files[i]->splits.size(); ++j) {
      ScanRange* split = files[i]->splits[j];

      DCHECK_LE(split->offset() + split->len(), files[i]->file_length);
      // If there are no materialized slots (such as count(*) over the table), we can
      // get the result with the file metadata alone and don't need to read any row
      // groups. We only want a single node to process the file footer in this case,
      // which is the node with the footer split.  If it's not a count(*), we create a
      // footer range for the split always.
      if (!footer_scanner || footer_split == split) {
        ScanRangeMetadata* split_metadata =
            static_cast<ScanRangeMetadata*>(split->meta_data());
        // Each split is processed by first issuing a scan range for the file footer, which
        // is done here, followed by scan ranges for the columns of each row group within
        // the actual split (in InitColumns()). The original split is stored in the
        // metadata associated with the footer range.
        ScanRange* footer_range;
        if (footer_split != nullptr) {
          footer_range = scan_node->AllocateScanRange(files[i]->GetFileInfo(),
              footer_size, footer_start, split_metadata->partition_id,
              footer_split->disk_id(), footer_split->expected_local(),
              BufferOpts(footer_split->cache_options()), split);
        } else {
          // If we did not find the last split, we know it is going to be a remote read.
          bool expected_local = false;
          int cache_options = !scan_node->IsDataCacheDisabled() ?
              BufferOpts::USE_DATA_CACHE : BufferOpts::NO_CACHING;
          footer_range =
              scan_node->AllocateScanRange(files[i]->GetFileInfo(),
                  footer_size, footer_start, split_metadata->partition_id, -1,
                  expected_local, BufferOpts(cache_options), split);
        }
        footer_ranges.push_back(footer_range);
      } else {
        scan_node->RangeComplete(file_type, THdfsCompression::NONE);
      }
    }
  }
  // The threads that process the footer will also do the scan.
  if (footer_ranges.size() > 0) {
    RETURN_IF_ERROR(scan_node->AddDiskIoRanges(footer_ranges, EnqueueLocation::TAIL));
  }
  return Status::OK();
}

ScanRange* HdfsScanner::FindFooterSplit(HdfsFileDesc* file) {
  DCHECK(file != nullptr);
  for (int i = 0; i < file->splits.size(); ++i) {
    ScanRange* split = file->splits[i];
    if (split->offset() + split->len() == file->file_length) return split;
  }
  return nullptr;
}

bool HdfsScanner::EvalRuntimeFilters(TupleRow* row) {
  int num_filters = filter_ctxs_.size();
  for (int i = 0; i < num_filters; ++i) {
    if (!EvalRuntimeFilter(i, row)) return false;
  }
  return true;
}
