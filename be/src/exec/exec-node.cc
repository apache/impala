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

#include "exec/exec-node.h"

#include <memory>
#include <sstream>
#include <unistd.h>  // for sleep()

#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exec/aggregation-node.h"
#include "exec/analytic-eval-node.h"
#include "exec/cardinality-check-node.h"
#include "exec/data-source-scan-node.h"
#include "exec/empty-set-node.h"
#include "exec/exchange-node.h"
#include "exec/exec-node-util.h"
#include "exec/hbase/hbase-scan-node.h"
#include "exec/hdfs-scan-node-mt.h"
#include "exec/hdfs-scan-node.h"
#include "exec/iceberg-delete-node.h"
#include "exec/iceberg-metadata/iceberg-metadata-scan-node.h"
#include "exec/kudu/kudu-scan-node-mt.h"
#include "exec/kudu/kudu-scan-node.h"
#include "exec/kudu/kudu-util.h"
#include "exec/nested-loop-join-node.h"
#include "exec/partial-sort-node.h"
#include "exec/partitioned-hash-join-node.h"
#include "exec/select-node.h"
#include "exec/singular-row-src-node.h"
#include "exec/sort-node.h"
#include "exec/streaming-aggregation-node.h"
#include "exec/subplan-node.h"
#include "exec/topn-node.h"
#include "exec/tuple-cache-node.h"
#include "exec/union-node.h"
#include "exec/unnest-node.h"
#include "exprs/expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/initial-reservations.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/string-parser.h"
#include "util/uid-util.h"

#include "common/names.h"

using strings::Substitute;

namespace impala {
Status PlanNode::Init(const TPlanNode& tnode, FragmentState* state) {
  tnode_ = &tnode;
  row_descriptor_ = state->obj_pool()->Add(
      new RowDescriptor(state->desc_tbl(), tnode_->row_tuples, tnode_->nullable_tuples));

  if (tnode_->node_type != TPlanNodeType::AGGREGATION_NODE) {
    // In Agg node the conjuncts are executed by the Aggregators.
    RETURN_IF_ERROR(
        ScalarExpr::Create(tnode_->conjuncts, *row_descriptor_, state, &conjuncts_));
  }

  if (state->query_options().compute_processing_cost) {
    DCHECK_GT(state->fragment().effective_instance_count, 0);
    is_mt_fragment_ = true;
    num_instances_per_node_ = state->fragment().effective_instance_count;
  } else if (state->query_options().mt_dop > 0) {
    is_mt_fragment_ = true;
    num_instances_per_node_ = state->query_options().mt_dop;
  } else {
    is_mt_fragment_ = false;
    num_instances_per_node_ = 1;
  }
  return Status::OK();
}

void PlanNode::Close() {
  ScalarExpr::Close(conjuncts_);
  ScalarExpr::Close(runtime_filter_exprs_);
  for (auto& child : children_) {
    child->Close();
  }
}

Status PlanNode::CreateTree(FragmentState* state, const TPlan& plan, PlanNode** root) {
  if (plan.nodes.size() == 0) {
    *root = NULL;
    return Status::OK();
  }
  int node_idx = 0;
  Status status =
      CreateTreeHelper(state, plan.nodes, NULL, &node_idx, root);
  if (status.ok() && node_idx + 1 != plan.nodes.size()) {
    status = Status(
        "Plan tree only partially reconstructed. Not all thrift nodes were used.");
  }
  if (!status.ok()) {
    LOG(ERROR) << "Could not construct plan tree:\n"
               << apache::thrift::ThriftDebugString(plan);
  }
  return status;
}

Status PlanNode::CreateTreeHelper(FragmentState* state,
    const std::vector<TPlanNode>& tnodes, PlanNode* parent, int* node_idx,
    PlanNode** root) {
  // propagate error case
  if (*node_idx >= tnodes.size()) {
    return Status("Failed to reconstruct plan tree from thrift.");
  }
  const TPlanNode& tnode = tnodes[*node_idx];

  int num_children = tnode.num_children;
  PlanNode* node = NULL;
  RETURN_IF_ERROR(CreatePlanNode(state->obj_pool(), tnode, &node));
  if (parent != NULL) {
    parent->children_.push_back(node);
  } else {
    *root = node;
  }
  for (int i = 0; i < num_children; ++i) {
    ++*node_idx;
    RETURN_IF_ERROR(
        CreateTreeHelper(state, tnodes, node, node_idx, nullptr));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= tnodes.size()) {
      return Status("Failed to reconstruct plan tree from thrift.");
    }
  }

  // Call Init() after children have been set and Init()'d themselves
  RETURN_IF_ERROR(node->Init(tnode, state));
  return Status::OK();
}

void PlanNode::AddCodegenStatus(
    const Status& codegen_status, const std::string& extra_label) {
  codegen_status_msgs_.emplace_back(FragmentState::GenerateCodegenMsg(
      codegen_status.ok(), codegen_status, extra_label));
}

Status PlanNode::CreatePlanNode(
    ObjectPool* pool, const TPlanNode& tnode, PlanNode** node) {
  switch (tnode.node_type) {
    case TPlanNodeType::HDFS_SCAN_NODE:
      *node = pool->Add(new HdfsScanPlanNode());
      break;
    case TPlanNodeType::HBASE_SCAN_NODE:
    case TPlanNodeType::DATA_SOURCE_NODE:
    case TPlanNodeType::KUDU_SCAN_NODE:
    case TPlanNodeType::SYSTEM_TABLE_SCAN_NODE:
      *node = pool->Add(new ScanPlanNode());
      break;
    case TPlanNodeType::AGGREGATION_NODE:
      *node = pool->Add(new AggregationPlanNode());
      break;
    case TPlanNodeType::HASH_JOIN_NODE:
      *node = pool->Add(new PartitionedHashJoinPlanNode());
      break;
    case TPlanNodeType::NESTED_LOOP_JOIN_NODE:
      *node = pool->Add(new NestedLoopJoinPlanNode());
      break;
    case TPlanNodeType::EMPTY_SET_NODE:
      *node = pool->Add(new EmptySetPlanNode());
      break;
    case TPlanNodeType::EXCHANGE_NODE:
      *node = pool->Add(new ExchangePlanNode());
      break;
    case TPlanNodeType::SELECT_NODE:
      *node = pool->Add(new SelectPlanNode());
      break;
    case TPlanNodeType::SORT_NODE:
      if (tnode.sort_node.type == TSortType::PARTIAL) {
        *node = pool->Add(new PartialSortPlanNode());
      } else if (tnode.sort_node.type == TSortType::TOPN ||
          tnode.sort_node.type == TSortType::PARTITIONED_TOPN) {
        *node = pool->Add(new TopNPlanNode());
      } else {
        DCHECK(tnode.sort_node.type == TSortType::TOTAL);
        *node = pool->Add(new SortPlanNode());
      }
      break;
    case TPlanNodeType::UNION_NODE:
      *node = pool->Add(new UnionPlanNode());
      break;
    case TPlanNodeType::ANALYTIC_EVAL_NODE:
      *node = pool->Add(new AnalyticEvalPlanNode());
      break;
    case TPlanNodeType::SINGULAR_ROW_SRC_NODE:
      *node = pool->Add(new SingularRowSrcPlanNode());
      break;
    case TPlanNodeType::SUBPLAN_NODE:
      *node = pool->Add(new SubplanPlanNode());
      break;
    case TPlanNodeType::UNNEST_NODE:
      *node = pool->Add(new UnnestPlanNode());
      break;
    case TPlanNodeType::CARDINALITY_CHECK_NODE:
      *node = pool->Add(new CardinalityCheckPlanNode());
      break;
    case TPlanNodeType::ICEBERG_DELETE_NODE:
      *node = pool->Add(new IcebergDeletePlanNode());
      break;
    case TPlanNodeType::ICEBERG_METADATA_SCAN_NODE:
      *node = pool->Add(new IcebergMetadataScanPlanNode());
      break;
    case TPlanNodeType::TUPLE_CACHE_NODE:
      *node = pool->Add(new TupleCachePlanNode());
      break;
    default:
      map<int, const char*>::const_iterator i =
          _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
      const char* str = "unknown node type";
      if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
        str = i->second;
      }
      stringstream error_msg;
      error_msg << str << " not implemented";
      return Status(error_msg.str());
  }
  return Status::OK();
}

void PlanNode::Codegen(FragmentState* state) {
  DCHECK(state->ShouldCodegen());
  DCHECK(state->codegen() != nullptr);
  for (PlanNode* child : children_) {
    child->Codegen(state);
  }
}

const string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

ExecNode::ExecNode(ObjectPool* pool, const PlanNode& pnode, const DescriptorTbl& descs)
  : plan_node_(pnode),
    id_(pnode.tnode_->node_id),
    type_(pnode.tnode_->node_type),
    pool_(pool),
    conjuncts_(pnode.conjuncts_),
    row_descriptor_(*(pnode.row_descriptor_)),
    resource_profile_(pnode.tnode_->resource_profile),
    limit_(pnode.tnode_->limit),
    runtime_profile_(RuntimeProfile::Create(
        pool_, Substitute("$0 (id=$1)", PrintValue(type_), id_))),
    rows_returned_counter_(nullptr),
    rows_returned_rate_(nullptr),
    containing_subplan_(nullptr),
    num_rows_returned_(0),
    is_closed_(false) {
  runtime_profile_->SetPlanNodeId(id_);
  debug_options_.phase = TExecNodePhase::INVALID;
}

ExecNode::~ExecNode() {
}

Status ExecNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::PREPARE, state));
  DCHECK(runtime_profile_ != NULL);
  mem_tracker_.reset(new MemTracker(runtime_profile_, -1, runtime_profile_->name(),
      state->instance_mem_tracker()));
  expr_mem_tracker_.reset(new MemTracker(-1, "Exprs", mem_tracker_.get(), false));
  expr_perm_pool_.reset(new MemPool(expr_mem_tracker_.get()));
  expr_results_pool_.reset(new MemPool(expr_mem_tracker_.get()));
  rows_returned_counter_ = ADD_COUNTER(runtime_profile_, "RowsReturned", TUnit::UNIT);
  rows_returned_rate_ = runtime_profile()->AddDerivedCounter(
      ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, rows_returned_counter_,
          runtime_profile()->total_time_counter()));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(conjuncts_, state, pool_, expr_perm_pool(),
      expr_results_pool(), &conjunct_evals_));
  DCHECK_EQ(conjunct_evals_.size(), conjuncts_.size());
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state));
  }
  reservation_manager_.Init(
      Substitute("$0 id=$1 ptr=$2", PrintValue(type_), id_, this), runtime_profile_,
      state->instance_buffer_reservation(), mem_tracker_.get(), resource_profile_,
      debug_options_);
  if (!IsInSubplan()) {
    events_ = runtime_profile_->AddEventSequence("Node Lifecycle Event Timeline");
    events_->Start(state->query_state()->fragment_events_start_time());
  }
  return Status::OK();
}

Status ExecNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN, state));
  DCHECK_EQ(conjunct_evals_.size(), conjuncts_.size());
  return ScalarExprEvaluator::Open(conjunct_evals_, state);
}

Status ExecNode::Reset(RuntimeState* state, RowBatch* row_batch) {
  num_rows_returned_ = 0;
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Reset(state, row_batch));
  }
  return Status::OK();
}

void ExecNode::Close(RuntimeState* state) {
  if (is_closed_) return;
  is_closed_ = true;

  if (rows_returned_counter_ != NULL) {
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  }
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Close(state);
  }

  ScalarExprEvaluator::Close(conjunct_evals_, state);
  if (expr_perm_pool() != nullptr) expr_perm_pool_->FreeAll();
  if (expr_results_pool() != nullptr) expr_results_pool_->FreeAll();
  reservation_manager_.Close(state);
  if (expr_mem_tracker_ != nullptr) expr_mem_tracker_->Close();
  if (mem_tracker_ != nullptr) {
    if (mem_tracker()->consumption() != 0) {
      LOG(WARNING) << "Query " << PrintId(state->query_id()) << " may have leaked memory."
          << endl << state->instance_mem_tracker()->LogUsage(MemTracker::UNLIMITED_DEPTH);
      DCHECK_EQ(mem_tracker()->consumption(), 0)
          << "Leaked memory." << endl
          << state->instance_mem_tracker()->LogUsage(MemTracker::UNLIMITED_DEPTH);
    }
    mem_tracker_->Close();
  }

  for (const string& codegen_msg : plan_node_.codegen_status_msgs_) {
    runtime_profile_->AppendExecOption(codegen_msg);
  }

  if (events_ != nullptr) events_->MarkEvent("Closed");
}

Status ExecNode::CreateTree(RuntimeState* state, const PlanNode& plan_node,
    const DescriptorTbl& descs, ExecNode** root) {
  RETURN_IF_ERROR(plan_node.CreateExecNode(state, root));
  DCHECK(*root != nullptr);
  for (auto& child : plan_node.children_) {
    ExecNode* child_node;
    RETURN_IF_ERROR(CreateTree(state, *child, descs, &child_node));
    DCHECK(child_node != nullptr);
    (*root)->children_.push_back(child_node);
  }

  // build up tree of profiles; add children >0 first, so that when we print
  // the profile, child 0 is printed last (makes the output more readable)
  for (int i = 1; i < (*root)->children_.size(); ++i) {
    (*root)->runtime_profile()->AddChild((*root)->children_[i]->runtime_profile());
  }
  if (!(*root)->children_.empty()) {
    (*root)->runtime_profile()->AddChild((*root)->children_[0]->runtime_profile(), false);
  }
  return Status::OK();
}

void ExecNode::SetDebugOptions(const TDebugOptions& debug_options, ExecNode* root) {
  DCHECK(debug_options.__isset.node_id);
  DCHECK(debug_options.__isset.phase);
  DCHECK(debug_options.__isset.action);
  if (debug_options.node_id == -1 || root->id_ == debug_options.node_id) {
    root->debug_options_ = debug_options;
  }
  for (int i = 0; i < root->children_.size(); ++i) {
    SetDebugOptions(debug_options, root->children_[i]);
  }
}

string ExecNode::DebugString() const {
  stringstream out;
  this->DebugString(0, &out);
  return out.str();
}

void ExecNode::DebugString(int indentation_level, stringstream* out) const {
  *out << " conjuncts=" << ScalarExpr::DebugString(conjuncts_);
  for (int i = 0; i < children_.size(); ++i) {
    *out << "\n";
    children_[i]->DebugString(indentation_level + 1, out);
  }
}

string ExecNode::label() const {
  map<int, const char*>::const_iterator i = _TPlanNodeType_VALUES_TO_NAMES.find(type_);
  string node_type_name = "UNKNOWN";
  if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
    node_type_name = i->second;
  }
  return Substitute("$0 (id=$1)", node_type_name, std::to_string(id_));
}

void ExecNode::CollectNodes(TPlanNodeType::type node_type, vector<ExecNode*>* nodes) {
  if (type_ == node_type) nodes->push_back(this);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->CollectNodes(node_type, nodes);
  }
}

void ExecNode::CollectScanNodes(vector<ExecNode*>* nodes) {
  CollectNodes(TPlanNodeType::HDFS_SCAN_NODE, nodes);
  CollectNodes(TPlanNodeType::HBASE_SCAN_NODE, nodes);
  CollectNodes(TPlanNodeType::KUDU_SCAN_NODE, nodes);
}

Status ExecNode::ExecDebugActionImpl(TExecNodePhase::type phase, RuntimeState* state) {
  DCHECK_EQ(debug_options_.phase, phase);
  if (debug_options_.action == TDebugAction::FAIL) {
    return Status(TErrorCode::INTERNAL_ERROR, "Debug Action: FAIL");
  } else if (debug_options_.action == TDebugAction::WAIT) {
    // See DebugOptions::DebugOptions().
    DCHECK(phase != TExecNodePhase::PREPARE && phase != TExecNodePhase::CLOSE);
    while (!state->is_cancelled()) {
      sleep(1);
    }
    return Status::CANCELLED;
  } else if (debug_options_.action == TDebugAction::INJECT_ERROR_LOG) {
    state->LogError(
        ErrorMsg(TErrorCode::INTERNAL_ERROR, "Debug Action: INJECT_ERROR_LOG"));
    return Status::OK();
  } else if (debug_options_.action == TDebugAction::MEM_LIMIT_EXCEEDED) {
    return MemTracker::MemLimitExceeded(
        mem_tracker(), state, "Debug Action: MEM_LIMIT_EXCEEDED");
  } else if (debug_options_.action == TDebugAction::SET_DENY_RESERVATION_PROBABILITY) {
    // We can only enable the debug action if the buffer pool client is registered.
    // If the buffer client is not registered at this point (e.g. if phase is PREPARE or
    // OPEN), then we will enable the debug action at the time when the client is
    // registered.
    if (reservation_manager_.buffer_pool_client()->is_registered()) {
      RETURN_IF_ERROR(reservation_manager_.EnableDenyReservationDebugAction());
    }
  } else {
    DCHECK_EQ(debug_options_.action, TDebugAction::DELAY);
    int64_t sleep_duration_ms = 100;
    if (!debug_options_.action_param.empty()) {
      const string& param = debug_options_.action_param;
      StringParser::ParseResult result;
      sleep_duration_ms =
          StringParser::StringToInt<int64_t>(param.c_str(), param.length(), &result);
      if (result != StringParser::PARSE_SUCCESS || sleep_duration_ms < 0) {
        return Status(Substitute("Invalid sleep duration: '$0'. "
              "Only non-negative numbers are allowed.", param));
      }
    }
    VLOG(1) << "DEBUG_ACTION: Sleeping for " << sleep_duration_ms << "ms";
    SleepForMs(sleep_duration_ms);
  }
  return Status::OK();
}

bool ExecNode::CheckLimitAndTruncateRowBatchIfNeeded(RowBatch* row_batch, bool* eos) {
  DCHECK(limit_ != 0);
  const int row_batch_size = row_batch->num_rows();
  const bool reached_limit =
      !(limit_ == -1 || (rows_returned() + row_batch_size) < limit_);
  const int num_rows_to_consume =
      !reached_limit ? row_batch_size : limit_ - rows_returned();
  IncrementNumRowsReturned(num_rows_to_consume);
  if (reached_limit) {
    row_batch->set_num_rows(num_rows_to_consume);
    *eos = true;
  }
  return reached_limit;
}

bool ExecNode::CheckLimitAndTruncateRowBatchIfNeededShared(
    RowBatch* row_batch, bool* eos) {
  DCHECK(limit_ != 0);
  const int row_batch_size = row_batch->num_rows();
  const bool reached_limit =
      !(limit_ == -1 || (rows_returned_shared() + row_batch_size) < limit_);
  const int num_rows_to_consume =
      !reached_limit ? row_batch_size : limit_ - rows_returned_shared();
  IncrementNumRowsReturnedShared(num_rows_to_consume);
  if (reached_limit) {
    row_batch->set_num_rows(num_rows_to_consume);
    *eos = true;
  }
  return reached_limit;
}

Status ExecNode::QueryMaintenance(RuntimeState* state) {
  expr_results_pool_->Clear();
  return state->CheckQueryState();
}

// Codegen for EvalConjuncts.  The generated signature is the same as EvalConjuncts().
//
// For a node with two conjunct predicates:
//
// define i1 @EvalConjuncts(%"class.impala::ScalarExprEvaluator"** %evals, i32 %num_evals,
//                          %"class.impala::TupleRow"* %row) #34 {
// entry:
//   %eval_ptr = getelementptr inbounds %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %evals, i32 0
//   %eval = load %"class.impala::ScalarExprEvaluator"*,
//       %"class.impala::ScalarExprEvaluator"** %eval_ptr
//   %result = call i16 @"impala::Operators::Eq_BigIntVal_BigIntValWrapper"(
//       %"class.impala::ScalarExprEvaluator"* %eval, %"class.impala::TupleRow"* %row)
//   %is_null = trunc i16 %result to i1
//   %0 = ashr i16 %result, 8
//   %1 = trunc i16 %0 to i8
//   %val = trunc i8 %1 to i1
//   %is_false = xor i1 %val, true
//   %return_false = or i1 %is_null, %is_false
//   br i1 %return_false, label %false, label %continue
//
// continue:                                         ; preds = %entry
//  %eval_ptr2 = getelementptr inbounds %"class.impala::ScalarExprEvaluator"*,
//      %"class.impala::ScalarExprEvaluator"** %evals, i32 1
//  %eval3 = load %"class.impala::ScalarExprEvaluator"*,
//      %"class.impala::ScalarExprEvaluator"** %eval_ptr2
//  %result4 = call i16 @"impala::Operators::Eq_StringVal_StringValWrapper"(
//      %"class.impala::ScalarExprEvaluator"* %eval3, %"class.impala::TupleRow"* %row)
//  %is_null5 = trunc i16 %result4 to i1
//  %2 = ashr i16 %result4, 8
//  %3 = trunc i16 %2 to i8
//  %val6 = trunc i8 %3 to i1
//  %is_false7 = xor i1 %val6, true
//  %return_false8 = or i1 %is_null5, %is_false
//  br i1 %return_false8, label %false, label %continue1
//
// continue1:                                        ; preds = %continue
//  ret i1 true
//
// false:                                            ; preds = %continue, %entry
//  ret i1 false
// }
//
Status ExecNode::CodegenEvalConjuncts(LlvmCodeGen* codegen,
    const vector<ScalarExpr*>& conjuncts, llvm::Function** fn, const char* name) {
  llvm::Function* conjunct_fns[conjuncts.size()];
  for (int i = 0; i < conjuncts.size(); ++i) {
    RETURN_IF_ERROR(conjuncts[i]->GetCodegendComputeFn(codegen, false, &conjunct_fns[i]));
    if (i >= LlvmCodeGen::CODEGEN_INLINE_EXPRS_THRESHOLD) {
      // Avoid bloating EvalConjuncts by inlining everything into it.
      codegen->SetNoInline(conjunct_fns[i]);
    }
  }

  // Construct function signature to match
  // bool EvalConjuncts(ScalarExprEvaluator**, int, TupleRow*)
  llvm::PointerType* tuple_row_ptr_type = codegen->GetStructPtrType<TupleRow>();
  llvm::Type* eval_type = codegen->GetStructType<ScalarExprEvaluator>();

  LlvmCodeGen::FnPrototype prototype(codegen, name, codegen->bool_type());
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("evals", codegen->GetPtrPtrType(eval_type)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("num_evals", codegen->i32_type()));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));

  LlvmBuilder builder(codegen->context());
  llvm::Value* args[3];
  *fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* evals_arg = args[0];
  llvm::Value* tuple_row_arg = args[2];

  if (conjuncts.size() > 0) {
    llvm::LLVMContext& context = codegen->context();
    llvm::BasicBlock* false_block = llvm::BasicBlock::Create(context, "false", *fn);

    for (int i = 0; i < conjuncts.size(); ++i) {
      llvm::BasicBlock* true_block =
          llvm::BasicBlock::Create(context, "continue", *fn, false_block);
      llvm::Value* eval_arg_ptr = builder.CreateInBoundsGEP(
          NULL, evals_arg, codegen->GetI32Constant(i), "eval_ptr");
      llvm::Value* eval_arg = builder.CreateLoad(eval_arg_ptr, "eval");

      // Call conjunct_fns[i]
      CodegenAnyVal result = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
          conjuncts[i]->type(), conjunct_fns[i], {eval_arg, tuple_row_arg},
          "result");

      // Return false if result.is_null || !result
      llvm::Value* is_null = result.GetIsNull();
      llvm::Value* is_false = builder.CreateNot(result.GetVal(), "is_false");
      llvm::Value* return_false = builder.CreateOr(is_null, is_false, "return_false");
      builder.CreateCondBr(return_false, false_block, true_block);

      // Set insertion point for continue/end
      builder.SetInsertPoint(true_block);
    }
    builder.CreateRet(codegen->true_value());

    builder.SetInsertPoint(false_block);
    builder.CreateRet(codegen->false_value());
  } else {
    builder.CreateRet(codegen->true_value());
  }

  // Avoid inlining EvalConjuncts into caller if it is large.
  if (conjuncts.size() > LlvmCodeGen::CODEGEN_INLINE_EXPR_BATCH_THRESHOLD) {
    codegen->SetNoInline(*fn);
  }

  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == NULL) {
    return Status("ExecNode::CodegenEvalConjuncts(): codegen'd EvalConjuncts() function "
                  "failed verification, see log");
  }
  return Status::OK();
}
}
