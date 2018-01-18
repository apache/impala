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
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exec/analytic-eval-node.h"
#include "exec/data-source-scan-node.h"
#include "exec/empty-set-node.h"
#include "exec/exchange-node.h"
#include "exec/hbase-scan-node.h"
#include "exec/hdfs-scan-node-mt.h"
#include "exec/hdfs-scan-node.h"
#include "exec/kudu-scan-node-mt.h"
#include "exec/kudu-scan-node.h"
#include "exec/kudu-util.h"
#include "exec/nested-loop-join-node.h"
#include "exec/partial-sort-node.h"
#include "exec/partitioned-aggregation-node.h"
#include "exec/partitioned-hash-join-node.h"
#include "exec/select-node.h"
#include "exec/singular-row-src-node.h"
#include "exec/sort-node.h"
#include "exec/subplan-node.h"
#include "exec/topn-node.h"
#include "exec/union-node.h"
#include "exec/unnest-node.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/initial-reservations.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"
#include "util/string-parser.h"

#include "common/names.h"

using strings::Substitute;

DECLARE_int32(be_port);
DECLARE_string(hostname);

namespace impala {

const string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

int ExecNode::GetNodeIdFromProfile(RuntimeProfile* p) {
  return p->metadata();
}

ExecNode::RowBatchQueue::RowBatchQueue(int max_batches)
  : BlockingQueue<unique_ptr<RowBatch>>(max_batches) {
}

ExecNode::RowBatchQueue::~RowBatchQueue() {
  DCHECK(cleanup_queue_.empty());
}

void ExecNode::RowBatchQueue::AddBatch(unique_ptr<RowBatch> batch) {
  if (!BlockingPut(move(batch))) {
    lock_guard<SpinLock> l(lock_);
    cleanup_queue_.push_back(move(batch));
  }
}

unique_ptr<RowBatch> ExecNode::RowBatchQueue::GetBatch() {
  unique_ptr<RowBatch> result;
  if (BlockingGet(&result)) return result;
  return unique_ptr<RowBatch>();
}

void ExecNode::RowBatchQueue::Cleanup() {
  unique_ptr<RowBatch> batch = NULL;
  while ((batch = GetBatch()) != NULL) {
    batch.reset();
  }

  lock_guard<SpinLock> l(lock_);
  cleanup_queue_.clear();
}

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : id_(tnode.node_id),
    type_(tnode.node_type),
    pool_(pool),
    row_descriptor_(descs, tnode.row_tuples, tnode.nullable_tuples),
    resource_profile_(tnode.resource_profile),
    debug_phase_(TExecNodePhase::INVALID),
    debug_action_(TDebugAction::WAIT),
    limit_(tnode.limit),
    num_rows_returned_(0),
    runtime_profile_(RuntimeProfile::Create(pool_,
        Substitute("$0 (id=$1)", PrintPlanNodeType(tnode.node_type), id_))),
    rows_returned_counter_(NULL),
    rows_returned_rate_(NULL),
    containing_subplan_(NULL),
    disable_codegen_(tnode.disable_codegen),
    is_closed_(false) {
  runtime_profile_->set_metadata(id_);
}

ExecNode::~ExecNode() {
}

Status ExecNode::Init(const TPlanNode& tnode, RuntimeState* state) {
  RETURN_IF_ERROR(
      ScalarExpr::Create(tnode.conjuncts, row_descriptor_, state, &conjuncts_));
  return Status::OK();
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
  return Status::OK();
}

void ExecNode::Codegen(RuntimeState* state) {
  DCHECK(state->ShouldCodegen());
  DCHECK(state->codegen() != NULL);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Codegen(state);
  }
}

Status ExecNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::OPEN, state));
  DCHECK_EQ(conjunct_evals_.size(), conjuncts_.size());
  return ScalarExprEvaluator::Open(conjunct_evals_, state);
}

Status ExecNode::Reset(RuntimeState* state) {
  num_rows_returned_ = 0;
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Reset(state));
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
  ScalarExpr::Close(conjuncts_);
  if (expr_perm_pool() != nullptr) expr_perm_pool_->FreeAll();
  if (expr_results_pool() != nullptr) expr_results_pool_->FreeAll();
  if (buffer_pool_client_.is_registered()) {
    VLOG_FILE << id_ << " returning reservation " << resource_profile_.min_reservation;
    state->query_state()->initial_reservations()->Return(
        &buffer_pool_client_, resource_profile_.min_reservation);
    state->exec_env()->buffer_pool()->DeregisterClient(&buffer_pool_client_);
  }
  if (expr_mem_tracker_ != nullptr) expr_mem_tracker_->Close();
  if (mem_tracker_ != nullptr) {
    if (mem_tracker()->consumption() != 0) {
      LOG(WARNING) << "Query " << state->query_id() << " may have leaked memory." << endl
          << state->instance_mem_tracker()->LogUsage(MemTracker::UNLIMITED_DEPTH);
      DCHECK_EQ(mem_tracker()->consumption(), 0)
          << "Leaked memory." << endl
          << state->instance_mem_tracker()->LogUsage(MemTracker::UNLIMITED_DEPTH);
    }
    mem_tracker_->Close();
  }
}

Status ExecNode::ClaimBufferReservation(RuntimeState* state) {
  DCHECK(!buffer_pool_client_.is_registered());
  BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
  // Check the minimum buffer size in case the minimum buffer size used by the planner
  // doesn't match this backend's.
  if (resource_profile_.__isset.spillable_buffer_size &&
      resource_profile_.spillable_buffer_size < buffer_pool->min_buffer_len()) {
    return Status(Substitute("Spillable buffer size for node $0 of $1 bytes is less "
                             "than the minimum buffer pool buffer size of $2 bytes",
        id_, resource_profile_.spillable_buffer_size, buffer_pool->min_buffer_len()));
  }

  RETURN_IF_ERROR(buffer_pool->RegisterClient(
      Substitute("$0 id=$1 ptr=$2", PrintPlanNodeType(type_), id_, this),
      state->query_state()->file_group(), state->instance_buffer_reservation(),
      mem_tracker(), resource_profile_.max_reservation, runtime_profile(),
      &buffer_pool_client_));
  VLOG_FILE << id_ << " claiming reservation " << resource_profile_.min_reservation;
  state->query_state()->initial_reservations()->Claim(
      &buffer_pool_client_, resource_profile_.min_reservation);
  if (debug_action_ == TDebugAction::SET_DENY_RESERVATION_PROBABILITY &&
      (debug_phase_ == TExecNodePhase::PREPARE || debug_phase_ == TExecNodePhase::OPEN)) {
    // We may not have been able to enable the debug action at the start of Prepare() or
    // Open() because the client is not registered then. Do it now to be sure that it is
    // effective.
    RETURN_IF_ERROR(EnableDenyReservationDebugAction());
  }
  return Status::OK();
}

Status ExecNode::ReleaseUnusedReservation() {
  return buffer_pool_client_.DecreaseReservationTo(resource_profile_.min_reservation);
}

Status ExecNode::EnableDenyReservationDebugAction() {
  DCHECK_EQ(debug_action_, TDebugAction::SET_DENY_RESERVATION_PROBABILITY);
  DCHECK(buffer_pool_client_.is_registered());
  // Parse [0.0, 1.0] probability.
  StringParser::ParseResult parse_result;
  double probability = StringParser::StringToFloat<double>(
      debug_action_param_.c_str(), debug_action_param_.size(), &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS || probability < 0.0
      || probability > 1.0) {
    return Status(Substitute(
        "Invalid SET_DENY_RESERVATION_PROBABILITY param: '$0'", debug_action_param_));
  }
  buffer_pool_client_.SetDebugDenyIncreaseReservation(probability);
  return Status::OK();
}

Status ExecNode::CreateTree(
    RuntimeState* state, const TPlan& plan, const DescriptorTbl& descs, ExecNode** root) {
  if (plan.nodes.size() == 0) {
    *root = NULL;
    return Status::OK();
  }
  int node_idx = 0;
  Status status = CreateTreeHelper(state, plan.nodes, descs, NULL, &node_idx, root);
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

Status ExecNode::CreateTreeHelper(RuntimeState* state, const vector<TPlanNode>& tnodes,
    const DescriptorTbl& descs, ExecNode* parent, int* node_idx, ExecNode** root) {
  // propagate error case
  if (*node_idx >= tnodes.size()) {
    return Status("Failed to reconstruct plan tree from thrift.");
  }
  const TPlanNode& tnode = tnodes[*node_idx];

  int num_children = tnode.num_children;
  ExecNode* node = NULL;
  RETURN_IF_ERROR(CreateNode(state->obj_pool(), tnode, descs, &node, state));
  if (parent != NULL) {
    parent->children_.push_back(node);
  } else {
    *root = node;
  }
  for (int i = 0; i < num_children; ++i) {
    ++*node_idx;
    RETURN_IF_ERROR(CreateTreeHelper(state, tnodes, descs, node, node_idx, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= tnodes.size()) {
      return Status("Failed to reconstruct plan tree from thrift.");
    }
  }

  // Call Init() after children have been set and Init()'d themselves
  RETURN_IF_ERROR(node->Init(tnode, state));

  // build up tree of profiles; add children >0 first, so that when we print
  // the profile, child 0 is printed last (makes the output more readable)
  for (int i = 1; i < node->children_.size(); ++i) {
    node->runtime_profile()->AddChild(node->children_[i]->runtime_profile());
  }
  if (!node->children_.empty()) {
    node->runtime_profile()->AddChild(node->children_[0]->runtime_profile(), false);
  }

  return Status::OK();
}

Status ExecNode::CreateNode(ObjectPool* pool, const TPlanNode& tnode,
    const DescriptorTbl& descs, ExecNode** node, RuntimeState* state) {
  stringstream error_msg;
  switch (tnode.node_type) {
    case TPlanNodeType::HDFS_SCAN_NODE:
      if (tnode.hdfs_scan_node.use_mt_scan_node) {
        DCHECK_GT(state->query_options().mt_dop, 0);
        *node = pool->Add(new HdfsScanNodeMt(pool, tnode, descs));
      } else {
        DCHECK(state->query_options().mt_dop == 0
            || state->query_options().num_scanner_threads == 1);
        *node = pool->Add(new HdfsScanNode(pool, tnode, descs));
      }
      break;
    case TPlanNodeType::HBASE_SCAN_NODE:
      *node = pool->Add(new HBaseScanNode(pool, tnode, descs));
      break;
    case TPlanNodeType::DATA_SOURCE_NODE:
      *node = pool->Add(new DataSourceScanNode(pool, tnode, descs));
      break;
    case TPlanNodeType::KUDU_SCAN_NODE:
      RETURN_IF_ERROR(CheckKuduAvailability());
      if (tnode.kudu_scan_node.use_mt_scan_node) {
        DCHECK_GT(state->query_options().mt_dop, 0);
        *node = pool->Add(new KuduScanNodeMt(pool, tnode, descs));
      } else {
        DCHECK(state->query_options().mt_dop == 0
            || state->query_options().num_scanner_threads == 1);
        *node = pool->Add(new KuduScanNode(pool, tnode, descs));
      }
      break;
    case TPlanNodeType::AGGREGATION_NODE:
      *node = pool->Add(new PartitionedAggregationNode(pool, tnode, descs));
      break;
    case TPlanNodeType::HASH_JOIN_NODE:
      *node = pool->Add(new PartitionedHashJoinNode(pool, tnode, descs));
      break;
    case TPlanNodeType::NESTED_LOOP_JOIN_NODE:
      *node = pool->Add(new NestedLoopJoinNode(pool, tnode, descs));
      break;
    case TPlanNodeType::EMPTY_SET_NODE:
      *node = pool->Add(new EmptySetNode(pool, tnode, descs));
      break;
    case TPlanNodeType::EXCHANGE_NODE:
      *node = pool->Add(new ExchangeNode(pool, tnode, descs));
      break;
    case TPlanNodeType::SELECT_NODE:
      *node = pool->Add(new SelectNode(pool, tnode, descs));
      break;
    case TPlanNodeType::SORT_NODE:
      if (tnode.sort_node.type == TSortType::PARTIAL) {
        *node = pool->Add(new PartialSortNode(pool, tnode, descs));
      } else if (tnode.sort_node.type == TSortType::TOPN) {
        *node = pool->Add(new TopNNode(pool, tnode, descs));
      } else {
        DCHECK(tnode.sort_node.type == TSortType::TOTAL);
        *node = pool->Add(new SortNode(pool, tnode, descs));
      }
      break;
    case TPlanNodeType::UNION_NODE:
      *node = pool->Add(new UnionNode(pool, tnode, descs));
      break;
    case TPlanNodeType::ANALYTIC_EVAL_NODE:
      *node = pool->Add(new AnalyticEvalNode(pool, tnode, descs));
      break;
    case TPlanNodeType::SINGULAR_ROW_SRC_NODE:
      *node = pool->Add(new SingularRowSrcNode(pool, tnode, descs));
      break;
    case TPlanNodeType::SUBPLAN_NODE:
      *node = pool->Add(new SubplanNode(pool, tnode, descs));
      break;
    case TPlanNodeType::UNNEST_NODE:
      *node = pool->Add(new UnnestNode(pool, tnode, descs));
      break;
    default:
      map<int, const char*>::const_iterator i =
          _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
      const char* str = "unknown node type";
      if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
        str = i->second;
      }
      error_msg << str << " not implemented";
      return Status(error_msg.str());
  }
  return Status::OK();
}

void ExecNode::SetDebugOptions(const TDebugOptions& debug_options, ExecNode* root) {
  DCHECK(debug_options.__isset.node_id);
  DCHECK(debug_options.__isset.phase);
  DCHECK(debug_options.__isset.action);
  if (debug_options.node_id == -1 || root->id_ == debug_options.node_id) {
    root->debug_phase_ = debug_options.phase;
    root->debug_action_ = debug_options.action;
    root->debug_action_param_ = debug_options.action_param;
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
  DCHECK_EQ(debug_phase_, phase);
  if (debug_action_ == TDebugAction::FAIL) {
    return Status(TErrorCode::INTERNAL_ERROR, "Debug Action: FAIL");
  } else if (debug_action_ == TDebugAction::WAIT) {
    while (!state->is_cancelled()) {
      sleep(1);
    }
    return Status::CANCELLED;
  } else if (debug_action_ == TDebugAction::INJECT_ERROR_LOG) {
    state->LogError(
        ErrorMsg(TErrorCode::INTERNAL_ERROR, "Debug Action: INJECT_ERROR_LOG"));
    return Status::OK();
  } else if (debug_action_ == TDebugAction::MEM_LIMIT_EXCEEDED) {
    return mem_tracker()->MemLimitExceeded(state, "Debug Action: MEM_LIMIT_EXCEEDED");
  } else {
    DCHECK_EQ(debug_action_, TDebugAction::SET_DENY_RESERVATION_PROBABILITY);
    // We can only enable the debug action right if the buffer pool client is registered.
    // If the buffer client is not registered at this point (e.g. if phase is PREPARE or
    // OPEN), then we will enable the debug action at the time when the client is
    // registered.
    if (buffer_pool_client_.is_registered()) {
      RETURN_IF_ERROR(EnableDenyReservationDebugAction());
    }
  }
  return Status::OK();
}

bool ExecNode::EvalConjuncts(
    ScalarExprEvaluator* const* evals, int num_conjuncts, TupleRow* row) {
  for (int i = 0; i < num_conjuncts; ++i) {
    if (!EvalPredicate(evals[i], row)) return false;
  }
  return true;
}

Status ExecNode::QueryMaintenance(RuntimeState* state) {
  expr_results_pool_->Clear();
  return state->CheckQueryState();
}

void ExecNode::AddCodegenDisabledMessage(RuntimeState* state) {
  if (state->CodegenDisabledByQueryOption()) {
    runtime_profile()->AddCodegenMsg(false, "disabled by query option DISABLE_CODEGEN");
  } else if (state->CodegenDisabledByHint()) {
    runtime_profile()->AddCodegenMsg(false, "disabled due to optimization hints");
  }
}

bool ExecNode::IsNodeCodegenDisabled() const {
  return disable_codegen_;
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
    RETURN_IF_ERROR(conjuncts[i]->GetCodegendComputeFn(codegen, &conjunct_fns[i]));
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
