// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/exec-node.h"

#include <sstream>
#include <glog/logging.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exec/aggregation-node.h"
#include "exec/hash-join-node.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hbase-scan-node.h"
#include "exec/exchange-node.h"
#include "exec/merge-node.h"
#include "exec/topn-node.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace llvm;
using namespace std;

namespace impala {

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : id_(tnode.node_id),
    pool_(pool),
    row_descriptor_(descs, tnode.row_tuples, tnode.nullable_tuples),
    limit_(tnode.limit),
    num_rows_returned_(0) {
  Status status = Expr::CreateExprTrees(pool, tnode.conjuncts, &conjuncts_);
  DCHECK(status.ok())
      << "ExecNode c'tor: deserialization of conjuncts failed:\n"
      << status.GetErrorMsg();
  InitRuntimeProfile(PrintPlanNodeType(tnode.node_type));
}

Status ExecNode::Prepare(RuntimeState* state) {
  DCHECK(runtime_profile_.get() != NULL);
  rows_returned_counter_ =
      ADD_COUNTER(runtime_profile_, "RowsReturned", TCounterType::UNIT);
  memory_used_counter_ =
      ADD_COUNTER(runtime_profile_, "MemoryUsed", TCounterType::BYTES);

  RETURN_IF_ERROR(PrepareConjuncts(state));
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Prepare(state));
  }
  return Status::OK;
}

Status ExecNode::Close(RuntimeState* state) {
  COUNTER_UPDATE(rows_returned_counter_, num_rows_returned_);
  return Status::OK;
}

Status ExecNode::CreateTree(ObjectPool* pool, const TPlan& plan,
                            const DescriptorTbl& descs, ExecNode** root) {
  if (plan.nodes.size() == 0) {
    *root = NULL;
    return Status::OK;
  }
  int node_idx = 0;
  RETURN_IF_ERROR(CreateTreeHelper(pool, plan.nodes, descs, NULL, &node_idx, root));
  if (node_idx + 1 != plan.nodes.size()) {
    // TODO: print thrift msg for diagnostic purposes.
    return Status(
        "Plan tree only partially reconstructed. Not all thrift nodes were used.");
  }
  return Status::OK;
}

Status ExecNode::CreateTreeHelper(
    ObjectPool* pool,
    const vector<TPlanNode>& tnodes,
    const DescriptorTbl& descs,
    ExecNode* parent,
    int* node_idx,
    ExecNode** root) {
  // propagate error case
  if (*node_idx >= tnodes.size()) {
    // TODO: print thrift msg
    return Status("Failed to reconstruct plan tree from thrift.");
  }
  int num_children = tnodes[*node_idx].num_children;
  ExecNode* node = NULL;
  RETURN_IF_ERROR(CreateNode(pool, tnodes[*node_idx], descs, &node));
  // assert(parent != NULL || (node_idx == 0 && root_expr != NULL));
  if (parent != NULL) {
    parent->children_.push_back(node);
    parent->runtime_profile()->AddChild(node->runtime_profile());
  } else {
    *root = node;
  }
  for (int i = 0; i < num_children; i++) {
    ++*node_idx;
    RETURN_IF_ERROR(CreateTreeHelper(pool, tnodes, descs, node, node_idx, NULL));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= tnodes.size()) {
      // TODO: print thrift msg
      return Status("Failed to reconstruct plan tree from thrift.");
    }
  }
  return Status::OK;
}

Status ExecNode::CreateNode(ObjectPool* pool, const TPlanNode& tnode,
                            const DescriptorTbl& descs, ExecNode** node) {
  stringstream error_msg;
  switch (tnode.node_type) {
    case TPlanNodeType::HDFS_SCAN_NODE:
      *node = pool->Add(new HdfsScanNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::HBASE_SCAN_NODE:
      *node = pool->Add(new HBaseScanNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::AGGREGATION_NODE:
      *node = pool->Add(new AggregationNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::HASH_JOIN_NODE:
      *node = pool->Add(new HashJoinNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::EXCHANGE_NODE:
      *node = pool->Add(new ExchangeNode(pool, tnode, descs));
      return Status::OK;
    case TPlanNodeType::SORT_NODE:
      if (tnode.sort_node.use_top_n) {
        *node = pool->Add(new TopNNode(pool, tnode, descs));
      } else {
      // TODO: Need Sort Node
      //  *node = pool->Add(new SortNode(pool, tnode, descs));
        error_msg << "ORDER BY with no LIMIT not implemented";
        return Status(error_msg.str());
      }
      return Status::OK;
    case TPlanNodeType::MERGE_NODE:
      *node = pool->Add(new MergeNode(pool, tnode, descs));
      return Status::OK;
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
  return Status::OK;
}

string ExecNode::DebugString() const {
  stringstream out;
  this->DebugString(0, &out);
  return out.str();
}

void ExecNode::DebugString(int indentation_level, stringstream* out) const {
  *out << " conjuncts=" << Expr::DebugString(conjuncts_);
  for (int i = 0; i < children_.size(); ++i) {
    *out << "\n";
    children_[i]->DebugString(indentation_level + 1, out);
  }
}

Status ExecNode::PrepareConjuncts(RuntimeState* state) {
  for (vector<Expr*>::iterator i = conjuncts_.begin(); i != conjuncts_.end(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare(*i, state, row_desc()));
  }
  return Status::OK;
}

bool ExecNode::EvalConjuncts(Expr* const* exprs, int num_exprs, TupleRow* row) {
  for (int i = 0; i < num_exprs; ++i) {
    void* value = exprs[i]->GetValue(row);
    if (value == NULL || *reinterpret_cast<bool*>(value) == false) return false;
  }
  return true;
}

// Codegen for EvalConjuncts.  The generated signature is
// For a node with two conjunct predicates
// define i1 @EvalConjuncts(%"class.impala::Expr"** %exprs, i32 %num_exprs, 
//                          %"class.impala::TupleRow"* %row) {
// entry:
//   %null_ptr = alloca i1
//   %0 = bitcast %"class.impala::TupleRow"* %row to i8**
//   %eval = call i1 @BinaryPredicate(i8** %0, i8* null, i1* %null_ptr)
//   br i1 %eval, label %continue, label %false
// 
// continue:                                         ; preds = %entry
//   %eval2 = call i1 @BinaryPredicate3(i8** %0, i8* null, i1* %null_ptr)
//   br i1 %eval2, label %continue1, label %false
// 
// continue1:                                        ; preds = %continue
//   ret i1 true
// 
// false:                                            ; preds = %continue, %entry
//   ret i1 false
// }
Function* ExecNode::CodegenEvalConjuncts(LlvmCodeGen* codegen,
    const vector<Expr*>& conjuncts) {
  for (int i = 0; i < conjuncts.size(); ++i) {
    if (conjuncts[i]->codegen_fn() == NULL) {
      VLOG(1) << "Could not codegen EvalConjuncts because one of the conjuncts "
              << "could not be codegen'd.";
      return NULL;
    }
  }

  // Construct function signature to match
  // bool EvalConjuncts(Expr** exprs, int num_exprs, TupleRow* row)
  Type* tuple_row_type = codegen->GetType(TupleRow::LLVM_CLASS_NAME);
  Type* expr_type = codegen->GetType(Expr::LLVM_CLASS_NAME);

  DCHECK(tuple_row_type != NULL);
  DCHECK(expr_type != NULL);
  
  PointerType* tuple_row_ptr_type = PointerType::get(tuple_row_type, 0);
  PointerType* expr_ptr_type = PointerType::get(expr_type, 0);

  LlvmCodeGen::FnPrototype prototype(
      codegen, "EvalConjuncts", codegen->GetType(TYPE_BOOLEAN));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("exprs", PointerType::get(expr_ptr_type, 0)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("num_exprs", codegen->GetType(TYPE_INT)));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("row", tuple_row_ptr_type));
  
  LlvmCodeGen::LlvmBuilder builder(codegen->context());
  Value* args[3];
  Function* fn = prototype.GeneratePrototype(&builder, args);
  // Other args are unused. 
  Value* tuple_row_arg = args[2];

  if (conjuncts.size() > 0) {
    LLVMContext& context = codegen->context();
    // The exprs type TupleRows as char** (instead of TupleRow* or Tuple**).  We
    // could plumb the expr codegen to know the tuples it will operate on.
    // TODO: think about doing that
    Type* tuple_row_llvm_type = PointerType::get(codegen->ptr_type(), 0);
    tuple_row_arg = builder.CreateBitCast(tuple_row_arg, tuple_row_llvm_type);
    BasicBlock* false_block = BasicBlock::Create(context, "false", fn);
  
    LlvmCodeGen::NamedVariable null_var("null_ptr", codegen->boolean_type());
    Value* is_null_ptr = codegen->CreateEntryBlockAlloca(fn, null_var);

    for (int i = 0; i < conjuncts.size(); ++i) {
      BasicBlock* true_block = BasicBlock::Create(context, "continue", fn, false_block);
      Function* conjunct_fn = conjuncts[i]->codegen_fn();
      DCHECK_EQ(conjuncts[i]->scratch_buffer_size(), 0);
      Value* expr_args[] = { tuple_row_arg, codegen->null_ptr_value(), is_null_ptr };

      // Ignore null result.  If null, expr's will return false which
      // is exactly the semantics for conjuncts
      Value* eval = builder.CreateCall(conjunct_fn, expr_args, "eval");
      builder.CreateCondBr(eval, true_block, false_block);

      // Set insertion point for continue/end
      builder.SetInsertPoint(true_block);
    }
    builder.CreateRet(codegen->true_value());

    builder.SetInsertPoint(false_block);
    builder.CreateRet(codegen->false_value());
  } else {
    builder.CreateRet(codegen->true_value());
  }

  return codegen->FinalizeFunction(fn);
}

void ExecNode::CollectScanNodes(vector<ExecNode*>* scan_nodes) {
  if (IsScanNode()) scan_nodes->push_back(this);
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->CollectScanNodes(scan_nodes);
  }
}

void ExecNode::InitRuntimeProfile(const string& name) {
  stringstream ss;
  ss << name << "(id=" << id_ << ")";
  runtime_profile_.reset(new RuntimeProfile(pool_, ss.str()));
}

}
