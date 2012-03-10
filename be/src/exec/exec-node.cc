// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/exec-node.h"

#include <sstream>
#include <glog/logging.h>

#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exec/aggregation-node.h"
#include "exec/hash-join-node.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hbase-scan-node.h"
#include "exec/exchange-node.h"
#include "exec/topn-node.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;

namespace impala {

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : id_(tnode.node_id),
    pool_(pool),
    row_descriptor_(descs, tnode.row_tuples),
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

  PrepareConjuncts(state);
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
  std::stringstream error_msg;
  switch (tnode.node_type) {
    case TPlanNodeType::HDFS_TEXT_SCAN_NODE:
    case TPlanNodeType::HDFS_RCFILE_SCAN_NODE:
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
  std::stringstream out;
  this->DebugString(0, &out);
  return out.str();
}

void ExecNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << " conjuncts=" << Expr::DebugString(conjuncts_);
  for (int i = 0; i < children_.size(); ++i) {
    *out << "\n";
    children_[i]->DebugString(indentation_level + 1, out);
  }
}

void ExecNode::PrepareConjuncts(RuntimeState* state) {
  for (vector<Expr*>::iterator i = conjuncts_.begin(); i != conjuncts_.end(); ++i) {
    (*i)->Prepare(state, row_desc());
  }
}

bool ExecNode::EvalConjuncts(TupleRow* row) {
  return EvalConjuncts(conjuncts_, row);
}

bool ExecNode::EvalConjuncts(const vector<Expr*>& conjuncts, TupleRow* row) {
  for (vector<Expr*>::const_iterator i = conjuncts.begin(); i != conjuncts.end(); ++i) {
    void* value = (*i)->GetValue(row);
    if (value == NULL || *reinterpret_cast<bool*>(value) == false) return false;
  }
  return true;
}

void ExecNode::CollectScanNodes(std::vector<ExecNode*>* scan_nodes) {
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
