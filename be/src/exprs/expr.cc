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

#include "exprs/expr.h"

#include <sstream>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/scalar-expr.h"
#include "runtime/lib-cache.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"

namespace impala {

const char* Expr::LLVM_CLASS_NAME = "class.impala::Expr";

Expr::Expr(const ColumnType& type)
  : type_(type) {
}

Expr::Expr(const TExprNode& node)
  : type_(ColumnType::FromThrift(node.type)) {
  if (node.__isset.fn) fn_ = node.fn;
}

Expr::~Expr() {
  DCHECK(cache_entry_ == nullptr);
}

Status Expr::CreateTree(const TExpr& texpr, ObjectPool* pool, Expr* root) {
  DCHECK(!texpr.nodes.empty());
  DCHECK(root != nullptr);
  // The root of the tree at nodes[0] is already created and stored in 'root'.
  int child_node_idx = 0;
  int num_children = texpr.nodes[0].num_children;
  for (int i = 0; i < num_children; ++i) {
    ++child_node_idx;
    Status status = CreateTreeInternal(texpr.nodes, pool, root, &child_node_idx);
    if (UNLIKELY(!status.ok())) {
      LOG(ERROR) << "Could not construct expr tree.\n" << status.GetDetail() << "\n"
                 << apache::thrift::ThriftDebugString(texpr);
      return status;
    }
  }
  if (UNLIKELY(child_node_idx + 1 != texpr.nodes.size())) {
    return Status("Expression tree only partially reconstructed. Not all thrift " \
                  "nodes were used.");
  }
  return Status::OK();
}

Status Expr::CreateTreeInternal(const vector<TExprNode>& nodes, ObjectPool* pool,
    Expr* root, int* child_node_idx) {
  // propagate error case
  if (*child_node_idx >= nodes.size()) {
    return Status("Failed to reconstruct expression tree from thrift.");
  }

  const TExprNode& texpr_node = nodes[*child_node_idx];
  DCHECK_NE(texpr_node.node_type, TExprNodeType::AGGREGATE_EXPR);
  ScalarExpr* child_expr;
  RETURN_IF_ERROR(ScalarExpr::CreateNode(texpr_node, pool, &child_expr));
  root->children_.push_back(child_expr);

  int num_children = nodes[*child_node_idx].num_children;
  for (int i = 0; i < num_children; ++i) {
    *child_node_idx += 1;
    RETURN_IF_ERROR(CreateTreeInternal(nodes, pool, child_expr, child_node_idx));
    DCHECK(child_expr->GetChild(i) != nullptr);
  }
  return Status::OK();
}

void Expr::Close() {
  for (ScalarExpr* child : children_) child->Close();
  if (cache_entry_ != nullptr) {
    LibCache::instance()->DecrementUseCount(cache_entry_);
    cache_entry_ = nullptr;
  }
}

}
