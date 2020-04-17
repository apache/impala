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


#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <memory>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/types.h"

namespace impala {

struct LibCacheEntry;
class ObjectPool;
class RuntimeState;
class ScalarExpr;
class TExpr;
class TExprNode;
class Tuple;
class TupleDescriptor;

/// --- Expr overview
///
/// Expr class represents expression embedded in various operators in a query plan
/// fragment in the backend. For example, it can be the join expressions in a PHJ
/// node, predicates in a scan node or the aggregate functions in a PAGG node.
///
/// There are two subclasses of Expr: ScalarExpr for scalar expressions and AggFn for
/// aggregate functions; A scalar expression computes a value over a single row while
/// an aggregate function computes a value over a set of rows. ScalarExpr is further
/// subclassed into various expressions such as Literal and ScalarFnCall to name two
/// examples.
///
/// Expr is internally represented as a tree of nodes. The root node can be either a
/// ScalarExpr or an AggFn node and all descendants are ScalarExpr nodes. Exprs and their
/// subclasses contain query compile-time information and the code to evaluate the exprs
/// (represented by the specific class). ScalarExprEvaluator and AggFnEvluator are the
/// evaluators for ScalarExpr and AggFn respectively. They contain the general runtime
/// state needed for the actual evaluation. They don't need to be subclassed because the
/// expr-specific code sits in the expr subclasses. An Expr can be shared by multiple
/// evaluators.
///
/// Please see the headers of ScalarExpr and AggFn for further details.
///
class Expr {
 public:
  const std::string& function_name() const { return fn_.name.function_name; }

  virtual ~Expr();

  /// Returns true if the given Expr is an AggFn. Overridden by AggFn.
  virtual bool IsAggFn() const { return false; }

  ScalarExpr* GetChild(int i) const { return children_[i]; }
  int GetNumChildren() const { return children_.size(); }

  const ColumnType& type() const { return type_; }
  const std::vector<ScalarExpr*>& children() const { return children_; }

  /// Returns the backing tuple descriptor for collection types.
  virtual const TupleDescriptor* GetCollectionTupleDesc() const {
    DCHECK(false);
    return nullptr;
  }

  /// Releases cache entries to LibCache in all nodes of the Expr tree.
  virtual void Close();

  /// Implemeneted by subclasses to provide debug string information about the expr.
  virtual std::string DebugString() const = 0;

  static const char* LLVM_CLASS_NAME;

 protected:
  /// Constructs an Expr tree from the thrift Expr 'texpr'. 'root' is the root of the
  /// Expr tree created from texpr.nodes[0] by the caller (either ScalarExpr or AggFn).
  /// The newly created Expr nodes are added to 'pool'. Returns error status on failure.
  static Status CreateTree(const TExpr& texpr, ObjectPool* pool, Expr* root);

  Expr(const ColumnType& type);
  Expr(const TExprNode& node);

  /// Cache entry for the UDF or UDAF loaded from the library. Used by AggFn and
  /// some ScalarExpr such as ScalarFnCall. NULL if it's not used.
  LibCacheEntry* cache_entry_ = nullptr;

  /// The thrift function. Set only for AggFn and some ScalarExpr such as ScalarFnCall.
  TFunction fn_;

  /// Return type of the expression.
  const ColumnType type_;

  /// Sub-expressions of this expression tree.
  std::vector<ScalarExpr*> children_;

 private:
  friend class ExprTest;
  friend class ExprCodegenTest;

  /// Creates an expression tree rooted at 'root' via depth-first traversal.
  /// Called recursively to create children expr trees for sub-expressions.
  ///
  /// parameters:
  ///   nodes: vector of thrift expression nodes to be unpacked.
  ///          It is essentially an Expr tree encoded in a depth-first manner.
  ///   pool: Object pool in which Expr created from nodes are stored.
  ///   root: root of the new tree. Created and initialized by the caller.
  ///   child_node_idx: index into 'nodes' to be unpacked. It's the root of the next child
  ///                   child Expr tree to be added to 'root'. Updated as 'nodes' are
  ///                   consumed to construct the tree.
  /// return
  ///   status.ok() if successful
  ///   !status.ok() if tree is inconsistent or corrupt
  static Status CreateTreeInternal(const std::vector<TExprNode>& nodes,
      ObjectPool* pool, Expr* parent, int* child_node_idx);
};

}

#endif
