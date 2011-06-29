// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <string>

#include "common/status.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/descriptors.h"

namespace impala {

struct RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;

// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  // Prepare for evaluation.
  // TODO: make function pure, default for now to make compile
  virtual void Prepare(RuntimeState* state) {}

  // Evaluate expr and return pointer to result. The result is
  // valid as long as 'row' doesn't change.
  void* GetValue(TupleRow* row);

  // Convenience function: extract value into col_val.
  void GetValue(TupleRow* row, PrimitiveType col_type, TColumnValue* col_val);

  // Convenience function: print value into 'str'.
  void PrintValue(TupleRow* row, PrimitiveType col_type, std::string* str);

  void AddChild(Expr* expr) { children_.push_back(expr); }

  PrimitiveType type() const { return type_; }

  const std::vector<Expr*>& children() const { return children_; }

  // create expression tree from the list of nodes contained in texpr
  // returns root of expression tree
  // it is the caller's responsibility to delete the memory of the expression tree
  static Status CreateExprTree(const TExpr& texpr, Expr** root_expr);

  // delete the expression (sub-)tree rooted at expr
  static void DeleteExprTree(Expr* expr);

 protected:
  Expr(const TExprNode& node);
  Expr(const TExprNode& node, bool is_slotref);

  // function to evaluate expr; typically set in Prepare()
  typedef void* (*ComputeFunction)(Expr*, TupleRow*);
  ComputeFunction compute_function_;

  // recognize if this node is a slotref in order to speed up GetValue()
  const bool is_slotref_;
  // analysis is done, types are fixed at this point
  const PrimitiveType type_;
  std::vector<Expr*> children_;

 private:
  // create a new Expr based on texpr_node.node_type
  // caller owns the memory pointed to by Expr*
  static Status CreateExpr(const TExprNode& texpr_node, Expr** expr);

  // recursive procedure for re-creating
  // an expression tree from a dfs list of thrift-based nodes.
  // input parameters
  //   nodes: vector of thrift expression nodes to be translated
  //   node_idx: used to propagate state during recursion. points to current index into nodes vector.
  //             should be called with node_idx == 0
  //   parent: used to propagate state during recursion.
  //           parent of current expression, should be called with NULL
  // output parameter
  //   root_expr: root of expression tree to be set
  // return
  //   status.ok() if successful
  //   !status.ok() if tree is inconsistent or corrupt
  static Status CreateTreeFromThrift(const std::vector<TExprNode>& nodes,
      int* node_idx, Expr* parent, Expr** root_expr);
};

// Reference to a single slot of a tuple.
// We inline this here in order for Expr::GetValue() to be able
// to reference SlotRef::ComputeFunction() directly.
// Splitting it up into separate .h files would require circular #includes.
class SlotRef : public Expr {
 public:
  SlotRef(const TExprNode& node);

  virtual void Prepare(RuntimeState* state);
  static void* ComputeFunction(Expr* expr, TupleRow* row);

 protected:
  int tuple_idx_;  // within row
  int slot_offset_;  // within tuple
  NullIndicatorOffset null_indicator_offset_;  // within tuple
  const SlotId slot_id_;
};

inline void* SlotRef::ComputeFunction(Expr* expr, TupleRow* row) {
  // TODO: does boost have different cast for this?
  SlotRef* ref = static_cast<SlotRef*>(expr);
  Tuple* t = row->GetTuple(ref->tuple_idx_);
  if (t == NULL || t->IsNull(ref->null_indicator_offset_)) return NULL;
  return t->GetSlot(ref->slot_offset_);
}

inline void* Expr::GetValue(TupleRow* row) {
  if (is_slotref_) {
    return SlotRef::ComputeFunction(this, row);
  } else {
    return compute_function_(this, row);
  }
}

}

#endif
