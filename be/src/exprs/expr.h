// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/string-value.h"

namespace impala {

class ObjectPool;
class RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;

// The materialized value returned by Expr::GetValue().
struct ExprValue {
  bool bool_val;
  char tinyint_val;
  short smallint_val;
  int int_val;
  long bigint_val;
  float float_val;
  double double_val;
  std::string string_data;
  StringValue string_val;

  ExprValue()
    : bool_val(false),
      tinyint_val(0),
      smallint_val(0),
      int_val(0),
      bigint_val(0),
      float_val(0.0),
      double_val(0.0),
      string_data(),
      string_val(NULL, 0) {
  }

  ExprValue(bool v): bool_val(v) {}
  ExprValue(char v): tinyint_val(v) {}
  ExprValue(short v): smallint_val(v) {}
  ExprValue(int v): int_val(v) {}
  ExprValue(float v): float_val(v) {}
  ExprValue(double v): double_val(v) {}

  // c'tor for string values
  ExprValue(const std::string& str)
    : string_data(str),
      string_val(const_cast<char*>(string_data.data()), string_data.size()) {
  }

  void SetStringVal(const std::string& str) {
    string_data = str;
    string_val.ptr = const_cast<char*>(string_data.data());
    string_val.len = string_data.size();
  }
};

// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  // Prepare for evaluation. In particular, set compute_function_.
  // TODO: make function pure, default for now to make compile
  virtual void Prepare(RuntimeState* state) {}

  // Evaluate expr and return pointer to result. The result is
  // valid as long as 'row' doesn't change.
  void* GetValue(TupleRow* row);

  // Convenience function: extract value into col_val and sets the
  // appropriate __isset flag.
  // If the value is NULL, nothing is set.
  // If 'as_ascii' is true, writes the value in ascii into stringVal;
  // if it is false, the specific field in col_val that receives the value is
  // based on the type of the expr:
  // TYPE_BOOLEAN: boolVal
  // TYPE_TINYINT/SMALLINT/INT: intVal
  // TYPE_BIGINT: longVal
  // TYPE_FLOAT/DOUBLE: doubleVal
  // TYPE_STRING: stringVal
  void GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val);

  // Convenience function: print value into 'str'.
  // NULL turns into an empty string.
  void PrintValue(TupleRow* row, std::string* str);

  void AddChild(Expr* expr) { children_.push_back(expr); }

  PrimitiveType type() const { return type_; }

  const std::vector<Expr*>& children() const { return children_; }

  // Create expression tree from the list of nodes contained in texpr
  // within 'pool'. Returns root of expression tree in 'root_expr'.
  // Returns OK if successful, otherwise an error.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr);

  // Creates vector of exprs from given vector of TExprs within 'pool'.
  // Returns an error if any of the individual conversions caused an error,
  // otherwise OK.
  static Status CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                std::vector<Expr*>* exprs);

 protected:
  friend class GetValueFunctions;

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
  ExprValue result_;

 private:
  // Create a new Expr based on texpr_node.node_type within 'pool'.
  static Status CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr);

  // Creates an expr tree for the node rooted at 'node_idx' via depth-first traversal.
  // parameters
  //   nodes: vector of thrift expression nodes to be translated
  //   parent: parent of node at node_idx (or NULL for node_idx == 0)
  //   node_idx:
  //     in: root of TExprNode tree
  //     out: next node in 'nodes' that isn't part of tree
  //   root_expr: out: root of constructed expr tree
  // return
  //   status.ok() if successful
  //   !status.ok() if tree is inconsistent or corrupt
  static Status CreateTreeFromThrift(ObjectPool* pool,
      const std::vector<TExprNode>& nodes, Expr* parent, int* node_idx,
      Expr** root_expr);

  void PrintValue(void* value, std::string* str);
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
