// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Opcodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/string-value.h"

namespace impala {

class Expr;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;

// The materialized value returned by Expr::GetValue().
// Some exprs may set multiple fields of this value at once
// for maintaining state across evaluations.
// For example, the rand() math function uses double_val as its return value,
// and int_val as the state for the random number generator.
struct ExprValue {
  bool bool_val;
  int8_t tinyint_val;
  int16_t smallint_val;
  int32_t int_val;
  int64_t bigint_val;
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
  ExprValue(int8_t v): tinyint_val(v) {}
  ExprValue(int16_t v): smallint_val(v) {}
  ExprValue(int32_t v): int_val(v) {}
  ExprValue(int64_t v): bigint_val(v) {}
  ExprValue(float v): float_val(v) {}
  ExprValue(double v): double_val(v) {}

  // c'tor for string values
  ExprValue(const std::string& str)
    : string_data(str),
      string_val(const_cast<char*>(string_data.data()), string_data.size()) {
  }
  
  // Update this ExprValue by parsing the string and return a pointer to the result.
  // NULL will be returned if the string and type are not compatible.
  void* TryParse(const std::string& string, PrimitiveType type);

  // Set string value to copy of str
  void SetStringVal(const StringValue& str) {
    string_data = std::string(str.ptr, str.len);
    SyncStringVal();
  }

  void SetStringVal(const std::string& str) {
    string_data = str;
    SyncStringVal();
  }

  // Updates string_val ptr / len pair to reflect any changes in
  // string_data. If not called after mutating string_data,
  // string_val->ptr may point at garbage.
  void SyncStringVal() {
    string_val.ptr = const_cast<char*>(string_data.data());
    string_val.len = string_data.size();
  }
};

// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  // typedef for compute functions.
  typedef void* (*ComputeFunction)(Expr*, TupleRow*);

  // Prepare expr tree for evaluation. In particular, set compute_function_.
  // Prepare should be invoked recurisvely on the expr tree.
  // Return OK if successful, otherwise return error status.
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);

  // Evaluate expr and return pointer to result. The result is
  // valid as long as 'row' doesn't change.
  void* GetValue(TupleRow* row);

  // Convenience function: extract value into col_val and sets the
  // appropriate __isset flag.
  // If the value is NULL and as_ascii is false, nothing is set.
  // If 'as_ascii' is true, writes the value in ascii into stringVal
  // (nulls turn into "NULL");
  // if it is false, the specific field in col_val that receives the value is
  // based on the type of the expr:
  // TYPE_BOOLEAN: boolVal
  // TYPE_TINYINT/SMALLINT/INT: intVal
  // TYPE_BIGINT: longVal
  // TYPE_FLOAT/DOUBLE: doubleVal
  // TYPE_STRING: stringVal
  void GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val);

  // Convenience function: print value into 'str'.
  // NULL turns into "NULL".
  void PrintValue(TupleRow* row, std::string* str);

  void AddChild(Expr* expr) { children_.push_back(expr); }
  Expr* GetChild(int i) const { return children_[i]; }
  int GetNumChildren() const { return children_.size(); }

  PrimitiveType type() const { return type_; }
  const std::vector<Expr*>& children() const { return children_; }

  TExprOpcode::type op() const { return opcode_; }

  // Returns true if expr doesn't contain slotrefs, ie, can be evaluated
  // with GetValue(NULL). The default implementation returns true if all of
  // the children are constant.
  virtual bool IsConstant() const;

  // Create expression tree from the list of nodes contained in texpr
  // within 'pool'. Returns root of expression tree in 'root_expr'.
  // Returns OK if successful, otherwise an error.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr);

  // Creates vector of exprs from given vector of TExprs within 'pool'.
  // Returns an error if any of the individual conversions caused an error,
  // otherwise OK.
  static Status CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                std::vector<Expr*>* exprs);

  // Prepare all exprs.
  static Status Prepare(const std::vector<Expr*>& exprs, RuntimeState* state,
                        const RowDescriptor& row_desc);

  // Create a new literal expr of 'type' with initial 'data'.
  // data should match the PrimitiveType (i.e. type == TYPE_INT, data is a int*)
  // The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, PrimitiveType type, void* data);

  // Create a new literal expr of 'type' by parsing the string.
  // NULL will be returned if the string and type are not compatible.
  // The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, PrimitiveType type, const std::string&);

  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<Expr*>& exprs);

 protected:
  friend class ComputeFunctions;
  friend class MathFunctions;
  friend class StringFunctions;

  Expr(PrimitiveType type);
  Expr(const TExprNode& node);
  Expr(const TExprNode& node, bool is_slotref);

  // Helper function that just calls prepare on all the children
  // Does not do anything on the this expr.
  // Return OK if successful, otherwise return error status.
  Status PrepareChildren(RuntimeState* state, const RowDescriptor& row_desc);

  // function to evaluate expr; typically set in Prepare()
  ComputeFunction compute_function_;

  // function opcode
  TExprOpcode::type opcode_;

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

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  static void* ComputeFunction(Expr* expr, TupleRow* row);
  virtual std::string DebugString() const;
  virtual bool IsConstant() const { return false; }

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
  DCHECK(type_ != INVALID_TYPE);
  if (is_slotref_) {
    return SlotRef::ComputeFunction(this, row);
  } else {
    return compute_function_(this, row);
  }
}

}

#endif
