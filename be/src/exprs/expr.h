// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/decimal-value.h"
#include "runtime/lib-cache.h"
#include "runtime/raw-value.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"

namespace llvm {
  class BasicBlock;
  class Function;
  class Type;
  class Value;
};

namespace impala {

class Expr;
class LlvmCodeGen;
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
  TimestampValue timestamp_val;
  Decimal4Value decimal4_val;
  Decimal8Value decimal8_val;
  Decimal16Value decimal16_val;

  ExprValue()
    : bool_val(false),
      tinyint_val(0),
      smallint_val(0),
      int_val(0),
      bigint_val(0),
      float_val(0.0),
      double_val(0.0),
      string_data(),
      string_val(NULL, 0),
      timestamp_val() {
  }

  ExprValue(bool v): bool_val(v) {}
  ExprValue(int8_t v): tinyint_val(v) {}
  ExprValue(int16_t v): smallint_val(v) {}
  ExprValue(int32_t v): int_val(v) {}
  ExprValue(int64_t v): bigint_val(v) {}
  ExprValue(float v): float_val(v) {}
  ExprValue(double v): double_val(v) {}
  ExprValue(int64_t t, int64_t n) : timestamp_val(t, n) {}

  // c'tor for string values
  ExprValue(const std::string& str)
    : string_data(str),
      string_val(const_cast<char*>(string_data.data()), string_data.size()) {
  }

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

  // Setter template with specializations for all types in expr-inline.h.
  // Returns the address of the set member.
  // TODO: Clean up built-in functions based on template Get() and Set().
  template<typename T> void* Set(const T& val);

  // Getter template with specializations for all types in expr-inline.h.
  // Returns the address of the set member corresponding to T,
  template<typename T> void* Get();

  // Sets the value for type to '0' and returns a pointer to the data
  void* SetToZero(const ColumnType& type) {
    switch (type.type) {
      case TYPE_NULL:
        return NULL;
      case TYPE_BOOLEAN:
        bool_val = false;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = 0;
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = 0;
        return &smallint_val;
      case TYPE_INT:
        int_val = 0;
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = 0;
        return &bigint_val;
      case TYPE_FLOAT:
        float_val = 0;
        return &float_val;
      case TYPE_DOUBLE:
        double_val = 0;
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }

  // Sets the value for type to min and returns a pointer to the data
  void* SetToMin(const ColumnType& type) {
    switch (type.type) {
      case TYPE_NULL:
        return NULL;
      case TYPE_BOOLEAN:
        bool_val = false;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = std::numeric_limits<int8_t>::min();
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = std::numeric_limits<int16_t>::min();
        return &smallint_val;
      case TYPE_INT:
        int_val = std::numeric_limits<int32_t>::min();
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = std::numeric_limits<int64_t>::min();
        return &bigint_val;
      case TYPE_FLOAT:
        // For floats and doubles, numeric_limits::min() is the smallest positive
        // representable value.
        float_val = -std::numeric_limits<float>::max();
        return &float_val;
      case TYPE_DOUBLE:
        double_val = -std::numeric_limits<double>::max();
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }

  // Sets the value for type to max and returns a pointer to the data
  void* SetToMax(const ColumnType& type) {
    switch (type.type) {
      case TYPE_NULL:
        return NULL;
      case TYPE_BOOLEAN:
        bool_val = true;
        return &bool_val;
      case TYPE_TINYINT:
        tinyint_val = std::numeric_limits<int8_t>::max();
        return &tinyint_val;
      case TYPE_SMALLINT:
        smallint_val = std::numeric_limits<int16_t>::max();
        return &smallint_val;
      case TYPE_INT:
        int_val = std::numeric_limits<int32_t>::max();
        return &int_val;
      case TYPE_BIGINT:
        bigint_val = std::numeric_limits<int64_t>::max();
        return &bigint_val;
      case TYPE_FLOAT:
        float_val = std::numeric_limits<float>::max();
        return &float_val;
      case TYPE_DOUBLE:
        double_val = std::numeric_limits<double>::max();
        return &double_val;
      default:
        DCHECK(false);
        return NULL;
    }
  }
};

// This is the superclass of all expr evaluation nodes.
//
// If codegen is enabled for the query, we will codegen as much of the expr evaluation
// as possible. This means all builtins will run through the codegen path and nothing
// (e.g. Exec nodes) will call GetValue(). Instead, they will call GetIrComputeFn().
//
// In order to call UDFs (external ones in a different binary/IR module), we need
// to use codegen to generate the wrapper to call the function, regardless of
// whether or not codegen is enabled for the query. If codegen is enabled for the
// query, the UDF will be wrapped and returned as an IR function in GetIrComputeFn().
// If codegen is disabled for the query, the UDF will be wrapped and jit compiled to
// a function that is called in GetValue().
class Expr {
 public:
  // typedef for compute functions.
  typedef void* (*ComputeFn)(Expr*, TupleRow*);

  virtual ~Expr();

  // Evaluate expr and return pointer to result. The result is
  // valid as long as 'row' doesn't change.
  // TODO: stop having the result cached in this Expr object
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
  // TYPE_TIMESTAMP: stringVal
  // Note: timestamp is converted to string via RawValue::PrintValue because HiveServer2
  // requires timestamp in a string format.
  void GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val);

  // Convenience functions: print value into 'str' or 'stream'.
  // NULL turns into "NULL".
  void PrintValue(TupleRow* row, std::string* str) {
    RawValue::PrintValue(GetValue(row), type(), output_scale_, str);
  }
  void PrintValue(void* value, std::string* str) {
    RawValue::PrintValue(value, type(), output_scale_, str);
  }
  void PrintValue(void* value, std::stringstream* stream) {
    RawValue::PrintValue(value, type(), output_scale_, stream);
  }
  void PrintValue(TupleRow* row, std::stringstream* stream) {
    RawValue::PrintValue(GetValue(row), type(), output_scale_, stream);
  }

  // Get the number of digits after the decimal that should be displayed for this
  // value. Returns -1 if no scale has been specified (currently the scale is only set for
  // doubles set by RoundUpTo). GetValue() must have already been called.
  // TODO: this will be unnecessary once we support the DECIMAL(precision, scale) type
  int output_scale() const { return output_scale_; }

  void AddChild(Expr* expr) { children_.push_back(expr); }
  Expr* GetChild(int i) const { return children_[i]; }
  int GetNumChildren() const { return children_.size(); }

  const ColumnType& type() const { return type_; }

  const std::vector<Expr*>& children() const { return children_; }

  // Returns true if expr doesn't contain slotrefs, ie, can be evaluated
  // with GetValue(NULL). The default implementation returns true if all of
  // the children are constant.
  virtual bool IsConstant() const;

  // Returns the slots that are referenced by this expr tree in 'slot_ids'.
  // Returns the number of slots added to the vector
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;

  // Create expression tree from the list of nodes contained in texpr
  // within 'pool'. Returns root of expression tree in 'root_expr'.
  // Returns OK if successful, otherwise an error.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root_expr);

  // Creates vector of exprs from given vector of TExprs within 'pool'.
  // Returns an error if any of the individual conversions caused an error,
  // otherwise OK.
  static Status CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
                                std::vector<Expr*>* exprs);

  // Prepare expr tree for evaluation, setting the compute_fn_ for each node
  // in the tree. If codegen is enabled, this function will also codegen each node
  // in the expr tree.
  // disable_codegen can be set for a particular Prepare() call to disable codegen for
  // a specific expr tree.
  // if thread_safe != NULL, it will on return, contain whether the resulting codegen
  // function is thread safe.
  static Status Prepare(Expr* root, RuntimeState* state, const RowDescriptor& row_desc,
      bool disable_codegen = true, bool* thread_safe = NULL);

  // Prepare all exprs.
  static Status Prepare(const std::vector<Expr*>& exprs, RuntimeState* state,
                        const RowDescriptor& row_desc, bool disable_codegen = true,
                        bool* thread_safe = NULL);

  // Must be called after calling Prepare(). Subclasses overriding this function should
  // call Expr::Open() to recursively call Open() on the expr tree.
  virtual Status Open(RuntimeState* state);

  // Calls Open() on every expr tree in 'exprs'.
  static Status Open(const std::vector<Expr*>& exprs, RuntimeState* state);

  // Must be called for each root expr. Subclasses overriding this function should call
  // Expr::Close().
  virtual void Close(RuntimeState* state);

  // Calls Close() on every expr tree in 'exprs'.
  static void Close(const std::vector<Expr*>& exprs, RuntimeState* state);

  // Create a new literal expr of 'type' with initial 'data'.
  // data should match the ColumnType (i.e. type == TYPE_INT, data is a int*)
  // The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type, void* data);

  // Create a new literal expr of 'type' by parsing the string.
  // NULL will be returned if the string and type are not compatible.
  // The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type,
      const std::string&);

  // Computes a memory efficient layout for storing the results of evaluating 'exprs'
  // Returns the number of bytes necessary to store all the results and offsets
  // where the result for each expr should be stored.
  // Variable length types are guaranteed to be at the end and 'var_result_begin'
  // will be set the beginning byte offset where variable length results begin.
  // 'var_result_begin' will be set to -1 if there are no variable len types.
  static int ComputeResultsLayout(const std::vector<Expr*>& exprs,
      std::vector<int>* offsets, int* var_result_begin);

  // Returns if codegen on each of the exprs is available.
  static bool IsCodegenAvailable(const std::vector<Expr*>& exprs);

  // Codegen the expr tree rooted at this node.  This does a post order traversal
  // of the expr tree and codegen's each node.  Returns NULL if the subtree cannot
  // be codegen'd.
  // Subclasses should override this function if it supports expr specific codegen,
  // otherwise it will just wrap the interpreted ComputeFn.  All exprs under this
  // one will also in turn be interpreted but codegen using this expr will be able
  // to continue.
  // All expr codegen'd functions have this signature:
  // <expr ret type> ComputeFn(int8_t** tuple_row, int8_t* scratch_buffer, bool* is_null)
  virtual llvm::Function* Codegen(LlvmCodeGen* code_gen);

  // Returns whether the subtree at this node is jittable.  This is temporary
  // until more expr types are supported.
  virtual bool IsJittable(LlvmCodeGen*) const;

  // Returns codegen function for the expr tree rooted at this expr.
  llvm::Function* codegen_fn() { return codegen_fn_; }

  // Returns the scratch buffer size needed to call codegen_fn
  int scratch_buffer_size() { return scratch_buffer_size_; }

  // Utility function to codegen a call to this expr (tree)'s codegen'd compute function.
  // This function will codegen a call instruction, a branch to check for null and a
  // jump to either the null or non-null block.
  // Returns the result of the compute function (only valid in non-null branch).
  // - caller: the code block from the calling function.  A call instruction will be
  //   added to the end of the block.
  // - args: args to call this compute function with
  // - null_block: block in caller function to jump to if the child is null
  // - not_null_block: block in caller function to jump to if the child is not null
  // - result_var_name: name of the variable for child result.  This is the name
  //   generated in the IR so it's only useful for debugging.
  llvm::Value* CodegenGetValue(LlvmCodeGen* codegen, llvm::BasicBlock* caller,
      llvm::Value** args, llvm::BasicBlock* null_block, llvm::BasicBlock* not_null_block,
      const char* result_var_name = "child_result");

  // This is a wrapper around CodegenGetValue() that is used by a parent Expr node to
  // call a child.  The parent args are used to call the child function.
  llvm::Value* CodegenGetValue(LlvmCodeGen* codegen, llvm::BasicBlock* parent,
      llvm::BasicBlock* null_block, llvm::BasicBlock* not_null_block,
      const char* result_var_name = "child_result");

  // Returns the llvm return type for this expr
  llvm::Type* GetLlvmReturnType(LlvmCodeGen* codegen) const;

  // If this expr is constant, evaluates the expr with no input row argument and returns
  // the output. Returns NULL if the argument is not constant. The returned AnyVal* is
  // owned by this expr. This should only be called after Open() has been called on this
  // expr.
  // TODO: ExprContext argument
  // TODO: Use this value directly in the codegen path
  virtual impala_udf::AnyVal* GetConstVal();

  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<Expr*>& exprs);

  // The builtin functions are not called from anywhere in the code and the
  // symbols are therefore not included in the binary. We call these functions
  // by using dlsym. The compiler must think this function is callable to
  // not strip these symbols.
  static void InitBuiltinsDummy();

  static const char* LLVM_CLASS_NAME;

 protected:
  friend class AggFnEvaluator;
  friend class CastExpr;
  friend class ComputeFunctions;
  friend class DecimalFunctions;
  friend class DecimalOperators;
  friend class MathFunctions;
  friend class StringFunctions;
  friend class TimestampFunctions;
  friend class ConditionalFunctions;
  friend class UtilityFunctions;
  friend class CaseExpr;
  friend class InPredicate;
  friend class FunctionCall;
  friend class NativeUdfExpr;

  Expr(const ColumnType& type, bool is_slotref = false);
  Expr(const TExprNode& node, bool is_slotref = false);

  // Prepare should be invoked recurisvely on the expr tree.
  // Return OK if successful, otherwise return error status.
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);

  // Helper function that just calls prepare on all the children
  // Does not do anything on the this expr.
  // Return OK if successful, otherwise return error status.
  Status PrepareChildren(RuntimeState* state, const RowDescriptor& row_desc);

  // Cache entry for the library implementing this function.
  LibCache::LibCacheEntry* cache_entry_;

  // Function description.
  TFunction fn_;
  bool is_udf_call_;

  // function to evaluate expr; typically set in Prepare()
  ComputeFn compute_fn_;

  // recognize if this node is a slotref in order to speed up GetValue()
  const bool is_slotref_;
  // analysis is done, types are fixed at this point
  const ColumnType type_;
  std::vector<Expr*> children_;
  ExprValue result_;
  int output_scale_;

  // Codegened IR function.  Will be NULL if this expr was not codegen'd.
  llvm::Function* codegen_fn_;

  // For functions that don't currently have an explicit codegen'd version,
  // we instead use an adapter, generic codegen fn, around the interpreted
  // ComputeFn.
  // Set to true if this expr or any of its children used the adapter fn.
  bool adapter_fn_used_;

  // Size of scratch buffer necessary to call codegen'd compute function.
  // TODO: not implemented, always 0
  int scratch_buffer_size_;

  // If this expr is constant, this will store and cache the value generated by
  // GetConstVal().
  boost::scoped_ptr<impala_udf::AnyVal> constant_val_;

  // Set to true after Open() has been called.
  bool opened_;

  // Returns an llvm::Function* with signature:
  // <subclass of AnyVal> ComputeFn(int8_t* context, TupleRow* row)
  //
  // The function should evaluate this expr over 'row' and return the result as the
  // appropriate type of AnyVal.
  // For now, 'context' is a placeholder and is always NULL. This will eventually be
  // replaced by an ExprContext* argument.
  //
  // The default implementation produces a wrapper around GetValue().
  virtual Status GetIrComputeFn(RuntimeState* state, llvm::Function** fn);

  // Helper function to create an empty Function* with the appropriate signature to be
  // returned by GetIrComputeFn(). 'name' is the name of the returned Function*. The
  // arguments to the function are returned in 'args'.
  llvm::Function* CreateIrFunctionPrototype(LlvmCodeGen* codegen, const std::string& name,
                                            llvm::Value* (*args)[2]);

  // Create a compute function prototype.
  // The signature is:
  // <expr ret type> ComputeFn(TupleRow* row, char* state_data, bool* is_null)
  llvm::Function* CreateComputeFnPrototype(LlvmCodeGen* codegen, const std::string& name);

  // Create dummy ret value for NULL result for this expr's return type
  llvm::Value* GetNullReturnValue(LlvmCodeGen* codegen);

  // Codegen IR to set the out is_null return arg to 'val'
  void CodegenSetIsNullArg(LlvmCodeGen* codegen, llvm::BasicBlock* block, bool val);

  // Codegen call to child compute function, passing the arguments from this
  // compute function.
  // Checks NULL return from child and will conditionally branch to the
  // null/not_null block of the parent function
  // - parent: the calling function.  A call instruction will be added to this function.
  // - child: the child function to call
  // - null_block: block in parent function to jump to if the child is null
  // - not_null_block: block in parent function to jump to if the child is not null
  llvm::Value* CodegenCallFn(LlvmCodeGen* codegen, llvm::Function* parent,
      llvm::Function* child, llvm::BasicBlock* null_block,
      llvm::BasicBlock* not_null_block);

  // Returns if the codegen function rooted at this node is thread safe.
  bool codegend_fn_thread_safe() const;

  // This is the default implementation of GetIrComputeFn and *always* returns
  // the wrapper around GetValue(), regardless of whether or not the Expr can
  // return a more optimal IR function. This forces as much of the execution as
  // possible in the interpreted path and is used when codegen has been disabled
  // for the query.
  Status GetWrapperIrComputeFunction(LlvmCodeGen* codegen, llvm::Function** fn);

 private:
  friend class ExprTest;

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

  // Update the compute function with the jitted function.
  void SetComputeFn(void* jitted_function, int scratch_size);

  // Jit compile expr tree.  Returns a function pointer to the jitted function.
  // scratch_size is an out parameter for the required size of the scratch buffer
  // to call the jitted function.
  // Returns NULL if the function is not jittable.
  llvm::Function* CodegenExprTree(LlvmCodeGen* codegen);

  // Compute function wrapper for jitted Expr trees.  This is temporary until
  // we are generating loops to evaluate batches of tuples.
  // This is a shim to convert the old ComputeFn signature to the code-
  // generated signature.  It will call the underlying jitted function and
  // stuff the result back in expr->result_.
  static void* EvalCodegendComputeFn(Expr* expr, TupleRow* row);

  // This is a function pointer to the compute function.  The return type
  // for jitted functions depends on the Expr so we need to store it as
  // a void* and then cast it on invocation.
  void* jitted_compute_fn_;
};

// Reference to a single slot of a tuple.
// We inline this here in order for Expr::GetValue() to be able
// to reference SlotRef::ComputeFn() directly.
// Splitting it up into separate .h files would require circular #includes.
class SlotRef : public Expr {
 public:
  SlotRef(const TExprNode& node);
  SlotRef(const SlotDescriptor* desc);

  // Used for testing.  GetValue will return tuple + offset interpreted as 'type'
  SlotRef(const ColumnType& type, int offset);

  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);
  static void* ComputeFn(Expr* expr, TupleRow* row);
  virtual std::string DebugString() const;
  virtual bool IsConstant() const { return false; }
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids) const;

  virtual llvm::Function* Codegen(LlvmCodeGen* codegen);

 protected:
  int tuple_idx_;  // within row
  int slot_offset_;  // within tuple
  NullIndicatorOffset null_indicator_offset_;  // within tuple
  const SlotId slot_id_;
  bool tuple_is_nullable_; // true if the tuple is nullable.
};

inline void* SlotRef::ComputeFn(Expr* expr, TupleRow* row) {
  SlotRef* ref = static_cast<SlotRef*>(expr);
  Tuple* t = row->GetTuple(ref->tuple_idx_);
  if (!ref->tuple_is_nullable_) DCHECK(t != NULL);
  if (t == NULL || t->IsNull(ref->null_indicator_offset_)) return NULL;
  return t->GetSlot(ref->slot_offset_);
}

inline void* Expr::GetValue(TupleRow* row) {
  DCHECK(type_.type != INVALID_TYPE);
  if (is_slotref_) {
    return SlotRef::ComputeFn(this, row);
  } else {
    DCHECK(compute_fn_ != NULL) << DebugString();
    return compute_fn_(this, row);
  }
}

}

#endif
