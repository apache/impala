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

#ifndef IMPALA_EXPRS_EXPR_CONTEXT_H
#define IMPALA_EXPRS_EXPR_CONTEXT_H

#include <boost/scoped_ptr.hpp>

#include "common/status.h"
#include "exprs/expr-value.h"
#include "udf/udf.h"
#include "udf/udf-internal.h" // for ArrayVal

using namespace impala_udf;

namespace impala {

class Expr;
class MemPool;
class MemTracker;
class RuntimeState;
class RowDescriptor;
class TColumnValue;
class TupleRow;

/// An ExprContext contains the state for the execution of a tree of Exprs, in particular
/// the FunctionContexts necessary for the expr tree. This allows for multi-threaded
/// expression evaluation, as a given tree can be evaluated using multiple ExprContexts
/// concurrently. A single ExprContext is not thread-safe.
class ExprContext {
 public:
  ExprContext(Expr* root);
  ~ExprContext();

  /// Prepare expr tree for evaluation.
  /// Allocations from this context will be counted against 'tracker'.
  Status Prepare(RuntimeState* state, const RowDescriptor& row_desc,
                 MemTracker* tracker);

  /// Must be called after calling Prepare(). Does not need to be called on clones.
  /// Idempotent (this allows exprs to be opened multiple times in subplans without
  /// reinitializing function state).
  Status Open(RuntimeState* state);

  /// Creates a copy of this ExprContext. Open() must be called first. The copy contains
  /// clones of each FunctionContext, which share the fragment-local state of the
  /// originals but have their own MemPool and thread-local state. Clone() should be used
  /// to create an ExprContext for each execution thread that needs to evaluate
  /// 'root'. Note that clones are already opened. '*new_context' must be initialized by
  /// the caller to NULL.
  Status Clone(RuntimeState* state, ExprContext** new_context);

  /// Closes all FunctionContexts. Must be called on every ExprContext, including clones.
  void Close(RuntimeState* state);

  /// Calls the appropriate Get*Val() function on this context's expr tree and stores the
  /// result in result_.
  void* GetValue(TupleRow* row);

  /// Convenience function: extract value into col_val and sets the
  /// appropriate __isset flag.
  /// If the value is NULL and as_ascii is false, nothing is set.
  /// If 'as_ascii' is true, writes the value in ascii into stringVal
  /// (nulls turn into "NULL");
  /// if it is false, the specific field in col_val that receives the value is
  /// based on the type of the expr:
  /// TYPE_BOOLEAN: boolVal
  /// TYPE_TINYINT/SMALLINT/INT: intVal
  /// TYPE_BIGINT: longVal
  /// TYPE_FLOAT/DOUBLE: doubleVal
  /// TYPE_STRING: stringVal
  /// TYPE_TIMESTAMP: stringVal
  /// Note: timestamp is converted to string via RawValue::PrintValue because HiveServer2
  /// requires timestamp in a string format.
  void GetValue(TupleRow* row, bool as_ascii, TColumnValue* col_val);

  /// Convenience functions: print value into 'str' or 'stream'.  NULL turns into "NULL".
  void PrintValue(TupleRow* row, std::string* str);
  void PrintValue(void* value, std::string* str);
  void PrintValue(void* value, std::stringstream* stream);
  void PrintValue(TupleRow* row, std::stringstream* stream);

  /// Creates a FunctionContext, and returns the index that's passed to fn_context() to
  /// retrieve the created context. Exprs that need a FunctionContext should call this in
  /// Prepare() and save the returned index. 'varargs_buffer_size', if specified, is the
  /// size of the varargs buffer in the created FunctionContext (see udf-internal.h).
  int Register(RuntimeState* state, const FunctionContext::TypeDesc& return_type,
               const std::vector<FunctionContext::TypeDesc>& arg_types,
               int varargs_buffer_size = 0);

  /// Retrieves a registered FunctionContext. 'i' is the index returned by the call to
  /// Register(). This should only be called by Exprs.
  FunctionContext* fn_context(int i) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, fn_contexts_.size());
    return fn_contexts_[i];
  }

  Expr* root() { return root_; }
  bool closed() { return closed_; }

  /// Calls Get*Val on root_
  BooleanVal GetBooleanVal(TupleRow* row);
  TinyIntVal GetTinyIntVal(TupleRow* row);
  SmallIntVal GetSmallIntVal(TupleRow* row);
  IntVal GetIntVal(TupleRow* row);
  BigIntVal GetBigIntVal(TupleRow* row);
  FloatVal GetFloatVal(TupleRow* row);
  DoubleVal GetDoubleVal(TupleRow* row);
  StringVal GetStringVal(TupleRow* row);
  ArrayVal GetArrayVal(TupleRow* row);
  TimestampVal GetTimestampVal(TupleRow* row);
  DecimalVal GetDecimalVal(TupleRow* row);

  /// Frees all local allocations made by fn_contexts_. This can be called when result
  /// data from this context is no longer needed.
  void FreeLocalAllocations();
  static void FreeLocalAllocations(const std::vector<ExprContext*>& ctxs);
  static void FreeLocalAllocations(const std::vector<FunctionContext*>& ctxs);

  static const char* LLVM_CLASS_NAME;

 private:
  friend class Expr;
  /// Users of private GetValue()
  friend class HiveUdfCall;
  friend class ScalarFnCall;

  /// FunctionContexts for each registered expression. The FunctionContexts are created
  /// and owned by this ExprContext.
  std::vector<FunctionContext*> fn_contexts_;

  /// Array access to fn_contexts_. Used by ScalarFnCall's codegen'd compute function
  /// to access the correct FunctionContext.
  /// TODO: revisit this
  FunctionContext** fn_contexts_ptr_;

  /// Pool backing fn_contexts_. Counts against the runtime state's UDF mem tracker.
  boost::scoped_ptr<MemPool> pool_;

  /// The expr tree this context is for.
  Expr* root_;

  /// Stores the result of the root expr. This is used in interpreted code when we need a
  /// void*.
  ExprValue result_;

  /// True if this context came from a Clone() call. Used to manage FunctionStateScope.
  bool is_clone_;

  /// Variables keeping track of current state.
  bool prepared_;
  bool opened_;
  bool closed_;

  /// Calls the appropriate Get*Val() function on 'e' and stores the result in result_.
  /// This is used by Exprs to call GetValue() on a child expr, rather than root_.
  void* GetValue(Expr* e, TupleRow* row);
};

}

#endif
