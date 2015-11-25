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


#ifndef IMPALA_UDF_UDF_INTERNAL_H
#define IMPALA_UDF_UDF_INTERNAL_H

#include <boost/cstdint.hpp>
#include <map>
#include <string>
#include <string.h>
#include <vector>

/// Be very careful when adding Impala includes in this file. We don't want to pull
/// in unnecessary dependencies for the development libs.
#include "udf/udf.h"

namespace impala {

class FreePool;
class MemPool;
class RuntimeState;

/// This class actually implements the interface of FunctionContext. This is split to
/// hide the details from the external header.
/// Note: The actual user code does not include this file.
class FunctionContextImpl {
 public:
  /// Create a FunctionContext for a UDF. Caller is responsible for deleting it.
  static impala_udf::FunctionContext* CreateContext(RuntimeState* state, MemPool* pool,
      const impala_udf::FunctionContext::TypeDesc& return_type,
      const std::vector<impala_udf::FunctionContext::TypeDesc>& arg_types,
      int varargs_buffer_size = 0, bool debug = false);

  /// Create a FunctionContext for a UDA. Identical to the UDF version except for the
  /// intermediate type. Caller is responsible for deleting it.
  static impala_udf::FunctionContext* CreateContext(RuntimeState* state, MemPool* pool,
      const impala_udf::FunctionContext::TypeDesc& intermediate_type,
      const impala_udf::FunctionContext::TypeDesc& return_type,
      const std::vector<impala_udf::FunctionContext::TypeDesc>& arg_types,
      int varargs_buffer_size = 0, bool debug = false);

  FunctionContextImpl(impala_udf::FunctionContext* parent);

  /// Checks for any outstanding memory allocations. If there is unfreed memory, adds a
  /// warning and frees the allocations. Note that local allocations are freed with the
  /// MemPool backing pool_.
  void Close();

  /// Returns a new FunctionContext with the same constant args, fragment-local state, and
  /// debug flag as this FunctionContext. The caller is responsible for calling delete on
  /// it.
  impala_udf::FunctionContext* Clone(MemPool* pool);

  /// Allocates a buffer of 'byte_size' with "local" memory management. These
  /// allocations are not freed one by one but freed as a pool by FreeLocalAllocations()
  /// This is used where the lifetime of the allocation is clear.
  /// For UDFs, the allocations can be freed at the row level.
  /// TODO: free them at the batch level and save some copies?
  uint8_t* AllocateLocal(int byte_size);

  /// Frees all allocations returned by AllocateLocal().
  void FreeLocalAllocations();

  /// Sets constant_args_. The AnyVal* values are owned by the caller.
  void SetConstantArgs(const std::vector<impala_udf::AnyVal*>& constant_args);

  uint8_t* varargs_buffer() { return varargs_buffer_; }

  std::vector<impala_udf::AnyVal*>* staging_input_vals() { return &staging_input_vals_; }

  bool debug() { return debug_; }
  bool closed() { return closed_; }

  int64_t num_updates() const { return num_updates_; }
  int64_t num_removes() const { return num_removes_; }
  void set_num_updates(int64_t n) { num_updates_ = n; }
  void set_num_removes(int64_t n) { num_removes_ = n; }
  void IncrementNumUpdates(int64_t n = 1) { num_updates_ += n; }
  void IncrementNumRemoves(int64_t n = 1) { num_removes_ += n; }

  static const char* LLVM_FUNCTIONCONTEXT_NAME;

  RuntimeState* state() { return state_; }

 private:
  friend class impala_udf::FunctionContext;
  friend class ExprContext;

  /// Preallocated buffer for storing varargs (if the function has any). Allocated and
  /// owned by this object, but populated by an Expr function.
  //
  /// This is the first field in the class so it's easy to access in codegen'd functions.
  /// Don't move it or add fields above unless you know what you're doing.
  uint8_t* varargs_buffer_;
  int varargs_buffer_size_;

  /// Parent context object. Not owned
  impala_udf::FunctionContext* context_;

  /// Pool to service allocations from.
  FreePool* pool_;

  /// We use the query's runtime state to report errors and warnings. NULL for test
  /// contexts.
  RuntimeState* state_;

  /// If true, indicates this is a debug context which will do additional validation.
  bool debug_;

  impala_udf::FunctionContext::ImpalaVersion version_;

  /// Empty if there's no error
  std::string error_msg_;

  /// The number of warnings reported.
  int64_t num_warnings_;

  /// The number of calls to Update()/Remove().
  int64_t num_updates_;
  int64_t num_removes_;

  /// Allocations made and still owned by the user function. Only used if debug_ is true
  /// because it is very expensive to maintain.
  std::map<uint8_t*, int> allocations_;
  /// Allocations owned by Impala.
  std::vector<uint8_t*> local_allocations_;

  /// The function state accessed via FunctionContext::Get/SetFunctionState()
  void* thread_local_fn_state_;
  void* fragment_local_fn_state_;

  /// The number of bytes allocated externally by the user function. In some cases,
  /// it is too inconvenient to use the Allocate()/Free() APIs in the FunctionContext,
  /// particularly for existing codebases (e.g. they use std::vector). Instead, they'll
  /// have to track those allocations manually.
  int64_t external_bytes_tracked_;

  /// Type descriptor for the intermediate type of a UDA. Set to INVALID_TYPE for UDFs.
  impala_udf::FunctionContext::TypeDesc intermediate_type_;

  /// Type descriptor for the return type of the function.
  impala_udf::FunctionContext::TypeDesc return_type_;

  /// Type descriptors for each argument of the function.
  std::vector<impala_udf::FunctionContext::TypeDesc> arg_types_;

  /// Contains an AnyVal* for each argument of the function. If the AnyVal* is NULL,
  /// indicates that the corresponding argument is non-constant. Otherwise contains the
  /// value of the argument.
  std::vector<impala_udf::AnyVal*> constant_args_;

  /// Used by ScalarFnCall to store the arguments when running without codegen. Allows us
  /// to pass AnyVal* arguments to the scalar function directly, rather than codegening a
  /// call that passes the correct AnyVal subclass pointer type. Note that this is only
  /// used for non-variadic arguments; varargs are always stored in varargs_buffer_.
  std::vector<impala_udf::AnyVal*> staging_input_vals_;

  /// Indicates whether this context has been closed. Used for verification/debugging.
  bool closed_;
};

}

namespace impala_udf {

/// Temporary CollectionVal definition, used to represent arrays and maps. This is not
/// ready for public consumption because users must have access to our internal tuple
/// layout.
struct CollectionVal : public AnyVal {
  uint8_t* ptr;
  int num_tuples;

  /// Construct an CollectionVal from ptr/num_tuples. Note: this does not make a copy of
  /// ptr so the buffer must exist as long as this CollectionVal does.
  CollectionVal(uint8_t* ptr = NULL, int num_tuples = 0)
      : ptr(ptr), num_tuples(num_tuples) {}

  static CollectionVal null() {
    CollectionVal cv;
    cv.is_null = true;
    return cv;
  }
};

}

#endif
