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
#include "udf/udf.h"

namespace impala {

class FreePool;
class MemPool;
class RuntimeState;

// This class actually implements the interface of FunctionContext. This is split to
// hide the details from the external header.
// Note: The actual user code does not include this file.
class FunctionContextImpl {
 public:
  // Create a FunctionContext. The caller is responsible for calling delete on it.
  static impala_udf::FunctionContext* CreateContext(RuntimeState* state, MemPool* pool);

  FunctionContextImpl(impala_udf::FunctionContext* parent);

  // Allocates a buffer of 'byte_size' with "local" memory management. These
  // allocations are not freed one by one but freed as a pool by FreeLocalAllocations()
  // This is used where the lifetime of the allocation is clear.
  // For UDFs, the allocations can be freed at the row level.
  // TODO: free them at the batch level and save some copies?
  uint8_t* AllocateLocal(int byte_size);

  // Frees all allocations returned by AllocateLocal().
  void FreeLocalAllocations();

  // Returns true if there are no outstanding allocations.
  bool CheckAllocationsEmpty();

  // Returns true if there are no outstanding local allocations.
  bool CheckLocalAlloctionsEmpty();

 private:
  friend class impala_udf::FunctionContext;

  // Parent context object. Not owned
  impala_udf::FunctionContext* context_;

  // Pool to service allocations from.
  FreePool* pool_;

  // We use the query's runtime state to report errors and warnings. NULL for test
  // contexts.
  RuntimeState* state_;

  // If true, indicates this is a debug context which will do additional validation.
  bool debug_;

  impala_udf::FunctionContext::ImpalaVersion version_;

  // Empty if there's no error
  std::string error_msg_;

  // The number of warnings reported.
  int64_t num_warnings_;

  // Allocations made and still owned by the user function.
  std::map<uint8_t*, int> allocations_;
  std::vector<uint8_t*> local_allocations_;

  // The number of bytes allocated externally by the user function. In some cases,
  // it is too inconvenient to use the Allocate()/Free() APIs in the FunctionContext,
  // particularly for existing codebases (e.g. they use std::vector). Instead, they'll
  // have to track those allocations manually.
  int64_t external_bytes_tracked_;
};

}

#endif

