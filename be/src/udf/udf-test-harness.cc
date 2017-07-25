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

#include "udf/udf-test-harness.h"

#include <vector>
#include "runtime/runtime-state.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using namespace impala_udf;
using namespace impala;

FunctionContext* UdfTestHarness::CreateTestContext(
    const FunctionContext::TypeDesc& return_type,
    const vector<FunctionContext::TypeDesc>& arg_types, RuntimeState* state,
    MemPool* pool) {
  return FunctionContextImpl::CreateContext(
      state, pool, pool, return_type, arg_types, 0, true);
}

void UdfTestHarness::SetConstantArgs(
    FunctionContext* context, const vector<AnyVal*>& constant_args) {
  if (!context->impl()->debug()) {
    context->SetError("SetConstantArgs() called on non-test FunctionContext");
    return;
  }
  context->impl()->SetConstantArgs(vector<AnyVal*>(constant_args));
}

void UdfTestHarness::CloseContext(FunctionContext* context) {
  context->impl()->Close();
}
