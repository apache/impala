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

#include "udf/udf-test-harness.h"

#include <vector>
#include "udf/udf-internal.h"

using namespace impala_udf;
using namespace impala;
using namespace std;

FunctionContext* UdfTestHarness::CreateTestContext(
    const FunctionContext::TypeDesc& return_type,
    const vector<FunctionContext::TypeDesc>& arg_types) {
  return FunctionContextImpl::CreateContext(NULL, NULL, return_type, arg_types, 0, true);
}

void UdfTestHarness::SetConstantArgs(
    FunctionContext* context, const vector<AnyVal*>& constant_args) {
  if (!context->impl()->debug()) {
    context->SetError("SetConstantArgs() called on non-test FunctionContext");
    return;
  }
  context->impl()->SetConstantArgs(constant_args);
}

void UdfTestHarness::CloseContext(FunctionContext* context) {
  context->impl()->Close();
}
