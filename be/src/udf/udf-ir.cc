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

#include "udf/udf-internal.h"

using namespace impala;
using namespace impala_udf;

bool FunctionContext::IsArgConstant(int i) const {
  if (i < 0 || i >= impl_->constant_args_.size()) return false;
  return impl_->constant_args_[i] != NULL;
}

AnyVal* FunctionContext::GetConstantArg(int i) const {
  if (i < 0 || i >= impl_->constant_args_.size()) return NULL;
  return impl_->constant_args_[i];
}

int FunctionContext::GetNumArgs() const {
  return impl_->arg_types_.size();
}

const FunctionContext::TypeDesc& FunctionContext::GetIntermediateType() const {
  return impl_->intermediate_type_;
}

const FunctionContext::TypeDesc& FunctionContext::GetReturnType() const {
  return impl_->return_type_;
}

void* FunctionContext::GetFunctionState(FunctionStateScope scope) const {
  assert(!impl_->closed_);
  switch (scope) {
    case THREAD_LOCAL:
      return impl_->thread_local_fn_state_;
    case FRAGMENT_LOCAL:
      return impl_->fragment_local_fn_state_;
    default:
      // TODO: signal error somehow
      return NULL;
  }
}

uint8_t* FnCtxAllocateForResults(FunctionContext* ctx, int64_t byte_size) {
  assert(ctx != nullptr);
  uint8_t* ptr = ctx->impl()->AllocateForResults(byte_size);
  return ptr;
}
