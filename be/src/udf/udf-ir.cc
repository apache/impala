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

const FunctionContext::TypeDesc& FunctionContext::GetReturnType() const {
  return impl_->return_type_;
}
