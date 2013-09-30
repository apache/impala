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

#include "udf/udf.h"
#include <iostream>
#include <assert.h>

// Be careful what this includes since this needs to be linked into the UDF's
// binary. For example, it would be unfortunate if they had a random dependency
// on libhdfs.
#include "udf/udf-internal.h"

#if IMPALA_UDF_SDK_BUILD
// For the SDK build, we are building the .lib that the developers would use to
// write UDFs. They want to link against this to run their UDFs in a test environment.
// Pulling in mem-pool is very undesirable since it pulls in many other libaries.
// Instead, we'll implement a dummy version that is not used.
// When they build their library to a .so, they'd use the version of FunctionContext
// in the main binary, which does include MemPool.
namespace impala {
class MemPool {
 public:
  uint8_t* Allocate(int byte_size) {
    assert(false);
    return NULL;
  }
};
}
#else
#include "runtime/mem-pool.h"
#endif

using namespace impala;
using namespace impala_udf;
using namespace std;

static const int MAX_WARNINGS = 1000;

FunctionContext* FunctionContext::CreateTestContext() {
  FunctionContext* context = new FunctionContext;
  context->impl()->debug_ = true;
  context->impl()->pool_ = NULL;
  return context;
}

FunctionContext::FunctionContext() : impl_(new FunctionContextImpl(this)) {
}

FunctionContext::~FunctionContext() {
  // TODO: this needs to free local allocations but there's a mem issue
  // in the uda harness now.
  impl_->CheckLocalAlloctionsEmpty();
  impl_->CheckAllocationsEmpty();
  if (has_error()) {
    cerr << "FunctionContext ran into error: " << error_msg() << endl;
  }
  delete impl_;
}

FunctionContextImpl::FunctionContextImpl(FunctionContext* parent)
  : context_(parent), debug_(false), version_(FunctionContext::v1_2),
    num_warnings_(0),
    external_bytes_tracked_(0) {
}

FunctionContext::ImpalaVersion FunctionContext::version() const {
  return impl_->version_;
}

bool FunctionContext::has_error() const {
  return !impl_->error_msg_.empty();
}

const char* FunctionContext::error_msg() const {
  if (has_error()) return impl_->error_msg_.c_str();
  return NULL;
}

uint8_t* FunctionContext::Allocate(int byte_size) {
  if (byte_size == 0) return NULL;
  uint8_t* buffer = NULL;
  if (impl_->pool_ == NULL) {
    buffer = new uint8_t[byte_size];
  } else {
    buffer = impl_->pool_->Allocate(byte_size);
  }
  impl_->allocations_[buffer] = byte_size;
  if (impl_->debug_) memset(buffer, 0xff, byte_size);
  return buffer;
}

void FunctionContext::Free(uint8_t* buffer) {
  if (buffer == NULL) return;
  if (impl_->debug_) {
    map<uint8_t*, int>::iterator it = impl_->allocations_.find(buffer);
    if (it != impl_->allocations_.end()) {
      // fill in garbage value into the buffer to increase the chance of detecting misuse
      memset(buffer, 0xff, it->second);
      impl_->allocations_.erase(it);
      if (impl_->pool_ == NULL) delete[] buffer;
    } else {
      SetError(
          "FunctionContext::Free() on buffer that is not freed or was not allocated.");
    }
  } else {
    impl_->allocations_.erase(buffer);
  }
}

void FunctionContext::TrackAllocation(int64_t bytes) {
  impl_->external_bytes_tracked_ += bytes;
}

void FunctionContext::Free(int64_t bytes) {
  impl_->external_bytes_tracked_ -= bytes;
}

void FunctionContext::SetError(const char* error_msg) {
  if (impl_->error_msg_.empty()) {
    impl_->error_msg_ = error_msg;
  }
}

bool FunctionContext::AddWarning(const char* warning_msg) {
  ++impl_->num_warnings_;
  if (impl_->warning_msgs_.size() >= MAX_WARNINGS) return false;
  impl_->warning_msgs_.push_back(warning_msg);
  return true;
}

uint8_t* FunctionContextImpl::AllocateLocal(int byte_size) {
  if (byte_size == 0) return NULL;
  uint8_t* buffer = NULL;
  if (pool_ == NULL) {
    buffer = new uint8_t[byte_size];
  } else {
    buffer = pool_->Allocate(byte_size);
  }
  local_allocations_.push_back(buffer);
  return buffer;
}

void FunctionContextImpl::FreeLocalAllocations() {
  // TODO: these should be reused rather than freed.
  // TODO: Integrate with MemPool
  for (int i = 0; i < local_allocations_.size(); ++i) {
    if (pool_ == NULL) delete[] local_allocations_[i];
  }
  local_allocations_.clear();
}

bool FunctionContextImpl::CheckAllocationsEmpty() {
  if (allocations_.empty() && external_bytes_tracked_ == 0) return true;
  // TODO: fix this
  //if (debug_) context_->SetError("Leaked allocations.");
  return false;
}

bool FunctionContextImpl::CheckLocalAlloctionsEmpty() {
  if (local_allocations_.empty()) return true;
  // TODO: fix this
  //if (debug_) context_->SetError("Leaked local allocations.");
  return false;
}

StringVal::StringVal(FunctionContext* context, int len)
  : len(len), ptr(context->impl()->AllocateLocal(len)) {
}

