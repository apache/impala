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
#include <sstream>
#include <assert.h>

// Be careful what this includes since this needs to be linked into the UDF's
// binary. For example, it would be unfortunate if they had a random dependency
// on libhdfs.
#include "udf/udf-internal.h"

#if IMPALA_UDF_SDK_BUILD
// For the SDK build, we are building the .lib that the developers would use to
// write UDFs. They want to link against this to run their UDFs in a test environment.
// Pulling in free-pool is very undesirable since it pulls in many other libraries.
// Instead, we'll implement a dummy version that is not used.
// When they build their library to a .so, they'd use the version of FunctionContext
// in the main binary, which does include FreePool.
namespace impala {
class FreePool {
 public:
  FreePool(MemPool*) { }

  uint8_t* Allocate(int byte_size) {
    return reinterpret_cast<uint8_t*>(malloc(byte_size));
  }

  uint8_t* Reallocate(uint8_t* ptr, int byte_size) {
    return reinterpret_cast<uint8_t*>(realloc(ptr, byte_size));
  }

  void Free(uint8_t* ptr) {
    free(ptr);
  }
};

class RuntimeState {
 public:
  void set_query_status(const std::string& error_msg) {
    assert(false);
  }

  bool LogError(const std::string& error) {
    assert(false);
    return false;
  }

  const std::string connected_user() const { return ""; }
};
}
#else
#include "runtime/free-pool.h"
#include "runtime/runtime-state.h"
#endif

using namespace impala;
using namespace impala_udf;
using namespace std;

static const int MAX_WARNINGS = 1000;

// Create a FunctionContext. The caller is responsible for calling delete on it.
FunctionContext* FunctionContextImpl::CreateContext(RuntimeState* state, MemPool* pool) {
  impala_udf::FunctionContext* ctx = new impala_udf::FunctionContext();
  ctx->impl_->state_ = state;
  ctx->impl_->pool_ = new FreePool(pool);
  return ctx;
}

FunctionContext* FunctionContext::CreateTestContext() {
  FunctionContext* context = new FunctionContext;
  context->impl()->debug_ = true;
  context->impl()->state_ = NULL;
  context->impl()->pool_ = new FreePool(NULL);
  return context;
}

FunctionContext::FunctionContext() : impl_(new FunctionContextImpl(this)) {
}

FunctionContext::~FunctionContext() {
  // TODO: this needs to free local allocations but there's a mem issue
  // in the uda harness now.
  impl_->CheckLocalAlloctionsEmpty();
  impl_->CheckAllocationsEmpty();
  delete impl_->pool_;
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

const char* FunctionContext::user() const {
  if (impl_->state_ == NULL) return NULL;
  return impl_->state_->connected_user().c_str();
}

FunctionContext::UniqueId FunctionContext::query_id() const {
  UniqueId id;
#if IMPALA_UDF_SDK_BUILD
  id.hi = id.lo = 0;
#else
  id.hi = impl_->state_->query_id().hi;
  id.lo = impl_->state_->query_id().lo;
#endif
  return id;
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
  uint8_t* buffer = impl_->pool_->Allocate(byte_size);
  impl_->allocations_[buffer] = byte_size;
  if (impl_->debug_) memset(buffer, 0xff, byte_size);
  return buffer;
}

uint8_t* FunctionContext::Reallocate(uint8_t* ptr, int byte_size) {
  impl_->allocations_.erase(ptr);
  ptr = impl_->pool_->Reallocate(ptr, byte_size);
  impl_->allocations_[ptr] = byte_size;
  return ptr;
}

void FunctionContext::Free(uint8_t* buffer) {
  if (buffer == NULL) return;
  if (impl_->debug_) {
    map<uint8_t*, int>::iterator it = impl_->allocations_.find(buffer);
    if (it != impl_->allocations_.end()) {
      // fill in garbage value into the buffer to increase the chance of detecting misuse
      memset(buffer, 0xff, it->second);
      impl_->allocations_.erase(it);
      impl_->pool_->Free(buffer);
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
    stringstream ss;
    ss << "UDF ERROR: " << error_msg;
    if (impl_->state_ != NULL) impl_->state_->set_query_status(ss.str());
  }
}

bool FunctionContext::AddWarning(const char* warning_msg) {
  if (impl_->num_warnings_++ >= MAX_WARNINGS) return false;
  stringstream ss;
  ss << "UDF WARNING: " << warning_msg;
  if (impl_->state_ != NULL) {
    return impl_->state_->LogError(ss.str());
  } else {
    cerr << ss.str() << endl;
    return true;
  }
}

uint8_t* FunctionContextImpl::AllocateLocal(int byte_size) {
  if (byte_size == 0) return NULL;
  uint8_t* buffer = pool_->Allocate(byte_size);
  local_allocations_.push_back(buffer);
  return buffer;
}

void FunctionContextImpl::FreeLocalAllocations() {
  for (int i = 0; i < local_allocations_.size(); ++i) {
    pool_->Free(local_allocations_[i]);
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
