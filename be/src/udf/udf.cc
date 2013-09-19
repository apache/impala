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

// Be careful what this includes since this needs to be linked into the UDF's
// binary. For example, it would be unfortunate if they had a random dependency
// on libhdfs.
#include "udf/udf-internal.h"

using namespace impala;
using namespace impala_udf;
using namespace std;

static const int MAX_WARNINGS = 1000;

UdfContext* UdfContext::CreateTestContext() {
  UdfContext* context = new UdfContext;
  context->impl()->debug_ = true;
  return context;
}

UdfContext::UdfContext() : impl_(new UdfContextImpl(this)) {
}

UdfContext::~UdfContext() {
  // TODO: this needs to free local allocations but there's a mem issue
  // in the uda harness now.
  impl_->CheckLocalAlloctionsEmpty();
  impl_->CheckAllocationsEmpty();
  if (has_error()) {
    cerr << "UdfContext ran into error: " << error_msg() << endl;
  }
  delete impl_;
}

UdfContextImpl::UdfContextImpl(UdfContext* parent)
  : context_(parent), debug_(false), version_(UdfContext::v1_2),
    num_warnings_(0),
    external_bytes_tracked_(0) {
}

UdfContext::ImpalaVersion UdfContext::version() const {
  return impl_->version_;
}

bool UdfContext::has_error() const {
  return !impl_->error_msg_.empty();
}

const char* UdfContext::error_msg() const {
  if (has_error()) return impl_->error_msg_.c_str();
  return NULL;
}

uint8_t* UdfContext::Allocate(int byte_size) {
  if (byte_size == 0) return NULL;
  uint8_t* buffer = new uint8_t[byte_size];
  impl_->allocations_[buffer] = byte_size;
  if (impl_->debug_) memset(buffer, 0xff, byte_size);
  return buffer;
}

void UdfContext::Free(uint8_t* buffer) {
  if (buffer == NULL) return;
  if (impl_->debug_) {
    map<uint8_t*, int>::iterator it = impl_->allocations_.find(buffer);
    if (it != impl_->allocations_.end()) {
      // fill in garbage value into the buffer to increase the chance of detecting misuse
      memset(buffer, 0xff, it->second);
      impl_->allocations_.erase(it);
      delete[] buffer;
    } else {
      SetError("UdfContext::Free() on buffer that is not freed or was not allocated.");
    }
  } else {
    impl_->allocations_.erase(buffer);
  }
}

void UdfContext::TrackAllocation(int64_t bytes) {
  impl_->external_bytes_tracked_ += bytes;
}

void UdfContext::Free(int64_t bytes) {
  impl_->external_bytes_tracked_ -= bytes;
}

void UdfContext::SetError(const char* error_msg) {
  if (impl_->error_msg_.empty()) {
    impl_->error_msg_ = error_msg;
  }
}

bool UdfContext::AddWarning(const char* warning_msg) {
  ++impl_->num_warnings_;
  if (impl_->warning_msgs_.size() >= MAX_WARNINGS) return false;
  impl_->warning_msgs_.push_back(warning_msg);
  return true;
}

uint8_t* UdfContextImpl::AllocateLocal(int byte_size) {
  if (byte_size == 0) return NULL;
  uint8_t* buffer = new uint8_t[byte_size];
  local_allocations_.push_back(buffer);
  return buffer;
}

void UdfContextImpl::FreeLocalAllocations() {
  // TODO: these should be reused rather than freed.
  // TODO: Integrate with MemPool
  for (int i = 0; i < local_allocations_.size(); ++i) {
    delete[] local_allocations_[i];
  }
  local_allocations_.clear();
}

bool UdfContextImpl::CheckAllocationsEmpty() {
  if (allocations_.empty() && external_bytes_tracked_ == 0) return true;
  // TODO: fix this
  //if (debug_) context_->SetError("Leaked allocations.");
  return false;
}

bool UdfContextImpl::CheckLocalAlloctionsEmpty() {
  if (local_allocations_.empty()) return true;
  // TODO: fix this
  //if (debug_) context_->SetError("Leaked local allocations.");
  return false;
}

StringVal::StringVal(UdfContext* context, int len)
  : len(len), ptr(context->impl()->AllocateLocal(len)) {
}

