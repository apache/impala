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
#include <gutil/port.h> // for aligned_malloc

#ifndef IMPALA_UDF_SDK_BUILD
#include "util/error-util.h"
#endif

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

#define VLOG_ROW while(false) std::cout
#define VLOG_ROW_IS_ON (false)

namespace impala {

class MemTracker {
 public:
  void Consume(int64_t bytes) { }
  void Release(int64_t bytes) { }
};

class FreePool {
 public:
  FreePool(MemPool*) : net_allocations_(0) { }

  uint8_t* Allocate(int byte_size) {
    ++net_allocations_;
    return reinterpret_cast<uint8_t*>(malloc(byte_size));
  }

  uint8_t* Reallocate(uint8_t* ptr, int byte_size) {
    return reinterpret_cast<uint8_t*>(realloc(ptr, byte_size));
  }

  void Free(uint8_t* ptr) {
    --net_allocations_;
    free(ptr);
  }

  MemTracker* mem_tracker() { return &mem_tracker_; }
  int64_t net_allocations() const { return net_allocations_; }

 private:
  MemTracker mem_tracker_;
  int64_t net_allocations_;
};

class RuntimeState {
 public:
  void SetQueryStatus(const std::string& error_msg) {
    assert(false);
  }

  bool abort_on_error() {
    assert(false);
    return false;
  }

  bool LogError(const std::string& error) {
    assert(false);
    return false;
  }

  const std::string connected_user() const { return ""; }
  const std::string effective_user() const { return ""; }
};

}

#else
#include "runtime/free-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#endif

#include "common/names.h"
#include "common/compiler-util.h"

using namespace impala;
using namespace impala_udf;

const char* FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME =
    "class.impala_udf::FunctionContext";

static const int MAX_WARNINGS = 1000;

FunctionContext* FunctionContextImpl::CreateContext(RuntimeState* state, MemPool* pool,
    const FunctionContext::TypeDesc& return_type,
    const vector<FunctionContext::TypeDesc>& arg_types,
    int varargs_buffer_size, bool debug) {
  FunctionContext::TypeDesc invalid_type;
  invalid_type.type = FunctionContext::INVALID_TYPE;
  invalid_type.precision = 0;
  invalid_type.scale = 0;
  return FunctionContextImpl::CreateContext(state, pool, invalid_type, return_type,
      arg_types, varargs_buffer_size, debug);
}

FunctionContext* FunctionContextImpl::CreateContext(RuntimeState* state, MemPool* pool,
    const FunctionContext::TypeDesc& intermediate_type,
    const FunctionContext::TypeDesc& return_type,
    const vector<FunctionContext::TypeDesc>& arg_types,
    int varargs_buffer_size, bool debug) {
  impala_udf::FunctionContext* ctx = new impala_udf::FunctionContext();
  ctx->impl_->state_ = state;
  ctx->impl_->pool_ = new FreePool(pool);
  ctx->impl_->intermediate_type_ = intermediate_type;
  ctx->impl_->return_type_ = return_type;
  ctx->impl_->arg_types_ = arg_types;
  // UDFs may manipulate DecimalVal arguments via SIMD instructions such as 'movaps'
  // that require 16-byte memory alignment.
  ctx->impl_->varargs_buffer_ =
      reinterpret_cast<uint8_t*>(aligned_malloc(varargs_buffer_size, 16));
  ctx->impl_->varargs_buffer_size_ = varargs_buffer_size;
  ctx->impl_->debug_ = debug;
  VLOG_ROW << "Created FunctionContext: " << ctx
           << " with pool " << ctx->impl_->pool_;
  return ctx;
}

FunctionContext* FunctionContextImpl::Clone(MemPool* pool) {
  impala_udf::FunctionContext* new_context =
      CreateContext(state_, pool, intermediate_type_, return_type_, arg_types_,
          varargs_buffer_size_, debug_);
  new_context->impl_->constant_args_ = constant_args_;
  new_context->impl_->fragment_local_fn_state_ = fragment_local_fn_state_;
  return new_context;
}

FunctionContext::FunctionContext() : impl_(new FunctionContextImpl(this)) {
}

FunctionContext::~FunctionContext() {
  assert(impl_->closed_ && "FunctionContext wasn't closed!");
  delete impl_->pool_;
  delete impl_;
}

FunctionContextImpl::FunctionContextImpl(FunctionContext* parent)
  : varargs_buffer_(NULL),
    varargs_buffer_size_(0),
    context_(parent),
    pool_(NULL),
    state_(NULL),
    debug_(false),
    version_(FunctionContext::v1_3),
    num_warnings_(0),
    num_updates_(0),
    num_removes_(0),
    thread_local_fn_state_(NULL),
    fragment_local_fn_state_(NULL),
    external_bytes_tracked_(0),
    closed_(false) {
}

void FunctionContextImpl::Close() {
  if (closed_) return;

  // Free local allocations first so we can detect leaks through any remaining allocations
  // (local allocations cannot be leaked, at least not by the UDF)
  FreeLocalAllocations();

  stringstream error_ss;
  if (!debug_) {
    if (pool_->net_allocations() > 0) {
      error_ss << "Memory leaked via FunctionContext::Allocate()";
    } else if (pool_->net_allocations() < 0) {
      error_ss << "FunctionContext::Free() called on buffer that was already freed or "
                  "was not allocated.";
    }
  } else if (!allocations_.empty()) {
    int bytes = 0;
    for (map<uint8_t*, int>::iterator i = allocations_.begin();
         i != allocations_.end(); ++i) {
      bytes += i->second;
    }
    error_ss << bytes << " bytes leaked via FunctionContext::Allocate()";
    allocations_.clear();
  }

  if (external_bytes_tracked_ > 0) {
    if (!error_ss.str().empty()) error_ss << ", ";
    error_ss << external_bytes_tracked_
             << " bytes leaked via FunctionContext::TrackAllocation()";
    // This isn't ideal because the memory is still leaked, but don't track it so our
    // accounting stays sane.
    // TODO: we need to modify the memtrackers to allow leaked user-allocated memory.
    context_->Free(external_bytes_tracked_);
  }

  if (!error_ss.str().empty()) {
    // Treat memory leaks as errors in the SDK build so they trigger test failures, but
    // don't blow up actual queries due to leaks (unless abort_on_error is true).
    // TODO: revisit abort_on_error case. Setting the error won't do anything in close.
    if (state_ == NULL || state_->abort_on_error()) {
      context_->SetError(error_ss.str().c_str());
    } else {
      context_->AddWarning(error_ss.str().c_str());
    }
  }

  free(varargs_buffer_);
  varargs_buffer_ = NULL;
  closed_ = true;
}

FunctionContext::ImpalaVersion FunctionContext::version() const {
  return impl_->version_;
}

const char* FunctionContext::user() const {
  if (impl_->state_ == NULL) return NULL;
  return impl_->state_->connected_user().c_str();
}

const char* FunctionContext::effective_user() const {
  if (impl_->state_ == NULL) return NULL;
  return impl_->state_->effective_user().c_str();
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

inline bool FunctionContextImpl::CheckAllocResult(const char* fn_name,
    uint8_t* buf, int byte_size) {
  if (UNLIKELY(buf == NULL)) {
    stringstream ss;
    ss << string(fn_name) << "() failed to allocate " << byte_size << " bytes.";
    context_->SetError(ss.str().c_str());
    return false;
  }
#ifndef IMPALA_UDF_SDK_BUILD
  MemTracker* mem_tracker = pool_->mem_tracker();
  if (mem_tracker->LimitExceeded()) {
    ErrorMsg msg = ErrorMsg(TErrorCode::UDF_MEM_LIMIT_EXCEEDED, string(fn_name));
    state_->SetMemLimitExceeded(mem_tracker, byte_size, &msg);
  }
#endif
  return true;
}

uint8_t* FunctionContext::Allocate(int byte_size) {
  assert(!impl_->closed_);
  if (byte_size == 0) return NULL;
  uint8_t* buffer = impl_->pool_->Allocate(byte_size);
  if (UNLIKELY(!impl_->CheckAllocResult("FunctionContext::Allocate",
      buffer, byte_size))) {
    return NULL;
  }
  if (UNLIKELY(impl_->debug_)) {
    impl_->allocations_[buffer] = byte_size;
    memset(buffer, 0xff, byte_size);
  }
  VLOG_ROW << "Allocate: FunctionContext=" << this
           << " size=" << byte_size
           << " result=" << reinterpret_cast<void*>(buffer);
  return buffer;
}

uint8_t* FunctionContext::Reallocate(uint8_t* ptr, int byte_size) {
  assert(!impl_->closed_);
  VLOG_ROW << "Reallocate: FunctionContext=" << this
           << " size=" << byte_size
           << " ptr=" << reinterpret_cast<void*>(ptr);
  uint8_t* new_ptr = impl_->pool_->Reallocate(ptr, byte_size);
  if (UNLIKELY(!impl_->CheckAllocResult("FunctionContext::Reallocate",
      new_ptr, byte_size))) {
    return NULL;
  }
  if (UNLIKELY(impl_->debug_)) {
    impl_->allocations_.erase(ptr);
    impl_->allocations_[new_ptr] = byte_size;
  }
  VLOG_ROW << "FunctionContext=" << this
           << " reallocated: " << reinterpret_cast<void*>(new_ptr);
  return new_ptr;
}

void FunctionContext::Free(uint8_t* buffer) {
  assert(!impl_->closed_);
  if (buffer == NULL) return;
  VLOG_ROW << "Free: FunctionContext=" << this << " "
           << reinterpret_cast<void*>(buffer);
  if (impl_->debug_) {
    map<uint8_t*, int>::iterator it = impl_->allocations_.find(buffer);
    if (it != impl_->allocations_.end()) {
      // fill in garbage value into the buffer to increase the chance of detecting misuse
      memset(buffer, 0xff, it->second);
      impl_->allocations_.erase(it);
      impl_->pool_->Free(buffer);
    } else {
      SetError("FunctionContext::Free() called on buffer that is already freed or was "
               "not allocated.");
    }
  } else {
    impl_->pool_->Free(buffer);
  }
}

void FunctionContext::TrackAllocation(int64_t bytes) {
  assert(!impl_->closed_);
  impl_->external_bytes_tracked_ += bytes;
  impl_->pool_->mem_tracker()->Consume(bytes);
}

void FunctionContext::Free(int64_t bytes) {
  assert(!impl_->closed_);
  if (bytes > impl_->external_bytes_tracked_) {
    stringstream ss;
    ss << "FunctionContext::Free() called with " << bytes << " bytes, but only "
       << impl_->external_bytes_tracked_ << " bytes are tracked via "
       << "FunctionContext::TrackAllocation()";
    SetError(ss.str().c_str());
    return;
  }
  impl_->external_bytes_tracked_ -= bytes;
  impl_->pool_->mem_tracker()->Release(bytes);
}

void FunctionContext::SetError(const char* error_msg) {
  assert(!impl_->closed_);
  if (impl_->error_msg_.empty()) {
    impl_->error_msg_ = error_msg;
    stringstream ss;
    ss << "UDF ERROR: " << error_msg;
    if (impl_->state_ != NULL) impl_->state_->SetQueryStatus(ss.str());
  }
}

// TODO: is there a way to tell the user the expr in a reasonable way?
// Plumb the ToSql() from the FE?
// TODO: de-dup warnings
bool FunctionContext::AddWarning(const char* warning_msg) {
  assert(!impl_->closed_);
  if (impl_->num_warnings_++ >= MAX_WARNINGS) return false;
  stringstream ss;
  ss << "UDF WARNING: " << warning_msg;
  if (impl_->state_ != NULL) {
#ifndef IMPALA_UDF_SDK_BUILD
    // If this is called while the query is being closed, the runtime state log will have
    // already been displayed to the user. Also log the warning so there's some chance
    // the user will actually see it.
    // TODO: somehow print the full error log in the shell? This is a problem for any
    // function using LogError() during close.
    LOG(WARNING) << ss.str();
    return impl_->state_->LogError(ErrorMsg(TErrorCode::GENERAL, ss.str()));
#else
    // In case of the SDK build, we simply, forward this call to a dummy method
    return impl_->state_->LogError(ss.str());
#endif

  } else {
    cerr << ss.str() << endl;
    return true;
  }
}

void FunctionContext::SetFunctionState(FunctionStateScope scope, void* ptr) {
  assert(!impl_->closed_);
  switch (scope) {
    case THREAD_LOCAL:
      impl_->thread_local_fn_state_ = ptr;
      break;
    case FRAGMENT_LOCAL:
      impl_->fragment_local_fn_state_ = ptr;
      break;
    default:
      stringstream ss;
      ss << "Unknown FunctionStateScope: " << scope;
      SetError(ss.str().c_str());
  }
}

uint8_t* FunctionContextImpl::AllocateLocal(int byte_size) {
  assert(!closed_);
  if (byte_size == 0) return NULL;
  uint8_t* buffer = pool_->Allocate(byte_size);
  if (UNLIKELY(!CheckAllocResult("FunctionContextImpl::AllocateLocal",
      buffer, byte_size))) {
    return NULL;
  }
  local_allocations_.push_back(buffer);
  VLOG_ROW << "Allocate Local: FunctionContext=" << context_
           << " size=" << byte_size
           << " result=" << reinterpret_cast<void*>(buffer);
  return buffer;
}

void FunctionContextImpl::FreeLocalAllocations() {
  assert(!closed_);
  if (VLOG_ROW_IS_ON) {
    stringstream ss;
    ss << "Free local allocations: FunctionContext=" << context_
       << " pool=" << pool_ << endl;
    for (int i = 0; i < local_allocations_.size(); ++i) {
      ss << "  " << reinterpret_cast<void*>(local_allocations_[i]) << endl;
    }
    VLOG_ROW << ss.str();
  }
  for (int i = 0; i < local_allocations_.size(); ++i) {
    pool_->Free(local_allocations_[i]);
  }
  local_allocations_.clear();
}

void FunctionContextImpl::SetConstantArgs(const vector<AnyVal*>& constant_args) {
  constant_args_ = constant_args;
}

// Note: this function crashes LLVM's JIT in expr-test if it's xcompiled. Do not move to
// expr-ir.cc. This could probably use further investigation.
StringVal::StringVal(FunctionContext* context, int len)
  : len(len), ptr(NULL) {
  if (UNLIKELY(len > StringVal::MAX_LENGTH)) {
    std::cout << "MAX_LENGTH, Trying to allocate " << len;
    context->SetError("String length larger than allowed limit of "
        "1 GB character data.");
    len = 0;
    is_null = true;
  } else {
    ptr = context->impl()->AllocateLocal(len);
    if (UNLIKELY(ptr == NULL && len > 0)) {
#ifndef IMPALA_UDF_SDK_BUILD
      assert(!context->impl()->state()->GetQueryStatus().ok());
#endif
      len = 0;
      is_null = true;
    }
  }
}

StringVal StringVal::CopyFrom(FunctionContext* ctx, const uint8_t* buf, size_t len) {
  StringVal result(ctx, len);
  if (LIKELY(!result.is_null)) {
    memcpy(result.ptr, buf, len);
  }
  return result;
}

// TODO: why doesn't libudasample.so build if this in udf-ir.cc?
const FunctionContext::TypeDesc* FunctionContext::GetArgType(int arg_idx) const {
  if (arg_idx < 0 || arg_idx >= impl_->arg_types_.size()) return NULL;
  return &impl_->arg_types_[arg_idx];
}
