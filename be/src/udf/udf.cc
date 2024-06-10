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

#include "udf/udf.h"

#include <algorithm>
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

#if defined(IMPALA_UDF_SDK_BUILD) && IMPALA_UDF_SDK_BUILD
// For the SDK build, we are building the .lib that the developers would use to
// write UDFs. They want to link against this to run their UDFs in a test environment.
// Pulling in free-pool is very undesirable since it pulls in many other libraries.
// Instead, we'll implement a dummy version that is not used.
// When they build their library to a .so, they'd use the version of FunctionContext
// in the main binary, which does include FreePool.

#define VLOG_ROW while(false) std::cout

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

class MemPool {
 public:
  static uint8_t* Allocate(int byte_size) {
    return reinterpret_cast<uint8_t*>(malloc(byte_size));
  }
};

class RuntimeState {
 public:
  void SetQueryStatus(const std::string& error_msg) {
    assert(false);
  }

  bool abort_on_error() const {
    assert(false);
    return false;
  }

  bool decimal_v2() const {
    assert(false);
    return false;
  }

  bool utf8_mode() const {
    assert(false);
    return false;
  }

  bool LogError(const std::string& error) {
    assert(false);
    return false;
  }

  const std::string connected_user() const { return user_string_; }
  const std::string GetEffectiveUser() const { return user_string_; }

 private:
  const std::string user_string_ = "";
};

// Dummy AiFunctions class for UDF SDK
static const std::string AI_FUNCTIONS_DUMMY_RESPONSE = "dummy response";
using impala_udf::StringVal;
using impala_udf::FunctionContext;
class AiFunctions {
 public:
  static StringVal AiGenerateText(FunctionContext* ctx, const StringVal& endpoint,
      const StringVal& prompt, const StringVal& model,
      const StringVal& api_key_jceks_secret, const StringVal& params) {
    return StringVal(AI_FUNCTIONS_DUMMY_RESPONSE.c_str());
  }
  static StringVal AiGenerateTextDefault(FunctionContext* ctx, const StringVal& prompt) {
    return StringVal(AI_FUNCTIONS_DUMMY_RESPONSE.c_str());
  }
};
}

#else
#include "common/atomic.h"
#include "exprs/ai-functions.h"
#include "exprs/anyval-util.h"
#include "runtime/free-pool.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#endif

#include "common/names.h"
#include "common/compiler-util.h"

using namespace impala;
using namespace impala_udf;
using std::pair;

const int FunctionContextImpl::VARARGS_BUFFER_ALIGNMENT;
const char* FunctionContextImpl::LLVM_FUNCTIONCONTEXT_NAME =
    "class.impala_udf::FunctionContext";
const char* FunctionContextImpl::GET_CONST_FN_ATTR_SYMBOL =
    "_ZN6impala19FunctionContextImpl14GetConstFnAttrENS0_11ConstFnAttrEi";

static const int MAX_WARNINGS = 1000;

static_assert(__BYTE_ORDER == __LITTLE_ENDIAN,
    "DecimalVal memory layout assumes little-endianness");

#if !defined(NDEBUG) && !defined(IMPALA_UDF_SDK_BUILD)
DECLARE_int32(stress_fn_ctx_alloc);

namespace {
/// Counter for tracking the number of allocations. Used only if the
/// the stress flag FLAGS_stress_fn_ctx_alloc is set.
AtomicInt32 alloc_counts(0);

bool FailNextAlloc() {
  return FLAGS_stress_fn_ctx_alloc > 0 &&
      (alloc_counts.Add(1) % FLAGS_stress_fn_ctx_alloc) == 0;
}
}
#endif

FunctionContext* FunctionContextImpl::CreateContext(RuntimeState* state,
    MemPool* udf_mem_pool, MemPool* results_pool,
    const FunctionContext::TypeDesc& return_type,
    const vector<FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size,
    bool debug) {
  FunctionContext::TypeDesc invalid_type;
  invalid_type.type = FunctionContext::INVALID_TYPE;
  invalid_type.precision = 0;
  invalid_type.scale = 0;
  return FunctionContextImpl::CreateContext(state, udf_mem_pool, results_pool,
      invalid_type, return_type, arg_types, varargs_buffer_size, debug);
}

FunctionContext* FunctionContextImpl::CreateContext(RuntimeState* state,
    MemPool* udf_mem_pool, MemPool* results_pool,
    const FunctionContext::TypeDesc& intermediate_type,
    const FunctionContext::TypeDesc& return_type,
    const vector<FunctionContext::TypeDesc>& arg_types, int varargs_buffer_size,
    bool debug) {
  impala_udf::FunctionContext* ctx = new impala_udf::FunctionContext();
  ctx->impl_->udf_pool_ = new FreePool(udf_mem_pool);
  ctx->impl_->results_pool_ = results_pool;
  ctx->impl_->state_ = state;
  ctx->impl_->intermediate_type_ = intermediate_type;
  ctx->impl_->return_type_ = return_type;
  ctx->impl_->arg_types_ = arg_types;
  ctx->impl_->varargs_buffer_ = reinterpret_cast<uint8_t*>(
      aligned_malloc(varargs_buffer_size, VARARGS_BUFFER_ALIGNMENT));
  ctx->impl_->varargs_buffer_size_ = varargs_buffer_size;
  ctx->impl_->debug_ = debug;
  ctx->impl_->functions_.ai_generate_text = impala::AiFunctions::AiGenerateText;
  ctx->impl_->functions_.ai_generate_text_default =
      impala::AiFunctions::AiGenerateTextDefault;
  VLOG_ROW << "Created FunctionContext: " << ctx;
  return ctx;
}

FunctionContext* FunctionContextImpl::Clone(
    MemPool* udf_mem_pool, MemPool* results_pool) {
  impala_udf::FunctionContext* new_context =
      CreateContext(state_, udf_mem_pool, results_pool, intermediate_type_,
          return_type_, arg_types_, varargs_buffer_size_, debug_);
  new_context->impl_->constant_args_ = constant_args_;
  new_context->impl_->fragment_local_fn_state_ = fragment_local_fn_state_;
  return new_context;
}

FunctionContext::FunctionContext() : impl_(new FunctionContextImpl(this)) {}

FunctionContext::~FunctionContext() {
  assert(impl_->closed_ && "FunctionContext wasn't closed!");
  delete impl_;
}

FunctionContextImpl::FunctionContextImpl(FunctionContext* parent)
  : varargs_buffer_(NULL),
    varargs_buffer_size_(0),
    context_(parent),
    udf_pool_(NULL),
    results_pool_(NULL),
    state_(NULL),
    debug_(false),
    version_(FunctionContext::v1_3),
    num_warnings_(0),
    num_updates_(0),
    num_removes_(0),
    thread_local_fn_state_(NULL),
    fragment_local_fn_state_(NULL),
    external_bytes_tracked_(0),
    closed_(false) {}

FunctionContextImpl::~FunctionContextImpl() {
  delete udf_pool_;
}

void FunctionContextImpl::Close() {
  if (closed_) return;

  stringstream error_ss;
  if (!debug_) {
    if (udf_pool_->net_allocations() > 0) {
      error_ss << "Memory leaked via FunctionContext::Allocate()";
    } else if (udf_pool_->net_allocations() < 0) {
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
  return impl_->state_->GetEffectiveUser().c_str();
}

FunctionContext::UniqueId FunctionContext::query_id() const {
  UniqueId id;
#if defined(IMPALA_UDF_SDK_BUILD) && IMPALA_UDF_SDK_BUILD
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
    uint8_t* buf, int64_t byte_size) {
  if (UNLIKELY(buf == NULL)) {
    stringstream ss;
    ss << string(fn_name) << "() failed to allocate " << byte_size << " bytes.";
    context_->SetError(ss.str().c_str());
    return false;
  }
  CheckMemLimit(fn_name, byte_size);
  return true;
}

inline void FunctionContextImpl::CheckMemLimit(const char* fn_name, int64_t byte_size) {
#ifndef IMPALA_UDF_SDK_BUILD
  MemTracker* mem_tracker = udf_pool_->mem_tracker();
  if (mem_tracker->AnyLimitExceeded(MemLimit::HARD)) {
    ErrorMsg msg = ErrorMsg(TErrorCode::UDF_MEM_LIMIT_EXCEEDED, string(fn_name));
    state_->SetMemLimitExceeded(mem_tracker, byte_size, &msg);
  }
#endif
}

uint8_t* FunctionContext::Allocate(int byte_size) noexcept {
  assert(!impl_->closed_);
#if !defined(NDEBUG) && !defined(IMPALA_UDF_SDK_BUILD)
  uint8_t* buffer = FailNextAlloc() ? nullptr : impl_->udf_pool_->Allocate(byte_size);
#else
  uint8_t* buffer = impl_->udf_pool_->Allocate(byte_size);
#endif
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

uint8_t* FunctionContext::Reallocate(uint8_t* ptr, int byte_size) noexcept {
  assert(!impl_->closed_);
  VLOG_ROW << "Reallocate: FunctionContext=" << this << " size=" << byte_size
           << " ptr=" << reinterpret_cast<void*>(ptr);
#if !defined(NDEBUG) && !defined(IMPALA_UDF_SDK_BUILD)
  uint8_t* new_ptr =
      FailNextAlloc() ? nullptr : impl_->udf_pool_->Reallocate(ptr, byte_size);
#else
  uint8_t* new_ptr = impl_->udf_pool_->Reallocate(ptr, byte_size);
#endif
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

void FunctionContext::Free(uint8_t* buffer) noexcept {
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
      impl_->udf_pool_->Free(buffer);
    } else {
      SetError("FunctionContext::Free() called on buffer that is already freed or was "
               "not allocated.");
    }
  } else {
    impl_->udf_pool_->Free(buffer);
  }
}

void FunctionContext::TrackAllocation(int64_t bytes) {
  assert(!impl_->closed_);
  impl_->external_bytes_tracked_ += bytes;
  impl_->udf_pool_->mem_tracker()->Consume(bytes);
  impl_->CheckMemLimit("FunctionContext::TrackAllocation", bytes);
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
  impl_->udf_pool_->mem_tracker()->Release(bytes);
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

const BuiltInFunctions* FunctionContext::Functions() const {
  return &impl_->functions_;
}

uint8_t* FunctionContextImpl::AllocateForResults(int64_t byte_size) noexcept {
  assert(!closed_);
#if !defined(NDEBUG) && !defined(IMPALA_UDF_SDK_BUILD)
  uint8_t* buffer = FailNextAlloc() ? nullptr : results_pool_->Allocate(byte_size);
#else
  uint8_t* buffer = results_pool_->Allocate(byte_size);
#endif
  if (UNLIKELY(
          !CheckAllocResult("FunctionContextImpl::AllocateForResults", buffer, byte_size))) {
    return NULL;
  }
  VLOG_ROW << "Allocate Results: FunctionContext=" << context_
           << " size=" << byte_size
           << " result=" << reinterpret_cast<void*>(buffer);
  return buffer;
}

void FunctionContextImpl::SetConstantArgs(vector<AnyVal*>&& constant_args) {
  constant_args_ = constant_args;
}

void FunctionContextImpl::SetNonConstantArgs(NonConstantArgsVector&& non_constant_args) {
  non_constant_args_ = non_constant_args;
}

// Note: this function crashes LLVM's JIT in expr-test if it's xcompiled. Do not move to
// expr-ir.cc. This could probably use further investigation.
StringVal::StringVal(FunctionContext* context, int str_len) noexcept : len(str_len),
                                                                       ptr(nullptr) {
  AllocateStringValWithLenCheck(context, str_len, this);
}

StringVal StringVal::CopyFrom(FunctionContext* ctx, const uint8_t* buf, size_t len) noexcept {
  StringVal result;
  AllocateStringValWithLenCheck(ctx, len, &result);
  if (LIKELY(!result.is_null)) {
    std::copy(buf, buf + len, result.ptr);
  }
  return result;
}

bool StringVal::Resize(FunctionContext* ctx, int new_len) noexcept {
  if (new_len <= len) {
    len = new_len;
    return true;
  }

  if (UNLIKELY(new_len > StringVal::MAX_LENGTH)) {
    ctx->SetError("String length larger than allowed limit of 1 GB character data.");
    len = 0;
    is_null = true;
    return false;
  }
  auto* new_ptr = ctx->impl()->AllocateForResults(new_len);
  if (new_ptr != nullptr) {
    memcpy(new_ptr, ptr, len);
    ptr = new_ptr;
    len = new_len;
    return true;
  }
  return false;
}

void StringVal::AllocateStringValWithLenCheck(FunctionContext* ctx, uint64_t str_len,
    StringVal* res) {
  if (UNLIKELY(str_len > StringVal::MAX_LENGTH)) {
    ctx->SetError("String length larger than allowed limit of 1 GB character data.");
    res->len = 0;
    res->is_null = true;
  } else {
    res->ptr = ctx->impl()->AllocateForResults(str_len);
    if (UNLIKELY(res->ptr == nullptr && str_len > 0)) {
#ifndef IMPALA_UDF_SDK_BUILD
      assert(!ctx->impl()->state()->GetQueryStatus().ok());
#endif
      res->len = 0;
      res->is_null = true;
    } else {
      res->len = str_len;
    }
  }
}

void StructVal::ReserveMemory(FunctionContext* ctx) {
  assert(ctx != nullptr);
  assert(num_children >= 0);
  assert(is_null == false);
  if (num_children == 0) return;
  ptr = reinterpret_cast<uint8_t**>(
      ctx->impl()->AllocateForResults(sizeof(uint8_t*) * num_children));
  if (UNLIKELY(ptr == nullptr)) {
    num_children = 0;
    is_null = true;
  }
}

// TODO: why doesn't libudasample.so build if this in udf-ir.cc?
const FunctionContext::TypeDesc* FunctionContext::GetArgType(int arg_idx) const {
  if (arg_idx < 0 || arg_idx >= impl_->arg_types_.size()) return NULL;
  return &impl_->arg_types_[arg_idx];
}

static int GetTypeByteSize(const FunctionContext::TypeDesc& type) {
#if defined(IMPALA_UDF_SDK_BUILD) && IMPALA_UDF_SDK_BUILD
  return 0;
#else
  return AnyValUtil::TypeDescToColumnType(type).GetByteSize();
#endif
}

int FunctionContextImpl::GetConstFnAttr(FunctionContextImpl::ConstFnAttr t, int i) {
  return GetConstFnAttr(state_->decimal_v2(), state_->utf8_mode(), return_type_,
      arg_types_, t, i);
}

int FunctionContextImpl::GetConstFnAttr(bool uses_decimal_v2, bool is_utf8_mode,
    const FunctionContext::TypeDesc& return_type,
    const vector<FunctionContext::TypeDesc>& arg_types, ConstFnAttr t, int i) {
  switch (t) {
    case RETURN_TYPE_SIZE:
      assert(i == -1);
      return GetTypeByteSize(return_type);
    case RETURN_TYPE_PRECISION:
      assert(i == -1);
      assert(return_type.type == FunctionContext::TYPE_DECIMAL);
      return return_type.precision;
    case RETURN_TYPE_SCALE:
      assert(i == -1);
      assert(return_type.type == FunctionContext::TYPE_DECIMAL);
      return return_type.scale;
    case ARG_TYPE_SIZE:
      assert(i >= 0);
      assert(i < arg_types.size());
      return GetTypeByteSize(arg_types[i]);
    case ARG_TYPE_PRECISION:
      assert(i >= 0);
      assert(i < arg_types.size());
      assert(arg_types[i].type == FunctionContext::TYPE_DECIMAL);
      return arg_types[i].precision;
    case ARG_TYPE_SCALE:
      assert(i >= 0);
      assert(i < arg_types.size());
      assert(arg_types[i].type == FunctionContext::TYPE_DECIMAL);
      return arg_types[i].scale;
    case DECIMAL_V2:
      return uses_decimal_v2;
    case UTF8_MODE:
      return is_utf8_mode;
    default:
      assert(false);
      return -1;
  }
}
