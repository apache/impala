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

#ifndef IMPALA_COMMON_ATOMIC_H
#define IMPALA_COMMON_ATOMIC_H

#include <type_traits>

#include "common/compiler-util.h"
#include "gutil/atomicops.h"
#include "gutil/macros.h"

namespace impala {

class AtomicUtil {
 public:
  /// Issues instruction to have the CPU wait, this is less busy (bus traffic
  /// etc) than just spinning.
  /// For example:
  ///  while (1);
  /// should be:
  ///  while (1) CpuWait();
  static ALWAYS_INLINE void CpuWait() {
    base::subtle::PauseCPU();
  }

  /// Provides "barrier" semantics (see below) without a memory access.
  static ALWAYS_INLINE void MemoryBarrier() {
    base::subtle::MemoryBarrier();
  }

  /// Provides a compiler barrier. The compiler is not allowed to reorder memory
  /// accesses across this (but the CPU can).  This generates no instructions.
  static ALWAYS_INLINE void CompilerBarrier() {
    __asm__ __volatile__("" : : : "memory");
  }
};

namespace internal {

/// Atomic integer. This class template should not be used directly; instead use the
/// typedefs below. 'T' can be either 32-bit or 64-bit signed integer. Each operation
/// is performed atomically and has a specified memory-ordering semantic:
///
/// Acquire: these operations ensure no later memory access by the same thread can be
/// reordered ahead of the operation. (C++11: memory_order_relaxed)
///
/// Release: these operations ensure that no previous memory access by the same thread
/// can be reordered after the operation (C++11: memory_order_release).
///
/// Barrier: these operations have both Acquire and Release semantics (C++11:
/// memory_order_acq_rel).
///
/// NoBarrier: these operations do not guarantee any ordering (C++11:
/// memory_order_relaxed). The compiler/CPU is free to reorder memory accesses (as seen
/// by other threads) just like any normal variable.
///
template<typename T>
class AtomicInt {
 public:
  AtomicInt(T initial = 0) : value_(initial) {
    static_assert(sizeof(T) == sizeof(base::subtle::Atomic32) ||
        sizeof(T) == sizeof(base::subtle::Atomic64),
            "Only AtomicInt32 and AtomicInt64 are implemented");
  }

  /// Atomic load with "acquire" memory-ordering semantic.
  ALWAYS_INLINE T Load() const {
    return base::subtle::Acquire_Load(&value_);
  }

  /// Atomic store with "release" memory-ordering semantic.
  ALWAYS_INLINE void Store(T x) {
    base::subtle::Release_Store(&value_, x);
  }

  /// Atomic add with "barrier" memory-ordering semantic. Returns the new value.
  ALWAYS_INLINE T Add(T x) {
    return base::subtle::Barrier_AtomicIncrement(&value_, x);
  }

  /// Atomically compare 'old_val' to 'value_' and set 'value_' to 'new_val' and return
  /// true if they compared equal, otherwise return false (and do no updates), with
  /// "barrier" memory-ordering semantic. That is, atomically performs:
  ///  if (value_ == old_val) {
  ///     value_ = new_val;
  ///     return true;
  ///  }
  ///  return false;
  ALWAYS_INLINE bool CompareAndSwap(T old_val, T new_val) {
    return base::subtle::Barrier_CompareAndSwap(&value_, old_val, new_val) == old_val;
  }

  /// Store 'new_val' and return the previous value. Implies a Release memory barrier
  /// (i.e. the same as Store()).
  ALWAYS_INLINE T Swap(T new_val) {
    return base::subtle::Release_AtomicExchange(&value_, new_val);
  }

 private:
  T value_;

  DISALLOW_COPY_AND_ASSIGN(AtomicInt);
};

} // namespace internal

/// Supported atomic types. Use these types rather than referring to AtomicInt<>
/// directly.
typedef internal::AtomicInt<int32_t> AtomicInt32;
typedef internal::AtomicInt<int64_t> AtomicInt64;

/// Atomic pointer. Operations have the same semantics as AtomicInt.
template<typename T>
class AtomicPtr {
 public:
  AtomicPtr(T* initial = nullptr) : ptr_(reinterpret_cast<intptr_t>(initial)) {}

  /// Atomic load with "acquire" memory-ordering semantic.
  ALWAYS_INLINE T* Load() const { return reinterpret_cast<T*>(ptr_.Load()); }

  /// Atomic store with "release" memory-ordering semantic.
  ALWAYS_INLINE void Store(T* val) { ptr_.Store(reinterpret_cast<intptr_t>(val)); }

  /// Store 'new_val' and return the previous value. Implies a Release memory barrier
  /// (i.e. the same as Store()).
  ALWAYS_INLINE T* Swap(T* val) {
    return reinterpret_cast<T*>(ptr_.Swap(reinterpret_cast<intptr_t>(val)));
  }
 private:
  internal::AtomicInt<intptr_t> ptr_;
};

/// Atomic enum. Operations have the same semantics as AtomicInt.
template<typename T>
class AtomicEnum {
  static_assert(std::is_enum<T>::value, "Type must be enum");
  static_assert(sizeof(typename std::underlying_type<T>::type) <= sizeof(int32_t),
      "Underlying enum type must fit into 4 bytes");

 public:
  AtomicEnum(T initial) : enum_(static_cast<int32_t>(initial)) {}
  /// Atomic load with "acquire" memory-ordering semantic.
  ALWAYS_INLINE T Load() const { return static_cast<T>(enum_.Load()); }

  /// Atomic store with "release" memory-ordering semantic.
  ALWAYS_INLINE void Store(T val) { enum_.Store(static_cast<int32_t>(val)); }

 private:
  internal::AtomicInt<int32_t> enum_;
};

/// Atomic bool. Operations have the same semantics as AtomicInt.
class AtomicBool {
 public:
  AtomicBool(bool initial = false) : boolean_(initial) {}

  /// Atomic load with "acquire" memory-ordering semantic.
  ALWAYS_INLINE bool Load() const { return boolean_.Load(); }

  /// Atomic store with "release" memory-ordering semantic.
  ALWAYS_INLINE void Store(bool val) { boolean_.Store(val); }

  ALWAYS_INLINE bool CompareAndSwap(bool old_val, bool new_val) {
    return boolean_.CompareAndSwap(old_val, new_val);
  }

 private:
  internal::AtomicInt<int32_t> boolean_;
};

}

#endif
