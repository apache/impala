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

#ifndef IMPALA_COMMON_ATOMIC_H
#define IMPALA_COMMON_ATOMIC_H

#include "common/compiler-util.h"

namespace impala {

class AtomicUtil {
 public:
  // Issues instruction to have the CPU wait, this is less busy (bus traffic
  // etc) than just spinning.
  // For example:
  //  while (1);
  // should be:
  //  while (1) CpuWait();
  static inline void CpuWait() {
    asm volatile("pause\n": : :"memory");
  }
};

// Wrapper for atomic integers.  This should be switched to c++ 11 when
// we can switch.
// This class overloads operators to behave like a regular integer type
// but all operators and functions are thread safe.
template<typename T>
class AtomicInt {
 public:
  AtomicInt(T initial = 0) : value_(initial) {}

  operator T() const { return value_; }

  AtomicInt& operator=(T val) { 
    value_ = val; 
    return *this;
  }
  AtomicInt& operator=(const AtomicInt<T>& val) { 
    value_ = val.value_; 
    return *this;
  }

  AtomicInt& operator+=(T delta) {
    __sync_add_and_fetch(&value_, delta);
    return *this;
  }
  AtomicInt& operator-=(T delta) {
    __sync_add_and_fetch(&value_, -delta);
    return *this;
  }
  
  // These define the preincrement (i.e. --value) operators.
  AtomicInt& operator++() {
    __sync_add_and_fetch(&value_, 1);
    return *this;
  }
  AtomicInt& operator--() {
    __sync_add_and_fetch(&value_, -1);
    return *this;
  }
  
  // This is post increment, which needs to return a new object.
  AtomicInt<T> operator++(int) {
    T prev = __sync_fetch_and_add(&value_, 1);
    return AtomicInt<T>(prev);
  }
  AtomicInt<T> operator--(int) {
    T prev = __sync_fetch_and_add(&value_, -1);
    return AtomicInt<T>(prev);
  }


  // Updates the int to 'value' if value is larger
  void UpdateMax(T value) {
    while (true) {
      T old_value = value_;
      T new_value = std::max(old_value, value);
      if (LIKELY(__sync_bool_compare_and_swap(&value_, old_value, new_value))) break;
    }
  }
  void UpdateMin(T value) {
    while (true) {
      T old_value = value_;
      T new_value = std::min(old_value, value);
      if (LIKELY(__sync_bool_compare_and_swap(&value_, old_value, new_value))) break;
    }
  }

 private:
  T value_;
};

}

#endif
