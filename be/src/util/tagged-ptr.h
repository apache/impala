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

#pragma once

#include <cstdint>
#include <utility>
#include "common/compiler-util.h"
#include "common/logging.h"

namespace impala {

/// Most OS uses paging to translate linear address (address used by software) to actual
/// physical address. Intel 64 bit processors restricts linear address to 48 bits and
/// with paging level 5 will extend it to 57 bits. Pointers used to store these addresses
/// are 64 bit long. 0 to 56 bits will be storing the linear address. Rest of the bits
/// can be used to store extra information to save memory. For example booleans in
/// buckets of HashTable can be folded into next pointer. But there is a caveat:
/// Rest of the bits i.e., 57 to 63 needs to be identical and same as 56th bit. Hence, it
/// is called 57-bit canonocal. address too. 48-bit canonical address is also 57-bit
/// canonical. For 64-bit address, processor first confirms if it is in canonical format
/// otherwise it would fail. This is the utility class that would help storing tags in
/// pointer at bit positions 57..63. Tags are stored from most significant bit to lower
/// bits. Tag bit 0 - corresponds to bit 63, bit 1 corresponds to 62 and so on.
/// Another point to be noted is that User space in both X86 and ARM are in the lower
/// half of virtual space so they will always have top bits as 0. For instance in AArch64
/// Linux with 4KB pages with 4 levels (48-bit) user space is between
/// 0000000000000000 - 0000ffffffffffff. This implementation assumes that.
/// Note on inheritance:
/// We can create it's derived classes, but virtual functions should not be declared here
/// as that would include pointer to virtual table defeating the purpose of saving space.
/// Derived class would be needed to define what each tag bit would mean or when
/// allocation/deallocation of the object to which pointer stored points to should be
/// the responsibility of client and not 'TaggedPtr'( check HashTable::TaggedBucketData).

template <class T, bool OWNS = true>
class TaggedPtr {
 public:
  TaggedPtr() = default;

  template <class... Args>
  static TaggedPtr make_tagptr(Args&&... args) {
    T* ptr = new T(std::forward<Args>(args)...);
    return TaggedPtr(ptr);
  }

  // Define move constructor and move assignment
  TaggedPtr(TaggedPtr&& other) : data_(std::exchange(other.data_, 0)) {}
  TaggedPtr<T, OWNS>& operator=(TaggedPtr<T, OWNS>&& other) noexcept {
    if (this != &other) {
      data_ = std::exchange(other.data_, 0);
    }
    return *this;
  }

  ~TaggedPtr() {
    if (OWNS) {
      // Cleanup if owning the pointer
      T* ptr = GetPtr();
      // will not work for arrays, but arrays are not yet supported.
      if (ptr) delete ptr;
    }
  }

  template <uint8_t bit>
  static constexpr uintptr_t DATA_MASK = 1ULL << (63 - bit);

  template <uint8_t bit>
  static constexpr uintptr_t DATA_MASK_INVERSE = ~DATA_MASK<bit>;

  template <uint8_t bit>
  ALWAYS_INLINE void SetTagBit() {
    static_assert(bit >= 0 && bit < 7, "'bit' must be in range 0 to 6 (inclusive).");
    data_ = (data_ | DATA_MASK<bit>);
  }

  template <uint8_t bit>
  ALWAYS_INLINE void UnsetTagBit() {
    static_assert(bit >= 0 && bit < 7, "'bit' must be in range 0 to 6 (inclusive).");
    data_ = (data_ & DATA_MASK_INVERSE<bit>);
  }

  template <uint8_t bit>
  ALWAYS_INLINE bool IsTagBitSet() {
    static_assert(bit >= 0 && bit < 7, "'bit' must be in range 0 to 6 (inclusive).");
    return data_ & DATA_MASK<bit>;
  }

  ALWAYS_INLINE bool IsNull() { return GetPtr() == 0; }

  ALWAYS_INLINE int GetTag() const { return data_ >> 57; }

  ALWAYS_INLINE T* GetPtr() const { return (T*)(data_ & MASK_0_56_BITS); }

  T& operator*() const noexcept { return *GetPtr(); }

  T* operator->() const noexcept { return GetPtr(); }

  bool operator!() const noexcept { return GetPtr() == 0; }

  bool operator==(const TaggedPtr<T>& a) noexcept { return data_ == a.data_; }

  bool operator!=(const TaggedPtr<T>& a) noexcept { return data_ != a.data_; }

 private:
  TaggedPtr(T* ptr) { SetPtr(ptr); }

  uintptr_t data_ = 0;
  static constexpr uintptr_t MASK_56_BIT = (1ULL << 56);
  static constexpr uintptr_t MASK_0_56_BITS = (1ULL << 57) - 1;
  static constexpr uintptr_t MASK_0_56_BITS_INVERSE = ~MASK_0_56_BITS;

 protected:
  // Don't use unless client wants to retain the ownership of pointer.
  ALWAYS_INLINE void SetPtr(T* ptr) {
    data_ = (data_ & MASK_0_56_BITS_INVERSE) | (reinterpret_cast<uintptr_t>(ptr));
  }
  ALWAYS_INLINE void SetData(uintptr_t data) { data_ = data; }
  ALWAYS_INLINE uintptr_t GetData() { return data_; }
  // No copies allowed to ensure no leaking of ownership.
  // Derived classes can opt to enable it.
  TaggedPtr(const TaggedPtr&) = default;
  TaggedPtr& operator=(const TaggedPtr&) = default;
};
}; // namespace impala