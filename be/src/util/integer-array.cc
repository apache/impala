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

#include "util/integer-array.h"

namespace impala {

int IntegerArray::ArraySize(int bit_size, int count) {
  int array_size = (count * bit_size) / 8;
  if (((count * bit_size) % 8) != 0) ++array_size;
  return array_size;
}

int IntegerArray::IntegerSize(int integer) {
  if (integer == 0)
    return (0);
  int bit_size = 1;
  while ((integer >>= 1) != 0) ++bit_size;
  return bit_size;
}

IntegerArray::IntegerArray(int bit_size, int count, uint8_t* array) 
    : bit_size_(bit_size),
      count_(count),
      array_(array),
      current_(reinterpret_cast<uint32_t*>(array)),
      mask_((1 << bit_size_) - 1),
  shift_(0) { 
}

uint32_t IntegerArray::GetNextValue() {
  if (count_ == 0) {
    return 0;
  }

  // TODO: look into performance of this.  Would recalculating a set
  // of bit masks help?
  if (shift_ + bit_size_ > 8 * sizeof(uint32_t)) {
    // move to next batch.
    if (shift_ == 8 * sizeof(uint32_t)) {
      // good fit.
      ++current_;
      mask_ = (1 << bit_size_) - 1;
      shift_ = 0;
    } else {
      // Grab the remaining bits.
      uint32_t value = (*current_ & mask_) >> shift_;

      // Move to the next word. 
      ++current_;
      // Adjust mask to bits in next word.
      mask_ = (1 << bit_size_) - 1;
      mask_ >>= (8 * sizeof(uint32_t)) - shift_;
      // Shift up the bits from the next word.
      value += (*current_ & mask_) <<  ((8 * sizeof(uint32_t)) - shift_);

      // Set the mask and shift for the next integer.
      shift_ = bit_size_ - ((8 * sizeof(uint32_t)) - shift_);
      mask_ = ((1 << bit_size_) - 1) << shift_;

      return value;
    }
  }

  uint32_t value = (*current_ & mask_) >> shift_;
  shift_ += bit_size_;
  mask_ <<= bit_size_;
  --count_;
  return value;
}

IntegerArrayBuilder::IntegerArrayBuilder(int bit_size, int max_count, MemPool* mempool)
  : max_count_(max_count),
    mempool_(mempool) { 
  array_size_ = IntegerArray::ArraySize(bit_size, max_count);
  integer_array_ = IntegerArray(bit_size, 0, mempool->Allocate(array_size_)); 
  memset(integer_array_.array_, 0, array_size_);
}

bool IntegerArrayBuilder::Put(uint32_t integer) {
  DCHECK(integer_array_.array_ != NULL);
  if (integer_array_.count_ == max_count_) {
    return (false);
  }

  // TODO: evaluate alternate algorithms for performance.
  DCHECK_LT(integer, 1 << integer_array_.bit_size_);
  // Put the bits in the current word. If the high bits don't fit they
  // will just fall on the floor. We pick them up below.
  *integer_array_.current_ |= integer << integer_array_.shift_;
  ++integer_array_.count_;

  // Is the current word full?
  if (integer_array_.shift_ + integer_array_.bit_size_ >= 8 * sizeof(uint32_t)) {
    // move to next word.
    ++integer_array_.current_;
    if (integer_array_.shift_ +integer_array_.bit_size_ == 8 * sizeof(uint32_t)) {
      // good fit.
      integer_array_.shift_ = 0;
    } else {
      // Pick up  the remaining bits.
      // Shift down the remaining bits into the next word.
      *integer_array_.current_ =
          integer >> ((8 * sizeof(uint32_t)) - integer_array_.shift_);

      // Set the shift for the next integer.
      integer_array_.shift_ =
          integer_array_.bit_size_ - (8 * (sizeof(uint32_t)) - integer_array_.shift_);
    }
  } else {
    integer_array_.shift_ += integer_array_.bit_size_;
  }
  return true;
}
}
