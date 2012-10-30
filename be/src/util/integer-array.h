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

#ifndef IMPALA_UTIL_INTEGER_ARRAY_H
#define IMPALA_UTIL_INGETER_ARRAY_H

#include "runtime/mem-pool.h"

namespace impala {

// Helper class to extract integers of a fixed number of bits from an array.
// The integers are packed into sequential words in memory.
// We assume the array is 4 byte aligned so that we can process the bits
// using integer masks.  This reduces the frequency of having to construct
// a value from two different bytes.
class IntegerArray {
 public:
  // bit_size - number of bits in each integer
  // count - number of integers
  // array - array of bytes containing the integers.  The array is owned by the caller.
  IntegerArray(int bit_size, int count, uint8_t* array);

  IntegerArray() : count_(0) { }
  
  // Return the next value in the array.
  // Returns 0 if there are no more values.
  uint32_t GetNextValue();

  // number of bytes needed to store 'count' integers of 'bit_size' bits.
  static int ArraySize(int bit_size, int count);

  // Number of bits needed to store the value passed in 'integer'.
  static int IntegerSize(int integer);
    
 private:
  friend class IntegerArrayBuilder;

  // Size of integer in bits.
  int bit_size_;
  
  // number of integers remaining in array
  int count_;

  // Array to hold the bits.
  uint8_t* array_;

  // Pointer to current offset.
  uint32_t* current_;

  // Mask of current value.
  int mask_;

  // Amount to shift the current masked value.
  int shift_;
};

// Class to incrementally build an integer array.
// TODO: There should be a call to reallocate the array to be bigger.
class IntegerArrayBuilder {
 public:
  // bit_size - number of bits in the integers
  // max_count - initial number of integers that will be put in the array.
  // mempool - memory pool to allocate the array in.
  IntegerArrayBuilder(int bit_size, int max_count, MemPool* mempool);

  // Constructor only used for vector initialization.
  IntegerArrayBuilder()
      : max_count_(0),
        mempool_(NULL) {
  }

  // Put an integer into the array
  // integer - the integer to put.
  // returns false if the array is full.
  bool Put(uint32_t integer);

  // Return the number of bytes occupied by the data in the array.
  int CurrentByteCount() {
    return IntegerArray::ArraySize(integer_array_.count_, integer_array_.bit_size_);
  }

  // Return a pointer to the allocated array.  
  uint8_t* array() { return integer_array_.array_; }
  int count() { return integer_array_.count_; }

 private:
  // maximum number of integers the array can hold
  int max_count_;

  // Memory pool to hold array.
  MemPool* mempool_;

  // Array to hold the bits.
  IntegerArray integer_array_;

  // Allocated size of the array.
  int array_size_;
};

}
#endif
