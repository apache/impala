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

#include <limits>
#include "common/logging.h"

#include "thirdparty/roaring/roaring.h"

namespace impala {

/// Simple wrapper around roaring bitmap. There's also a C++ version of the roaring
/// bitmap in CRoaring, the main reason for creating our own is to provide ContainsBulk().
class RoaringBitmap64 {
public:
   /// A bit of context usable with `*Bulk()` functions.
   /// A context may only be used with a single bitmap, and any modification to a
   /// bitmap (other than modifications performed with `Bulk()` functions with the
   /// context passed) will invalidate any contexts associated with that bitmap.
   /// Different threads need to use different contexts.
  class BulkContext {
    public:
      BulkContext() = default;
      BulkContext(const BulkContext &) = delete;
      BulkContext &operator=(const BulkContext &) = delete;
      BulkContext(BulkContext &&) noexcept = delete;
      BulkContext &operator=(BulkContext &&) noexcept = delete;

    private:
      friend RoaringBitmap64;
      roaring64_bulk_context_t context_ = {};
  };

  class Iterator {
    public:
      Iterator() {};

      // No copy, no move, no cry.
      Iterator(const Iterator&) = delete;
      Iterator &operator=(const Iterator&) = delete;
      Iterator(Iterator&& other) noexcept = delete;
      Iterator &operator=(Iterator&& other) noexcept = delete;

      Iterator(const RoaringBitmap64& roaring_bitmap) {
        Init(roaring_bitmap);
      }

      void Init(const RoaringBitmap64& roaring_bitmap) {
        DCHECK(it_ == nullptr);
        it_ = roaring64_iterator_create(roaring_bitmap.rbitmap_);
      }

      // Returns true if the iterator currently points to a value in the bitmap.
      // New iterators point to the first (minimum) element.
      // It returns false when we iterated over all elements (or bitmap is empty).
      bool HasValue() const {
        return roaring64_iterator_has_value(it_);
      }

      uint64_t Value() const {
        DCHECK(HasValue());
        return roaring64_iterator_value(it_);
      }

      void Advance() {
        DCHECK(HasValue());
        roaring64_iterator_advance(it_);
      }

      // Sets iterator to the next element that is equal or larger to 'x', and
      // returns the element. If no such element is found then return INT64_MAX.
      // Incoming 'x' values must be in ascending order.
      uint64_t GetEqualOrLarger(uint64_t x) {
        // MOVE_THRESHOLD was chosen empirically. If 10% of the data is deleted and
        // if the distance is 30 then AdvanceAndGetEqualOrLarger() should find the next
        // element in ~3 iterations. The point of it is we shouldn't use
        // AdvanceAndGetEqualOrLarger() when the distance is large and the bitmap is
        // dense. The distance is typically large when we start processing a new
        // probe batch (we are in the middle of a file, but we have a new iterator).
        constexpr int MOVE_THRESHOLD = 30;
        // We need to choose between MoveAndGetEqualOrLarger() and
        // AdvanceAndGetEqualOrLarger(). MoveAndGetEqualOrLarger() is more efficient
        // when the distance between 'x' and iterator's current element is large.
        // AdvanceAndGetEqualOrLarger() is more efficient when the distance is small.
        // There are 5 possible cases:
        // - We start a new probe batch:
        //   => For the first probe batch we likely choose AdvanceAndGetEqualOrLarger()
        //      (unless it starts with a high file position), but we choose
        //      MoveAndGetEqualOrLarger() for subsequent probe batches. Both should be
        //      efficient.
        // - Incoming x values are sparse, Bitmap is sparse:
        //   => MoveAndGetEqualOrLarger() gets chosen most of the time. This sould give
        //      us OK performance, also this method won't be invoked that much in this
        //      case.
        // - Incoming x values are dense, Bitmap is sparse:
        //   => AdvanceAndGetEqualOrLarger() gets chosen most of the time which should
        //      find the next element in 1-2 iterations (as input is dense, bitmap is
        //      sparse). Also, this won't be invoked that much since the bitmap is sparse.
        // - Incoming x values are sparse, Bitmap is dense
        //   => MoveAndGetEqualOrLarger() gets chosen which is the efficient one for
        //      this case.
        // - Incoming x values are dense, Bitmap is dense
        //   => AdvanceAndGetEqualOrLarger() gets chosen which is the optimal choice
        //      for this case.
        if (HasValue() && x - Value() > MOVE_THRESHOLD) {
          return MoveAndGetEqualOrLarger(x);
        } else {
          return AdvanceAndGetEqualOrLarger(x);
        }
      }

      ~Iterator() {
        roaring64_iterator_free(it_);
      }

    private:
      static constexpr uint64_t MAX_VALUE = std::numeric_limits<uint64_t>::max();

      // Moves 'this' to an element that is equal or larger to 'x'.
      uint64_t MoveAndGetEqualOrLarger(uint64_t x) {
        if (roaring64_iterator_move_equalorlarger(it_, x)) {
          return Value();
        } else {
          return MAX_VALUE;
        }
      }

      // Advances 'this' until we find an element which is equal or larger to 'x'.
      uint64_t AdvanceAndGetEqualOrLarger(uint64_t x) {
        while (HasValue() && Value() < x) {
          Advance();
        }
        return HasValue() ? Value() : MAX_VALUE;
      }

      roaring64_iterator_t *it_ = nullptr;
  };

  RoaringBitmap64() = default;
  ~RoaringBitmap64() { roaring64_bitmap_free(rbitmap_); }

  RoaringBitmap64(const RoaringBitmap64 &) = delete;
  RoaringBitmap64 &operator=(const RoaringBitmap64 &) = delete;
  RoaringBitmap64(RoaringBitmap64 &&) noexcept = delete;
  RoaringBitmap64 &operator=(RoaringBitmap64 &&) noexcept = delete;

  void AddElements(const std::vector<uint64_t>& elements) {
    roaring64_bitmap_add_many(rbitmap_, elements.size(), elements.data());
  }

  bool ContainsBulk(uint64_t x, BulkContext* context) const {
    return roaring64_bitmap_contains_bulk(rbitmap_, &context->context_, x);
  }

  bool IsEmpty() const { return roaring64_bitmap_is_empty(rbitmap_); }
  uint64_t Min() const { return roaring64_bitmap_minimum(rbitmap_); }
  uint64_t Max() const { return roaring64_bitmap_maximum(rbitmap_); }

private:
  roaring64_bitmap_t* rbitmap_ = roaring64_bitmap_create();
};

}
