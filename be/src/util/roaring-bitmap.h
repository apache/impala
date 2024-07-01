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
