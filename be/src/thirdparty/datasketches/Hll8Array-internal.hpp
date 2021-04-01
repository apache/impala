/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _HLL8ARRAY_INTERNAL_HPP_
#define _HLL8ARRAY_INTERNAL_HPP_

#include "Hll8Array.hpp"

namespace datasketches {

template<typename A>
Hll8Array<A>::Hll8Array(const int lgConfigK, const bool startFullSize, const A& allocator):
HllArray<A>(lgConfigK, target_hll_type::HLL_8, startFullSize, allocator)
{
  const int numBytes = this->hll8ArrBytes(lgConfigK);
  this->hllByteArr.resize(numBytes, 0);
}

template<typename A>
std::function<void(HllSketchImpl<A>*)> Hll8Array<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    Hll8Array<A>* hll = static_cast<Hll8Array<A>*>(ptr);
    using Hll8Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>>;
    Hll8Alloc hll8Alloc(hll->getAllocator());
    hll->~Hll8Array();
    hll8Alloc.deallocate(hll, 1);
  };
}

template<typename A>
Hll8Array<A>* Hll8Array<A>::copy() const {
  using Hll8Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll8Array<A>>;
  Hll8Alloc hll8Alloc(this->getAllocator());
  return new (hll8Alloc.allocate(1)) Hll8Array<A>(*this);
}

template<typename A>
uint8_t Hll8Array<A>::getSlot(const int slotNo) const {
  return this->hllByteArr[slotNo];
}

template<typename A>
void Hll8Array<A>::putSlot(const int slotNo, uint8_t value) {
  this->hllByteArr[slotNo] = value;
}

template<typename A>
int Hll8Array<A>::getHllByteArrBytes() const {
  return this->hll8ArrBytes(this->lgConfigK);
}

template<typename A>
HllSketchImpl<A>* Hll8Array<A>::couponUpdate(int coupon) {
  internalCouponUpdate(coupon);
  return this;
}

template<typename A>
void Hll8Array<A>::internalCouponUpdate(int coupon) {
  const int configKmask = (1 << this->lgConfigK) - 1;
  const int slotNo = HllUtil<A>::getLow26(coupon) & configKmask;
  const int newVal = HllUtil<A>::getValue(coupon);

  const int curVal = getSlot(slotNo);
  if (newVal > curVal) {
    putSlot(slotNo, newVal);
    this->hipAndKxQIncrementalUpdate(curVal, newVal);
    if (curVal == 0) {
      this->numAtCurMin--; // interpret numAtCurMin as num zeros
    }
  }
}

template<typename A>
void Hll8Array<A>::mergeList(const CouponList<A>& src) {
  for (auto coupon: src) {
    internalCouponUpdate(coupon);
  }
}

template<typename A>
void Hll8Array<A>::mergeHll(const HllArray<A>& src) {
  // at this point src_k >= dst_k
  const int src_k = 1 << src.getLgConfigK();
  const  int dst_mask = (1 << this->getLgConfigK()) - 1;
  // duplication below is to avoid a virtual method call in a loop
  if (src.getTgtHllType() == target_hll_type::HLL_8) {
    for (int i = 0; i < src_k; i++) {
      const uint8_t new_v = static_cast<const Hll8Array<A>&>(src).getSlot(i);
      const int j = i & dst_mask;
      const uint8_t old_v = this->hllByteArr[j];
      if (new_v > old_v) {
        this->hllByteArr[j] = new_v;
        this->hipAndKxQIncrementalUpdate(old_v, new_v);
        if (old_v == 0) {
          this->numAtCurMin--;
        }
      }
    }
  } else if (src.getTgtHllType() == target_hll_type::HLL_6) {
    for (int i = 0; i < src_k; i++) {
      const uint8_t new_v = static_cast<const Hll6Array<A>&>(src).getSlot(i);
      const int j = i & dst_mask;
      const uint8_t old_v = this->hllByteArr[j];
      if (new_v > old_v) {
        this->hllByteArr[j] = new_v;
        this->hipAndKxQIncrementalUpdate(old_v, new_v);
        if (old_v == 0) {
          this->numAtCurMin--;
        }
      }
    }
  } else { // HLL_4
    for (int i = 0; i < src_k; i++) {
      const uint8_t new_v = static_cast<const Hll4Array<A>&>(src).get_value(i);
      const int j = i & dst_mask;
      const uint8_t old_v = this->hllByteArr[j];
      if (new_v > old_v) {
        this->hllByteArr[j] = new_v;
        this->hipAndKxQIncrementalUpdate(old_v, new_v);
        if (old_v == 0) {
          this->numAtCurMin--;
        }
      }
    }
  }
}

}

#endif // _HLL8ARRAY_INTERNAL_HPP_
