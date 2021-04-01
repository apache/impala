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

#ifndef _HLL6ARRAY_INTERNAL_HPP_
#define _HLL6ARRAY_INTERNAL_HPP_

#include <cstring>

#include "Hll6Array.hpp"

namespace datasketches {

template<typename A>
Hll6Array<A>::Hll6Array(const int lgConfigK, const bool startFullSize, const A& allocator):
HllArray<A>(lgConfigK, target_hll_type::HLL_6, startFullSize, allocator)
{
  const int numBytes = this->hll6ArrBytes(lgConfigK);
  this->hllByteArr.resize(numBytes, 0);
}

template<typename A>
std::function<void(HllSketchImpl<A>*)> Hll6Array<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    using Hll6Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll6Array<A>>;
    Hll6Array<A>* hll = static_cast<Hll6Array<A>*>(ptr);
    Hll6Alloc hll6Alloc(hll->getAllocator());
    hll->~Hll6Array();
    hll6Alloc.deallocate(hll, 1);
  };
}

template<typename A>
Hll6Array<A>* Hll6Array<A>::copy() const {
  using Hll6Alloc = typename std::allocator_traits<A>::template rebind_alloc<Hll6Array<A>>;
  Hll6Alloc hll6Alloc(this->getAllocator());
  return new (hll6Alloc.allocate(1)) Hll6Array<A>(*this);
}

template<typename A>
uint8_t Hll6Array<A>::getSlot(int slotNo) const {
  const int startBit = slotNo * 6;
  const int shift = startBit & 0x7;
  const int byteIdx = startBit >> 3;  
  const uint16_t twoByteVal = (this->hllByteArr[byteIdx + 1] << 8) | this->hllByteArr[byteIdx];
  return (twoByteVal >> shift) & HllUtil<A>::VAL_MASK_6;
}

template<typename A>
void Hll6Array<A>::putSlot(int slotNo, uint8_t value) {
  const int startBit = slotNo * 6;
  const int shift = startBit & 0x7;
  const int byteIdx = startBit >> 3;
  const uint16_t valShifted = (value & 0x3F) << shift;
  uint16_t curMasked = (this->hllByteArr[byteIdx + 1] << 8) | this->hllByteArr[byteIdx];
  curMasked &= (~(HllUtil<A>::VAL_MASK_6 << shift));
  const uint16_t insert = curMasked | valShifted;
  this->hllByteArr[byteIdx]     = insert & 0xFF;
  this->hllByteArr[byteIdx + 1] = (insert & 0xFF00) >> 8;
}

template<typename A>
int Hll6Array<A>::getHllByteArrBytes() const {
  return this->hll6ArrBytes(this->lgConfigK);
}

template<typename A>
HllSketchImpl<A>* Hll6Array<A>::couponUpdate(const int coupon) {
  internalCouponUpdate(coupon);
  return this;
}

template<typename A>
void Hll6Array<A>::internalCouponUpdate(const int coupon) {
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
void Hll6Array<A>::mergeHll(const HllArray<A>& src) {
  for (auto coupon: src) {
    internalCouponUpdate(coupon);
  }
}

}

#endif // _HLL6ARRAY_INTERNAL_HPP_
