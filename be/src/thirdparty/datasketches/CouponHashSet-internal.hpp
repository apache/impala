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

#ifndef _COUPONHASHSET_INTERNAL_HPP_
#define _COUPONHASHSET_INTERNAL_HPP_

#include "CouponHashSet.hpp"

#include <cstring>
#include <exception>

namespace datasketches {

template<typename A>
static int find(const int* array, const int lgArrInts, const int coupon);

template<typename A>
CouponHashSet<A>::CouponHashSet(const int lgConfigK, const target_hll_type tgtHllType, const A& allocator)
  : CouponList<A>(lgConfigK, tgtHllType, hll_mode::SET, allocator)
{
  if (lgConfigK <= 7) {
    throw std::invalid_argument("CouponHashSet must be initialized with lgConfigK > 7. Found: "
                                + std::to_string(lgConfigK));
  }
}

template<typename A>
CouponHashSet<A>::CouponHashSet(const CouponHashSet<A>& that, const target_hll_type tgtHllType)
  : CouponList<A>(that, tgtHllType) {}

template<typename A>
std::function<void(HllSketchImpl<A>*)> CouponHashSet<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    CouponHashSet<A>* chs = static_cast<CouponHashSet<A>*>(ptr);
    ChsAlloc chsa(chs->getAllocator());
    chs->~CouponHashSet();
    chsa.deallocate(chs, 1);
  };
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::newSet(const void* bytes, size_t len, const A& allocator) {
  if (len < HllUtil<A>::HASH_SET_INT_ARR_START) { // hard-coded 
    throw std::out_of_range("Input data length insufficient to hold CouponHashSet");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::HASH_SET_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  const hll_mode mode = HllSketchImpl<A>::extractCurMode(data[HllUtil<A>::MODE_BYTE]);
  if (mode != SET) {
    throw std::invalid_argument("Calling set constructor with non-set mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(data[HllUtil<A>::MODE_BYTE]);

  const int lgK = data[HllUtil<A>::LG_K_BYTE];
  if (lgK <= 7) {
    throw std::invalid_argument("Attempt to deserialize invalid CouponHashSet with lgConfigK <= 7. Found: "
                                + std::to_string(lgK));
  }   
  int lgArrInts = data[HllUtil<A>::LG_ARR_BYTE];
  const bool compactFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);

  int couponCount;
  std::memcpy(&couponCount, data + HllUtil<A>::HASH_SET_COUNT_INT, sizeof(couponCount));
  if (lgArrInts < HllUtil<A>::LG_INIT_SET_SIZE) { 
    lgArrInts = HllUtil<A>::computeLgArrInts(SET, couponCount, lgK);
  }
  // Don't set couponCount in sketch here;
  // we'll set later if updatable, and increment with updates if compact
  const int couponsInArray = (compactFlag ? couponCount : (1 << lgArrInts));
  const size_t expectedLength = HllUtil<A>::HASH_SET_INT_ARR_START + (couponsInArray * sizeof(int));
  if (len < expectedLength) {
    throw std::out_of_range("Byte array too short for sketch. Expected " + std::to_string(expectedLength)
                                + ", found: " + std::to_string(len));
  }

  ChsAlloc chsa(allocator);
  CouponHashSet<A>* sketch = new (chsa.allocate(1)) CouponHashSet<A>(lgK, tgtHllType, allocator);

  if (compactFlag) {
    const uint8_t* curPos = data + HllUtil<A>::HASH_SET_INT_ARR_START;
    int coupon;
    for (int i = 0; i < couponCount; ++i, curPos += sizeof(coupon)) {
      std::memcpy(&coupon, curPos, sizeof(coupon));
      sketch->couponUpdate(coupon);
    }
  } else {
    sketch->coupons.resize(1 << lgArrInts);
    sketch->couponCount = couponCount;
    // only need to read valid coupons, unlike in stream case
    std::memcpy(sketch->coupons.data(),
                data + HllUtil<A>::HASH_SET_INT_ARR_START,
                couponCount * sizeof(int));
  }

  return sketch;
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::newSet(std::istream& is, const A& allocator) {
  uint8_t listHeader[8];
  is.read((char*)listHeader, 8 * sizeof(uint8_t));

  if (listHeader[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::HASH_SET_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(listHeader[HllUtil<A>::MODE_BYTE]);
  if (mode != SET) {
    throw std::invalid_argument("Calling set constructor with non-set mode data");
  }

  target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(listHeader[HllUtil<A>::MODE_BYTE]);

  const int lgK = listHeader[HllUtil<A>::LG_K_BYTE];
  if (lgK <= 7) {
    throw std::invalid_argument("Attempt to deserialize invalid CouponHashSet with lgConfigK <= 7. Found: "
                                + std::to_string(lgK));
  }
  int lgArrInts = listHeader[HllUtil<A>::LG_ARR_BYTE];
  const bool compactFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);

  int couponCount;
  is.read((char*)&couponCount, sizeof(couponCount));
  if (lgArrInts < HllUtil<A>::LG_INIT_SET_SIZE) { 
    lgArrInts = HllUtil<A>::computeLgArrInts(SET, couponCount, lgK);
  }

  ChsAlloc chsa(allocator);
  CouponHashSet<A>* sketch = new (chsa.allocate(1)) CouponHashSet<A>(lgK, tgtHllType, allocator);
  typedef std::unique_ptr<CouponHashSet<A>, std::function<void(HllSketchImpl<A>*)>> coupon_hash_set_ptr;
  coupon_hash_set_ptr ptr(sketch, sketch->get_deleter());

  // Don't set couponCount here;
  // we'll set later if updatable, and increment with updates if compact
  if (compactFlag) {
    for (int i = 0; i < couponCount; ++i) {
      int coupon;
      is.read((char*)&coupon, sizeof(coupon));
      sketch->couponUpdate(coupon);
    }
  } else {
    sketch->coupons.resize(1 << lgArrInts);
    sketch->couponCount = couponCount;
    // for stream processing, read entire list so read pointer ends up set correctly
    is.read((char*)sketch->coupons.data(), sketch->coupons.size() * sizeof(int));
  } 

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return ptr.release();
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::copy() const {
  ChsAlloc chsa(this->coupons.get_allocator());
  return new (chsa.allocate(1)) CouponHashSet<A>(*this);
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::copyAs(const target_hll_type tgtHllType) const {
  ChsAlloc chsa(this->coupons.get_allocator());
  return new (chsa.allocate(1)) CouponHashSet<A>(*this, tgtHllType);
}

template<typename A>
HllSketchImpl<A>* CouponHashSet<A>::couponUpdate(int coupon) {
  const uint8_t lgCouponArrInts = count_trailing_zeros_in_u32(this->coupons.size());
  const int index = find<A>(this->coupons.data(), lgCouponArrInts, coupon);
  if (index >= 0) {
    return this; // found duplicate, ignore
  }
  this->coupons[~index] = coupon; // found empty
  ++this->couponCount;
  if (checkGrowOrPromote()) {
    return this->promoteHeapListOrSetToHll(*this);
  }
  return this;
}

template<typename A>
int CouponHashSet<A>::getMemDataStart() const {
  return HllUtil<A>::HASH_SET_INT_ARR_START;
}

template<typename A>
int CouponHashSet<A>::getPreInts() const {
  return HllUtil<A>::HASH_SET_PREINTS;
}

template<typename A>
bool CouponHashSet<A>::checkGrowOrPromote() {
  if (static_cast<size_t>(HllUtil<A>::RESIZE_DENOM * this->couponCount) > (HllUtil<A>::RESIZE_NUMER * this->coupons.size())) {
    const uint8_t lgCouponArrInts = count_trailing_zeros_in_u32(this->coupons.size());
    if (lgCouponArrInts == (this->lgConfigK - 3)) { // at max size
      return true; // promote to HLL
    }
    growHashSet(lgCouponArrInts + 1);
  }
  return false;
}

template<typename A>
void CouponHashSet<A>::growHashSet(int tgtLgCoupArrSize) {
  const int tgtLen = 1 << tgtLgCoupArrSize;
  vector_int coupons_new(tgtLen, 0, this->coupons.get_allocator());

  const int srcLen = this->coupons.size();
  for (int i = 0; i < srcLen; ++i) { // scan existing array for non-zero values
    const int fetched = this->coupons[i];
    if (fetched != HllUtil<A>::EMPTY) {
      const int idx = find<A>(coupons_new.data(), tgtLgCoupArrSize, fetched); // search TGT array
      if (idx < 0) { // found EMPTY
        coupons_new[~idx] = fetched; // insert
        continue;
      }
      throw std::runtime_error("Error: Found duplicate coupon");
    }
  }
  this->coupons = std::move(coupons_new);
}

template<typename A>
static int find(const int* array, const int lgArrInts, const int coupon) {
  const int arrMask = (1 << lgArrInts) - 1;
  int probe = coupon & arrMask;
  const int loopIndex = probe;
  do {
    const int couponAtIdx = array[probe];
    if (couponAtIdx == HllUtil<A>::EMPTY) {
      return ~probe; //empty
    }
    else if (coupon == couponAtIdx) {
      return probe; //duplicate
    }
    const int stride = ((coupon & HllUtil<A>::KEY_MASK_26) >> lgArrInts) | 1;
    probe = (probe + stride) & arrMask;
  } while (probe != loopIndex);
  throw std::invalid_argument("Key not found and no empty slots!");
}

}

#endif // _COUPONHASHSET_INTERNAL_HPP_
