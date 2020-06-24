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
CouponHashSet<A>::CouponHashSet(const int lgConfigK, const target_hll_type tgtHllType)
  : CouponList<A>(lgConfigK, tgtHllType, hll_mode::SET)
{
  if (lgConfigK <= 7) {
    throw std::invalid_argument("CouponHashSet must be initialized with lgConfigK > 7. Found: "
                                + std::to_string(lgConfigK));
  }
}

template<typename A>
CouponHashSet<A>::CouponHashSet(const CouponHashSet<A>& that)
  : CouponList<A>(that) {}

template<typename A>
CouponHashSet<A>::CouponHashSet(const CouponHashSet<A>& that, const target_hll_type tgtHllType)
  : CouponList<A>(that, tgtHllType) {}

template<typename A>
CouponHashSet<A>::~CouponHashSet() {}

template<typename A>
std::function<void(HllSketchImpl<A>*)> CouponHashSet<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    CouponHashSet<A>* chs = static_cast<CouponHashSet<A>*>(ptr);
    chs->~CouponHashSet();
    chsAlloc().deallocate(chs, 1);
  };
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::newSet(const void* bytes, size_t len) {
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
    throw std::invalid_argument("Calling set construtor with non-set mode data");
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

  CouponHashSet<A>* sketch = new (chsAlloc().allocate(1)) CouponHashSet<A>(lgK, tgtHllType);

  if (compactFlag) {
    const uint8_t* curPos = data + HllUtil<A>::HASH_SET_INT_ARR_START;
    int coupon;
    for (int i = 0; i < couponCount; ++i, curPos += sizeof(coupon)) {
      std::memcpy(&coupon, curPos, sizeof(coupon));
      sketch->couponUpdate(coupon);
    }
  } else {
    int* oldArr = sketch->couponIntArr;
    const size_t oldArrLen = 1 << sketch->lgCouponArrInts;
    sketch->lgCouponArrInts = lgArrInts;
    typedef typename std::allocator_traits<A>::template rebind_alloc<int> intAlloc;
    sketch->couponIntArr = intAlloc().allocate(1 << lgArrInts);
    sketch->couponCount = couponCount;
    // only need to read valid coupons, unlike in stream case
    std::memcpy(sketch->couponIntArr,
                data + HllUtil<A>::HASH_SET_INT_ARR_START,
                couponCount * sizeof(int));
    intAlloc().deallocate(oldArr, oldArrLen);
  }

  return sketch;
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::newSet(std::istream& is) {
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
    throw std::invalid_argument("Calling set construtor with non-set mode data");
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

  CouponHashSet<A>* sketch = new (chsAlloc().allocate(1)) CouponHashSet<A>(lgK, tgtHllType);
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
    typedef typename std::allocator_traits<A>::template rebind_alloc<int> intAlloc;
    intAlloc().deallocate(sketch->couponIntArr, 1 << sketch->lgCouponArrInts);
    sketch->lgCouponArrInts = lgArrInts;
    sketch->couponIntArr = intAlloc().allocate(1 << lgArrInts);
    sketch->couponCount = couponCount;
    // for stream processing, read entire list so read pointer ends up set correctly
    is.read((char*)sketch->couponIntArr, (1 << sketch->lgCouponArrInts) * sizeof(int));
  } 

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return ptr.release();
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::copy() const {
  return new (chsAlloc().allocate(1)) CouponHashSet<A>(*this);
}

template<typename A>
CouponHashSet<A>* CouponHashSet<A>::copyAs(const target_hll_type tgtHllType) const {
  return new (chsAlloc().allocate(1)) CouponHashSet<A>(*this, tgtHllType);
}

template<typename A>
HllSketchImpl<A>* CouponHashSet<A>::couponUpdate(int coupon) {
  const int index = find<A>(this->couponIntArr, this->lgCouponArrInts, coupon);
  if (index >= 0) {
    return this; // found duplicate, ignore
  }
  this->couponIntArr[~index] = coupon; // found empty
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
  if ((HllUtil<A>::RESIZE_DENOM * this->couponCount) > (HllUtil<A>::RESIZE_NUMER * (1 << this->lgCouponArrInts))) {
    if (this->lgCouponArrInts == (this->lgConfigK - 3)) { // at max size
      return true; // promote to HLL
    }
    int tgtLgCoupArrSize = this->lgCouponArrInts + 1;
    growHashSet(this->lgCouponArrInts, tgtLgCoupArrSize);
  }
  return false;
}

template<typename A>
void CouponHashSet<A>::growHashSet(const int srcLgCoupArrSize, const int tgtLgCoupArrSize) {
  const int tgtLen = 1 << tgtLgCoupArrSize;
  typedef typename std::allocator_traits<A>::template rebind_alloc<int> intAlloc;
  int* tgtCouponIntArr = intAlloc().allocate(tgtLen);
  std::fill(tgtCouponIntArr, tgtCouponIntArr + tgtLen, 0);

  const int srcLen = 1 << srcLgCoupArrSize;
  for (int i = 0; i < srcLen; ++i) { // scan existing array for non-zero values
    const int fetched = this->couponIntArr[i];
    if (fetched != HllUtil<A>::EMPTY) {
      const int idx = find<A>(tgtCouponIntArr, tgtLgCoupArrSize, fetched); // search TGT array
      if (idx < 0) { // found EMPTY
        tgtCouponIntArr[~idx] = fetched; // insert
        continue;
      }
      throw std::runtime_error("Error: Found duplicate coupon");
    }
  }

  intAlloc().deallocate(this->couponIntArr, 1 << this->lgCouponArrInts);
  this->couponIntArr = tgtCouponIntArr;
  this->lgCouponArrInts = tgtLgCoupArrSize;
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
