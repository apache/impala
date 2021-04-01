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

#ifndef _COUPONLIST_INTERNAL_HPP_
#define _COUPONLIST_INTERNAL_HPP_

#include "CouponList.hpp"
#include "CubicInterpolation.hpp"
#include "HllUtil.hpp"
#include "count_zeros.hpp"

#include <algorithm>
#include <cmath>

namespace datasketches {

template<typename A>
CouponList<A>::CouponList(const int lgConfigK, const target_hll_type tgtHllType, const hll_mode mode, const A& allocator):
HllSketchImpl<A>(lgConfigK, tgtHllType, mode, false),
couponCount(0),
oooFlag(false),
coupons(1 << (mode == hll_mode::LIST ? HllUtil<A>::LG_INIT_LIST_SIZE : HllUtil<A>::LG_INIT_SET_SIZE), 0, allocator)
{}

template<typename A>
CouponList<A>::CouponList(const CouponList& that, const target_hll_type tgtHllType):
HllSketchImpl<A>(that.lgConfigK, tgtHllType, that.mode, false),
couponCount(that.couponCount),
oooFlag(that.oooFlag),
coupons(that.coupons)
{}

template<typename A>
std::function<void(HllSketchImpl<A>*)> CouponList<A>::get_deleter() const {
  return [](HllSketchImpl<A>* ptr) {
    CouponList<A>* cl = static_cast<CouponList<A>*>(ptr);
    ClAlloc cla(cl->getAllocator());
    cl->~CouponList();
    cla.deallocate(cl, 1);
  };
}

template<typename A>
CouponList<A>* CouponList<A>::copy() const {
  ClAlloc cla(coupons.get_allocator());
  return new (cla.allocate(1)) CouponList<A>(*this);
}

template<typename A>
CouponList<A>* CouponList<A>::copyAs(target_hll_type tgtHllType) const {
  ClAlloc cla(coupons.get_allocator());
  return new (cla.allocate(1)) CouponList<A>(*this, tgtHllType);
}

template<typename A>
CouponList<A>* CouponList<A>::newList(const void* bytes, size_t len, const A& allocator) {
  if (len < HllUtil<A>::LIST_INT_ARR_START) {
    throw std::out_of_range("Input data length insufficient to hold CouponHashSet");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::LIST_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(data[HllUtil<A>::MODE_BYTE]);
  if (mode != LIST) {
    throw std::invalid_argument("Calling list constructor with non-list mode data");
  }

  target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(data[HllUtil<A>::MODE_BYTE]);

  const int lgK = data[HllUtil<A>::LG_K_BYTE];
  const bool compact = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);
  const bool oooFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool emptyFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::EMPTY_FLAG_MASK) ? true : false);

  const int couponCount = data[HllUtil<A>::LIST_COUNT_BYTE];
  const int couponsInArray = (compact ? couponCount : (1 << HllUtil<A>::computeLgArrInts(LIST, couponCount, lgK)));
  const size_t expectedLength = HllUtil<A>::LIST_INT_ARR_START + (couponsInArray * sizeof(int));
  if (len < expectedLength) {
    throw std::out_of_range("Byte array too short for sketch. Expected " + std::to_string(expectedLength)
                                + ", found: " + std::to_string(len));
  }

  ClAlloc cla(allocator);
  CouponList<A>* sketch = new (cla.allocate(1)) CouponList<A>(lgK, tgtHllType, mode, allocator);
  sketch->couponCount = couponCount;
  sketch->putOutOfOrderFlag(oooFlag); // should always be false for LIST

  if (!emptyFlag) {
    // only need to read valid coupons, unlike in stream case
    std::memcpy(sketch->coupons.data(), data + HllUtil<A>::LIST_INT_ARR_START, couponCount * sizeof(int));
  }
  
  return sketch;
}

template<typename A>
CouponList<A>* CouponList<A>::newList(std::istream& is, const A& allocator) {
  uint8_t listHeader[8];
  is.read((char*)listHeader, 8 * sizeof(uint8_t));

  if (listHeader[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::LIST_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(listHeader[HllUtil<A>::MODE_BYTE]);
  if (mode != LIST) {
    throw std::invalid_argument("Calling list constructor with non-list mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(listHeader[HllUtil<A>::MODE_BYTE]);

  const int lgK = (int) listHeader[HllUtil<A>::LG_K_BYTE];
  const bool compact = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);
  const bool oooFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool emptyFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::EMPTY_FLAG_MASK) ? true : false);

  ClAlloc cla(allocator);
  CouponList<A>* sketch = new (cla.allocate(1)) CouponList<A>(lgK, tgtHllType, mode, allocator);
  using coupon_list_ptr = std::unique_ptr<CouponList<A>, std::function<void(HllSketchImpl<A>*)>>;
  coupon_list_ptr ptr(sketch, sketch->get_deleter());
  const int couponCount = listHeader[HllUtil<A>::LIST_COUNT_BYTE];
  sketch->couponCount = couponCount;
  sketch->putOutOfOrderFlag(oooFlag); // should always be false for LIST

  if (!emptyFlag) {
    // For stream processing, need to read entire number written to stream so read
    // pointer ends up set correctly.
    // If not compact, still need to read empty items even though in order.
    const int numToRead = (compact ? couponCount : sketch->coupons.size());
    is.read((char*)sketch->coupons.data(), numToRead * sizeof(int));
  }

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return ptr.release();
}

template<typename A>
vector_u8<A> CouponList<A>::serialize(bool compact, unsigned header_size_bytes) const {
  const size_t sketchSizeBytes = (compact ? getCompactSerializationBytes() : getUpdatableSerializationBytes()) + header_size_bytes;
  vector_u8<A> byteArr(sketchSizeBytes, 0, getAllocator());
  uint8_t* bytes = byteArr.data() + header_size_bytes;

  bytes[HllUtil<A>::PREAMBLE_INTS_BYTE] = static_cast<uint8_t>(getPreInts());
  bytes[HllUtil<A>::SER_VER_BYTE] = static_cast<uint8_t>(HllUtil<A>::SER_VER);
  bytes[HllUtil<A>::FAMILY_BYTE] = static_cast<uint8_t>(HllUtil<A>::FAMILY_ID);
  bytes[HllUtil<A>::LG_K_BYTE] = static_cast<uint8_t>(this->lgConfigK);
  bytes[HllUtil<A>::LG_ARR_BYTE] = count_trailing_zeros_in_u32(coupons.size());
  bytes[HllUtil<A>::FLAGS_BYTE] = this->makeFlagsByte(compact);
  bytes[HllUtil<A>::LIST_COUNT_BYTE] = static_cast<uint8_t>(this->mode == LIST ? couponCount : 0);
  bytes[HllUtil<A>::MODE_BYTE] = this->makeModeByte();

  if (this->mode == SET) {
    std::memcpy(bytes + HllUtil<A>::HASH_SET_COUNT_INT, &couponCount, sizeof(couponCount));
  }

  // coupons
  // isCompact() is always false for now
  const int sw = (isCompact() ? 2 : 0) | (compact ? 1 : 0);
  switch (sw) {
    case 0: { // src updatable, dst updatable
      std::memcpy(bytes + getMemDataStart(), coupons.data(), coupons.size() * sizeof(int));
      break;
    }
    case 1: { // src updatable, dst compact
      bytes += getMemDataStart(); // reusing pointer for incremental writes
      for (uint32_t coupon: *this) {
        std::memcpy(bytes, &coupon, sizeof(coupon));
        bytes += sizeof(coupon);
      }
      break;
    }

    default:
      throw std::runtime_error("Impossible condition when serializing");
  }

  return byteArr;
}

template<typename A>
void CouponList<A>::serialize(std::ostream& os, const bool compact) const {
  // header
  const uint8_t preInts(getPreInts());
  os.write((char*)&preInts, sizeof(preInts));
  const uint8_t serialVersion(HllUtil<A>::SER_VER);
  os.write((char*)&serialVersion, sizeof(serialVersion));
  const uint8_t familyId(HllUtil<A>::FAMILY_ID);
  os.write((char*)&familyId, sizeof(familyId));
  const uint8_t lgKByte((uint8_t) this->lgConfigK);
  os.write((char*)&lgKByte, sizeof(lgKByte));
  const uint8_t lgArrIntsByte(count_trailing_zeros_in_u32(coupons.size()));
  os.write((char*)&lgArrIntsByte, sizeof(lgArrIntsByte));
  const uint8_t flagsByte(this->makeFlagsByte(compact));
  os.write((char*)&flagsByte, sizeof(flagsByte));

  if (this->mode == LIST) {
    const uint8_t listCount((uint8_t) couponCount);
    os.write((char*)&listCount, sizeof(listCount));
  } else { // mode == SET
    const uint8_t unused(0);
    os.write((char*)&unused, sizeof(unused));
  }

  const uint8_t modeByte(this->makeModeByte());
  os.write((char*)&modeByte, sizeof(modeByte));

  if (this->mode == SET) {
    // writing as int, already stored as int
    os.write((char*)&couponCount, sizeof(couponCount));
  }

  // coupons
  // isCompact() is always false for now
  const int sw = (isCompact() ? 2 : 0) | (compact ? 1 : 0);
  switch (sw) {
    case 0: { // src updatable, dst updatable
      os.write((char*)coupons.data(), coupons.size() * sizeof(int));
      break;
    }
    case 1: { // src updatable, dst compact
      for (uint32_t coupon: *this) {
        os.write((char*)&coupon, sizeof(coupon));
      }
      break;
    }

    default:
      throw std::runtime_error("Impossible condition when serializing");
  }
  
  return;
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::couponUpdate(int coupon) {
  for (size_t i = 0; i < coupons.size(); ++i) { // search for empty slot
    const int couponAtIdx = coupons[i];
    if (couponAtIdx == HllUtil<A>::EMPTY) {
      coupons[i] = coupon; // the actual update
      ++couponCount;
      if (couponCount == static_cast<int>(coupons.size())) { // array full
        if (this->lgConfigK < 8) {
          return promoteHeapListOrSetToHll(*this);
        }
        return promoteHeapListToSet(*this);
      }
      return this;
    }
    // cell not empty
    if (couponAtIdx == coupon) {
      return this; // duplicate
    }
    // cell not empty and not a duplicate, continue
  }
  throw std::runtime_error("Array invalid: no empties and no duplicates");
}

template<typename A>
double CouponList<A>::getCompositeEstimate() const { return getEstimate(); }

template<typename A>
double CouponList<A>::getEstimate() const {
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount);
  return fmax(est, couponCount);
}

template<typename A>
double CouponList<A>::getLowerBound(const int numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 + (numStdDev * HllUtil<A>::COUPON_RSE));
  return fmax(tmp, couponCount);
}

template<typename A>
double CouponList<A>::getUpperBound(const int numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const int couponCount = getCouponCount();
  const double est = CubicInterpolation<A>::usingXAndYTables(couponCount);
  const double tmp = est / (1.0 - (numStdDev * HllUtil<A>::COUPON_RSE));
  return fmax(tmp, couponCount);
}

template<typename A>
bool CouponList<A>::isEmpty() const { return getCouponCount() == 0; }

template<typename A>
int CouponList<A>::getUpdatableSerializationBytes() const {
  return getMemDataStart() + coupons.size() * sizeof(int);
}

template<typename A>
int CouponList<A>::getCouponCount() const {
  return couponCount;
}

template<typename A>
int CouponList<A>::getCompactSerializationBytes() const {
  return getMemDataStart() + (couponCount << 2);
}

template<typename A>
int CouponList<A>::getMemDataStart() const {
  return HllUtil<A>::LIST_INT_ARR_START;
}

template<typename A>
int CouponList<A>::getPreInts() const {
  return HllUtil<A>::LIST_PREINTS;
}

template<typename A>
bool CouponList<A>::isCompact() const { return false; }

template<typename A>
bool CouponList<A>::isOutOfOrderFlag() const { return oooFlag; }

template<typename A>
void CouponList<A>::putOutOfOrderFlag(bool oooFlag) {
  this->oooFlag = oooFlag;
}

template<typename A>
A CouponList<A>::getAllocator() const {
  return coupons.get_allocator();
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::promoteHeapListToSet(CouponList& list) {
  return HllSketchImplFactory<A>::promoteListToSet(list);
}

template<typename A>
HllSketchImpl<A>* CouponList<A>::promoteHeapListOrSetToHll(CouponList& src) {
  return HllSketchImplFactory<A>::promoteListOrSetToHll(src);
}

template<typename A>
coupon_iterator<A> CouponList<A>::begin(bool all) const {
  return coupon_iterator<A>(coupons.data(), coupons.size(), 0, all);
}

template<typename A>
coupon_iterator<A> CouponList<A>::end() const {
  return coupon_iterator<A>(coupons.data(), coupons.size(), coupons.size(), false);
}

}

#endif // _COUPONLIST_INTERNAL_HPP_
