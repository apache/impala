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

#ifndef _HLLARRAY_INTERNAL_HPP_
#define _HLLARRAY_INTERNAL_HPP_

#include "HllArray.hpp"
#include "HllUtil.hpp"
#include "HarmonicNumbers.hpp"
#include "CubicInterpolation.hpp"
#include "CompositeInterpolationXTable.hpp"
#include "CouponList.hpp"
#include "inv_pow2_table.hpp"
#include <cstring>
#include <cmath>
#include <stdexcept>
#include <string>

namespace datasketches {

template<typename A>
HllArray<A>::HllArray(const int lgConfigK, const target_hll_type tgtHllType, bool startFullSize, const A& allocator):
HllSketchImpl<A>(lgConfigK, tgtHllType, hll_mode::HLL, startFullSize),
hipAccum(0.0),
kxq0(1 << lgConfigK),
kxq1(0.0),
hllByteArr(allocator),
curMin(0),
numAtCurMin(1 << lgConfigK),
oooFlag(false)
{}

template<typename A>
HllArray<A>* HllArray<A>::copyAs(const target_hll_type tgtHllType) const {
  if (tgtHllType == this->getTgtHllType()) {
    return static_cast<HllArray*>(copy());
  }
  if (tgtHllType == target_hll_type::HLL_4) {
    return HllSketchImplFactory<A>::convertToHll4(*this);
  } else if (tgtHllType == target_hll_type::HLL_6) {
    return HllSketchImplFactory<A>::convertToHll6(*this);
  } else { // tgtHllType == HLL_8
    return HllSketchImplFactory<A>::convertToHll8(*this);
  }
}

template<typename A>
HllArray<A>* HllArray<A>::newHll(const void* bytes, size_t len, const A& allocator) {
  if (len < HllUtil<A>::HLL_BYTE_ARR_START) {
    throw std::out_of_range("Input data length insufficient to hold HLL array");
  }

  const uint8_t* data = static_cast<const uint8_t*>(bytes);
  if (data[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::HLL_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (data[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (data[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input array is not an HLL sketch");
  }

  const hll_mode mode = HllSketchImpl<A>::extractCurMode(data[HllUtil<A>::MODE_BYTE]);
  if (mode != HLL) {
    throw std::invalid_argument("Calling HLL array construtor with non-HLL mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(data[HllUtil<A>::MODE_BYTE]);
  const bool oooFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool comapctFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);
  const bool startFullSizeFlag = ((data[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::FULL_SIZE_FLAG_MASK) ? true : false);

  const int lgK = (int) data[HllUtil<A>::LG_K_BYTE];
  const int curMin = (int) data[HllUtil<A>::HLL_CUR_MIN_BYTE];

  const int arrayBytes = hllArrBytes(tgtHllType, lgK);
  if (len < static_cast<size_t>(HllUtil<A>::HLL_BYTE_ARR_START + arrayBytes)) {
    throw std::out_of_range("Input array too small to hold sketch image");
  }

  double hip, kxq0, kxq1;
  std::memcpy(&hip, data + HllUtil<A>::HIP_ACCUM_DOUBLE, sizeof(double));
  std::memcpy(&kxq0, data + HllUtil<A>::KXQ0_DOUBLE, sizeof(double));
  std::memcpy(&kxq1, data + HllUtil<A>::KXQ1_DOUBLE, sizeof(double));

  int numAtCurMin, auxCount;
  std::memcpy(&numAtCurMin, data + HllUtil<A>::CUR_MIN_COUNT_INT, sizeof(int));
  std::memcpy(&auxCount, data + HllUtil<A>::AUX_COUNT_INT, sizeof(int));

  AuxHashMap<A>* auxHashMap = nullptr;
  typedef std::unique_ptr<AuxHashMap<A>, std::function<void(AuxHashMap<A>*)>> aux_hash_map_ptr;
  aux_hash_map_ptr aux_ptr;
  if (auxCount > 0) { // necessarily TgtHllType == HLL_4
    int auxLgIntArrSize = (int) data[4];
    const size_t offset = HllUtil<A>::HLL_BYTE_ARR_START + arrayBytes;
    const uint8_t* auxDataStart = data + offset;
    auxHashMap = AuxHashMap<A>::deserialize(auxDataStart, len - offset, lgK, auxCount, auxLgIntArrSize, comapctFlag, allocator);
    aux_ptr = aux_hash_map_ptr(auxHashMap, auxHashMap->make_deleter());
  }

  HllArray<A>* sketch = HllSketchImplFactory<A>::newHll(lgK, tgtHllType, startFullSizeFlag, allocator);
  sketch->putCurMin(curMin);
  sketch->putOutOfOrderFlag(oooFlag);
  if (!oooFlag) sketch->putHipAccum(hip);
  sketch->putKxQ0(kxq0);
  sketch->putKxQ1(kxq1);
  sketch->putNumAtCurMin(numAtCurMin);

  std::memcpy(sketch->hllByteArr.data(), data + HllUtil<A>::HLL_BYTE_ARR_START, arrayBytes);

  if (auxHashMap != nullptr)
    ((Hll4Array<A>*)sketch)->putAuxHashMap(auxHashMap);

  aux_ptr.release();
  return sketch;
}

template<typename A>
HllArray<A>* HllArray<A>::newHll(std::istream& is, const A& allocator) {
  uint8_t listHeader[8];
  is.read((char*)listHeader, 8 * sizeof(uint8_t));

  if (listHeader[HllUtil<A>::PREAMBLE_INTS_BYTE] != HllUtil<A>::HLL_PREINTS) {
    throw std::invalid_argument("Incorrect number of preInts in input stream");
  }
  if (listHeader[HllUtil<A>::SER_VER_BYTE] != HllUtil<A>::SER_VER) {
    throw std::invalid_argument("Wrong ser ver in input stream");
  }
  if (listHeader[HllUtil<A>::FAMILY_BYTE] != HllUtil<A>::FAMILY_ID) {
    throw std::invalid_argument("Input stream is not an HLL sketch");
  }

  hll_mode mode = HllSketchImpl<A>::extractCurMode(listHeader[HllUtil<A>::MODE_BYTE]);
  if (mode != HLL) {
    throw std::invalid_argument("Calling HLL construtor with non-HLL mode data");
  }

  const target_hll_type tgtHllType = HllSketchImpl<A>::extractTgtHllType(listHeader[HllUtil<A>::MODE_BYTE]);
  const bool oooFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::OUT_OF_ORDER_FLAG_MASK) ? true : false);
  const bool comapctFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::COMPACT_FLAG_MASK) ? true : false);
  const bool startFullSizeFlag = ((listHeader[HllUtil<A>::FLAGS_BYTE] & HllUtil<A>::FULL_SIZE_FLAG_MASK) ? true : false);

  const int lgK = (int) listHeader[HllUtil<A>::LG_K_BYTE];
  const int curMin = (int) listHeader[HllUtil<A>::HLL_CUR_MIN_BYTE];

  HllArray* sketch = HllSketchImplFactory<A>::newHll(lgK, tgtHllType, startFullSizeFlag, allocator);
  typedef std::unique_ptr<HllArray<A>, std::function<void(HllSketchImpl<A>*)>> hll_array_ptr;
  hll_array_ptr sketch_ptr(sketch, sketch->get_deleter());
  sketch->putCurMin(curMin);
  sketch->putOutOfOrderFlag(oooFlag);

  double hip, kxq0, kxq1;
  is.read((char*)&hip, sizeof(hip));
  is.read((char*)&kxq0, sizeof(kxq0));
  is.read((char*)&kxq1, sizeof(kxq1));
  if (!oooFlag) sketch->putHipAccum(hip);
  sketch->putKxQ0(kxq0);
  sketch->putKxQ1(kxq1);

  int numAtCurMin, auxCount;
  is.read((char*)&numAtCurMin, sizeof(numAtCurMin));
  is.read((char*)&auxCount, sizeof(auxCount));
  sketch->putNumAtCurMin(numAtCurMin);
  
  is.read((char*)sketch->hllByteArr.data(), sketch->getHllByteArrBytes());
  
  if (auxCount > 0) { // necessarily TgtHllType == HLL_4
    int auxLgIntArrSize = listHeader[4];
    AuxHashMap<A>* auxHashMap = AuxHashMap<A>::deserialize(is, lgK, auxCount, auxLgIntArrSize, comapctFlag, allocator);
    ((Hll4Array<A>*)sketch)->putAuxHashMap(auxHashMap);
  }

  if (!is.good())
    throw std::runtime_error("error reading from std::istream"); 

  return sketch_ptr.release();
}

template<typename A>
vector_u8<A> HllArray<A>::serialize(bool compact, unsigned header_size_bytes) const {
  const size_t sketchSizeBytes = (compact ? getCompactSerializationBytes() : getUpdatableSerializationBytes()) + header_size_bytes;
  vector_u8<A> byteArr(sketchSizeBytes, 0, getAllocator());
  uint8_t* bytes = byteArr.data() + header_size_bytes;
  AuxHashMap<A>* auxHashMap = getAuxHashMap();

  bytes[HllUtil<A>::PREAMBLE_INTS_BYTE] = static_cast<uint8_t>(getPreInts());
  bytes[HllUtil<A>::SER_VER_BYTE] = static_cast<uint8_t>(HllUtil<A>::SER_VER);
  bytes[HllUtil<A>::FAMILY_BYTE] = static_cast<uint8_t>(HllUtil<A>::FAMILY_ID);
  bytes[HllUtil<A>::LG_K_BYTE] = static_cast<uint8_t>(this->lgConfigK);
  bytes[HllUtil<A>::LG_ARR_BYTE] = static_cast<uint8_t>(auxHashMap == nullptr ? 0 : auxHashMap->getLgAuxArrInts());
  bytes[HllUtil<A>::FLAGS_BYTE] = this->makeFlagsByte(compact);
  bytes[HllUtil<A>::HLL_CUR_MIN_BYTE] = static_cast<uint8_t>(curMin);
  bytes[HllUtil<A>::MODE_BYTE] = this->makeModeByte();

  std::memcpy(bytes + HllUtil<A>::HIP_ACCUM_DOUBLE, &hipAccum, sizeof(double));
  std::memcpy(bytes + HllUtil<A>::KXQ0_DOUBLE, &kxq0, sizeof(double));
  std::memcpy(bytes + HllUtil<A>::KXQ1_DOUBLE, &kxq1, sizeof(double));
  std::memcpy(bytes + HllUtil<A>::CUR_MIN_COUNT_INT, &numAtCurMin, sizeof(int));
  const int auxCount = (auxHashMap == nullptr ? 0 : auxHashMap->getAuxCount());
  std::memcpy(bytes + HllUtil<A>::AUX_COUNT_INT, &auxCount, sizeof(int));

  const int hllByteArrBytes = getHllByteArrBytes();
  std::memcpy(bytes + getMemDataStart(), hllByteArr.data(), hllByteArrBytes);

  // aux map if HLL_4
  if (this->tgtHllType == HLL_4) {
    bytes += getMemDataStart() + hllByteArrBytes; // start of auxHashMap
    if (auxHashMap != nullptr) {
      if (compact) {
        for (uint32_t coupon: *auxHashMap) {
          std::memcpy(bytes, &coupon, sizeof(coupon));
          bytes += sizeof(coupon);
        }
      } else {
        std::memcpy(bytes, auxHashMap->getAuxIntArr(), auxHashMap->getUpdatableSizeBytes());
      }
    } else if (!compact) {
      // if updatable, we write even if currently unused so the binary can be wrapped
      int auxBytes = 4 << HllUtil<A>::LG_AUX_ARR_INTS[this->lgConfigK];
      std::fill_n(bytes, auxBytes, 0);
    }
  }

  return byteArr;
}

template<typename A>
void HllArray<A>::serialize(std::ostream& os, const bool compact) const {
  // header
  const uint8_t preInts(getPreInts());
  os.write((char*)&preInts, sizeof(preInts));
  const uint8_t serialVersion(HllUtil<A>::SER_VER);
  os.write((char*)&serialVersion, sizeof(serialVersion));
  const uint8_t familyId(HllUtil<A>::FAMILY_ID);
  os.write((char*)&familyId, sizeof(familyId));
  const uint8_t lgKByte((uint8_t) this->lgConfigK);
  os.write((char*)&lgKByte, sizeof(lgKByte));

  AuxHashMap<A>* auxHashMap = getAuxHashMap();
  uint8_t lgArrByte(0);
  if (auxHashMap != nullptr) {
    lgArrByte = auxHashMap->getLgAuxArrInts();
  }
  os.write((char*)&lgArrByte, sizeof(lgArrByte));

  const uint8_t flagsByte(this->makeFlagsByte(compact));
  os.write((char*)&flagsByte, sizeof(flagsByte));
  const uint8_t curMinByte((uint8_t) curMin);
  os.write((char*)&curMinByte, sizeof(curMinByte));
  const uint8_t modeByte(this->makeModeByte());
  os.write((char*)&modeByte, sizeof(modeByte));

  // estimator data
  os.write((char*)&hipAccum, sizeof(hipAccum));
  os.write((char*)&kxq0, sizeof(kxq0));
  os.write((char*)&kxq1, sizeof(kxq1));

  // array data
  os.write((char*)&numAtCurMin, sizeof(numAtCurMin));

  const int auxCount = (auxHashMap == nullptr ? 0 : auxHashMap->getAuxCount());
  os.write((char*)&auxCount, sizeof(auxCount));
  os.write((char*)hllByteArr.data(), getHllByteArrBytes());

  // aux map if HLL_4
  if (this->tgtHllType == HLL_4) {
    if (auxHashMap != nullptr) {
      if (compact) {
        for (uint32_t coupon: *auxHashMap) {
          os.write((char*)&coupon, sizeof(coupon));
        }
      } else {
        os.write((char*)auxHashMap->getAuxIntArr(), auxHashMap->getUpdatableSizeBytes());
      }
    } else if (!compact) {
      // if updatable, we write even if currently unused so the binary can be wrapped      
      int auxBytes = 4 << HllUtil<A>::LG_AUX_ARR_INTS[this->lgConfigK];
      std::fill_n(std::ostreambuf_iterator<char>(os), auxBytes, 0);
    }
  }
}

template<typename A>
double HllArray<A>::getEstimate() const {
  if (oooFlag) {
    return getCompositeEstimate();
  }
  return getHipAccum();
}

// HLL UPPER AND LOWER BOUNDS

/*
 * The upper and lower bounds are not symmetric and thus are treated slightly differently.
 * For the lower bound, when the unique count is <= k, LB >= numNonZeros, where
 * numNonZeros = k - numAtCurMin AND curMin == 0.
 *
 * For HLL6 and HLL8, curMin is always 0 and numAtCurMin is initialized to k and is decremented
 * down for each valid update until it reaches 0, where it stays. Thus, for these two
 * isomorphs, when numAtCurMin = 0, means the true curMin is > 0 and the unique count must be
 * greater than k.
 *
 * HLL4 always maintains both curMin and numAtCurMin dynamically. Nonetheless, the rules for
 * the very small values <= k where curMin = 0 still apply.
 */
template<typename A>
double HllArray<A>::getLowerBound(const int numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const int configK = 1 << this->lgConfigK;
  const double numNonZeros = ((curMin == 0) ? (configK - numAtCurMin) : configK);

  double estimate;
  double rseFactor;
  if (oooFlag) {
    estimate = getCompositeEstimate();
    rseFactor = HllUtil<A>::HLL_NON_HIP_RSE_FACTOR;
  } else {
    estimate = hipAccum;
    rseFactor = HllUtil<A>::HLL_HIP_RSE_FACTOR;
  }

  double relErr;
  if (this->lgConfigK > 12) {
    relErr = (numStdDev * rseFactor) / sqrt(configK);
  } else {
    relErr = HllUtil<A>::getRelErr(false, oooFlag, this->lgConfigK, numStdDev);
  }
  return fmax(estimate / (1.0 + relErr), numNonZeros);
}

template<typename A>
double HllArray<A>::getUpperBound(const int numStdDev) const {
  HllUtil<A>::checkNumStdDev(numStdDev);
  const int configK = 1 << this->lgConfigK;

  double estimate;
  double rseFactor;
  if (oooFlag) {
    estimate = getCompositeEstimate();
    rseFactor = HllUtil<A>::HLL_NON_HIP_RSE_FACTOR;
  } else {
    estimate = hipAccum;
    rseFactor = HllUtil<A>::HLL_HIP_RSE_FACTOR;
  }

  double relErr;
  if (this->lgConfigK > 12) {
    relErr = (-1.0) * (numStdDev * rseFactor) / sqrt(configK);
  } else {
    relErr = HllUtil<A>::getRelErr(true, oooFlag, this->lgConfigK, numStdDev);
  }
  return estimate / (1.0 + relErr);
}

/**
 * This is the (non-HIP) estimator.
 * It is called "composite" because multiple estimators are pasted together.
 * @param absHllArr an instance of the AbstractHllArray class.
 * @return the composite estimate
 */
// Original C: again-two-registers.c hhb_get_composite_estimate L1489
template<typename A>
double HllArray<A>::getCompositeEstimate() const {
  const double rawEst = getHllRawEstimate(this->lgConfigK, kxq0 + kxq1);

  const double* xArr = CompositeInterpolationXTable<A>::get_x_arr(this->lgConfigK);
  const int xArrLen = CompositeInterpolationXTable<A>::get_x_arr_length();
  const double yStride = CompositeInterpolationXTable<A>::get_y_stride(this->lgConfigK);

  if (rawEst < xArr[0]) {
    return 0;
  }

  const int xArrLenM1 = xArrLen - 1;

  if (rawEst > xArr[xArrLenM1]) {
    double finalY = yStride * xArrLenM1;
    double factor = finalY / xArr[xArrLenM1];
    return rawEst * factor;
  }

  double adjEst = CubicInterpolation<A>::usingXArrAndYStride(xArr, xArrLen, yStride, rawEst);

  // We need to completely avoid the linear_counting estimator if it might have a crazy value.
  // Empirical evidence suggests that the threshold 3*k will keep us safe if 2^4 <= k <= 2^21.

  if (adjEst > (3 << this->lgConfigK)) { return adjEst; }

  const double linEst =
      getHllBitMapEstimate(this->lgConfigK, curMin, numAtCurMin);

  // Bias is created when the value of an estimator is compared with a threshold to decide whether
  // to use that estimator or a different one.
  // We conjecture that less bias is created when the average of the two estimators
  // is compared with the threshold. Empirical measurements support this conjecture.

  const double avgEst = (adjEst + linEst) / 2.0;

  // The following constants comes from empirical measurements of the crossover point
  // between the average error of the linear estimator and the adjusted hll estimator
  double crossOver = 0.64;
  if (this->lgConfigK == 4)      { crossOver = 0.718; }
  else if (this->lgConfigK == 5) { crossOver = 0.672; }

  return (avgEst > (crossOver * (1 << this->lgConfigK))) ? adjEst : linEst;
}

template<typename A>
double HllArray<A>::getKxQ0() const {
  return kxq0;
}

template<typename A>
double HllArray<A>::getKxQ1() const {
  return kxq1;
}

template<typename A>
double HllArray<A>::getHipAccum() const {
  return hipAccum;
}

template<typename A>
int HllArray<A>::getCurMin() const {
  return curMin;
}

template<typename A>
int HllArray<A>::getNumAtCurMin() const {
  return numAtCurMin;
}

template<typename A>
void HllArray<A>::putKxQ0(const double kxq0) {
  this->kxq0 = kxq0;
}

template<typename A>
void HllArray<A>::putKxQ1(const double kxq1) {
  this->kxq1 = kxq1;
}

template<typename A>
void HllArray<A>::putHipAccum(const double hipAccum) {
  this->hipAccum = hipAccum;
}

template<typename A>
void HllArray<A>::putCurMin(const int curMin) {
  this->curMin = curMin;
}

template<typename A>
void HllArray<A>::putNumAtCurMin(const int numAtCurMin) {
  this->numAtCurMin = numAtCurMin;
}

template<typename A>
void HllArray<A>::decNumAtCurMin() {
  --numAtCurMin;
}

template<typename A>
void HllArray<A>::addToHipAccum(const double delta) {
  hipAccum += delta;
}

template<typename A>
bool HllArray<A>::isCompact() const {
  return false;
}

template<typename A>
bool HllArray<A>::isEmpty() const {
  const int configK = 1 << this->lgConfigK;
  return (getCurMin() == 0) && (getNumAtCurMin() == configK);
}

template<typename A>
void HllArray<A>::putOutOfOrderFlag(bool flag) {
  oooFlag = flag;
}

template<typename A>
bool HllArray<A>::isOutOfOrderFlag() const {
  return oooFlag;
}

template<typename A>
int HllArray<A>::hllArrBytes(target_hll_type tgtHllType, int lgConfigK) {
  switch (tgtHllType) {
  case HLL_4:
    return hll4ArrBytes(lgConfigK);
  case HLL_6:
    return hll6ArrBytes(lgConfigK);
  case HLL_8:
    return hll8ArrBytes(lgConfigK);
  default:
    throw std::invalid_argument("Invalid target HLL type"); 
  }
}

template<typename A>
int HllArray<A>::hll4ArrBytes(const int lgConfigK) {
  return 1 << (lgConfigK - 1);
}

template<typename A>
int HllArray<A>::hll6ArrBytes(const int lgConfigK) {
  const int numSlots = 1 << lgConfigK;
  return ((numSlots * 3) >> 2) + 1;
}

template<typename A>
int HllArray<A>::hll8ArrBytes(const int lgConfigK) {
  return 1 << lgConfigK;
}

template<typename A>
int HllArray<A>::getMemDataStart() const {
  return HllUtil<A>::HLL_BYTE_ARR_START;
}

template<typename A>
int HllArray<A>::getUpdatableSerializationBytes() const {
  return HllUtil<A>::HLL_BYTE_ARR_START + getHllByteArrBytes();
}

template<typename A>
int HllArray<A>::getCompactSerializationBytes() const {
  AuxHashMap<A>* auxHashMap = getAuxHashMap();
  const int auxCountBytes = ((auxHashMap == nullptr) ? 0 : auxHashMap->getCompactSizeBytes());
  return HllUtil<A>::HLL_BYTE_ARR_START + getHllByteArrBytes() + auxCountBytes;
}

template<typename A>
int HllArray<A>::getPreInts() const {
  return HllUtil<A>::HLL_PREINTS;
}

template<typename A>
AuxHashMap<A>* HllArray<A>::getAuxHashMap() const {
  return nullptr;
}

template<typename A>
void HllArray<A>::hipAndKxQIncrementalUpdate(uint8_t oldValue, uint8_t newValue) {
  const int configK = 1 << this->getLgConfigK();
  // update hip BEFORE updating kxq
  if (!oooFlag) hipAccum += configK / (kxq0 + kxq1);
  // update kxq0 and kxq1; subtract first, then add
  if (oldValue < 32) { kxq0 -= INVERSE_POWERS_OF_2[oldValue]; }
  else               { kxq1 -= INVERSE_POWERS_OF_2[oldValue]; }
  if (newValue < 32) { kxq0 += INVERSE_POWERS_OF_2[newValue]; }
  else               { kxq1 += INVERSE_POWERS_OF_2[newValue]; }
}

/**
 * Estimator when N is small, roughly less than k log(k).
 * Refer to Wikipedia: Coupon Collector Problem
 * @return the very low range estimate
 */
//In C: again-two-registers.c hhb_get_improved_linear_counting_estimate L1274
template<typename A>
double HllArray<A>::getHllBitMapEstimate(const int lgConfigK, const int curMin, const int numAtCurMin) const {
  const  int configK = 1 << lgConfigK;
  const  int numUnhitBuckets =  ((curMin == 0) ? numAtCurMin : 0);

  //This will eventually go away.
  if (numUnhitBuckets == 0) {
    return configK * log(configK / 0.5);
  }

  const int numHitBuckets = configK - numUnhitBuckets;
  return HarmonicNumbers<A>::getBitMapEstimate(configK, numHitBuckets);
}

//In C: again-two-registers.c hhb_get_raw_estimate L1167
template<typename A>
double HllArray<A>::getHllRawEstimate(const int lgConfigK, const double kxqSum) const {
  const int configK = 1 << lgConfigK;
  double correctionFactor;
  if (lgConfigK == 4) { correctionFactor = 0.673; }
  else if (lgConfigK == 5) { correctionFactor = 0.697; }
  else if (lgConfigK == 6) { correctionFactor = 0.709; }
  else { correctionFactor = 0.7213 / (1.0 + (1.079 / configK)); }
  const double hyperEst = (correctionFactor * configK * configK) / kxqSum;
  return hyperEst;
}

template<typename A>
typename HllArray<A>::const_iterator HllArray<A>::begin(bool all) const {
  return const_iterator(hllByteArr.data(), 1 << this->lgConfigK, 0, this->tgtHllType, nullptr, 0, all);
}

template<typename A>
typename HllArray<A>::const_iterator HllArray<A>::end() const {
  return const_iterator(hllByteArr.data(), 1 << this->lgConfigK, 1 << this->lgConfigK, this->tgtHllType, nullptr, 0, false);
}

template<typename A>
HllArray<A>::const_iterator::const_iterator(const uint8_t* array, size_t array_size, size_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset, bool all):
array(array), array_size(array_size), index(index), hll_type(hll_type), exceptions(exceptions), offset(offset), all(all)
{
  while (this->index < array_size) {
    value = get_value(array, this->index, hll_type, exceptions, offset);
    if (all || value != HllUtil<A>::EMPTY) break;
    this->index++;
  }
}

template<typename A>
typename HllArray<A>::const_iterator& HllArray<A>::const_iterator::operator++() {
  while (++index < array_size) {
    value = get_value(array, index, hll_type, exceptions, offset);
    if (all || value != HllUtil<A>::EMPTY) break;
  }
  return *this;
}

template<typename A>
bool HllArray<A>::const_iterator::operator!=(const const_iterator& other) const {
  return index != other.index;
}

template<typename A>
uint32_t HllArray<A>::const_iterator::operator*() const {
  return HllUtil<A>::pair(index, value);
}

template<typename A>
uint8_t HllArray<A>::const_iterator::get_value(const uint8_t* array, size_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset) {
  if (hll_type == target_hll_type::HLL_4) {
    uint8_t value = array[index >> 1];
    if ((index & 1) > 0) { // odd
        value >>= 4;
    } else {
      value &= HllUtil<A>::loNibbleMask;
    }
    if (value == HllUtil<A>::AUX_TOKEN) { // exception
      return exceptions->mustFindValueFor(index);
    }
    return value + offset;
  } else if (hll_type == target_hll_type::HLL_6) {
    const int start_bit = index * 6;
    const int shift = start_bit & 0x7;
    const int byte_idx = start_bit >> 3;
    const uint16_t two_byte_val = (array[byte_idx + 1] << 8) | array[byte_idx];
    return (two_byte_val >> shift) & HllUtil<A>::VAL_MASK_6;
  }
  // HLL_8
  return array[index];
}

template<typename A>
A HllArray<A>::getAllocator() const {
  return hllByteArr.get_allocator();
}

}

#endif // _HLLARRAY_INTERNAL_HPP_
