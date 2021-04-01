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

#ifndef _HLLARRAY_HPP_
#define _HLLARRAY_HPP_

#include "HllSketchImpl.hpp"
#include "HllUtil.hpp"

namespace datasketches {

template<typename A>
class AuxHashMap;

template<typename A>
class HllArray : public HllSketchImpl<A> {
  public:
    HllArray(int lgConfigK, target_hll_type tgtHllType, bool startFullSize, const A& allocator);

    static HllArray* newHll(const void* bytes, size_t len, const A& allocator);
    static HllArray* newHll(std::istream& is, const A& allocator);

    virtual vector_u8<A> serialize(bool compact, unsigned header_size_bytes) const;
    virtual void serialize(std::ostream& os, bool compact) const;

    virtual ~HllArray() = default;
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const = 0;

    virtual HllArray* copy() const = 0;
    virtual HllArray* copyAs(target_hll_type tgtHllType) const;

    virtual HllSketchImpl<A>* couponUpdate(int coupon) = 0;

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getLowerBound(int numStdDev) const;
    virtual double getUpperBound(int numStdDev) const;

    inline void addToHipAccum(double delta);

    inline void decNumAtCurMin();

    inline int getCurMin() const;
    inline int getNumAtCurMin() const;
    inline double getHipAccum() const;

    virtual int getHllByteArrBytes() const = 0;

    virtual int getUpdatableSerializationBytes() const;
    virtual int getCompactSerializationBytes() const;

    virtual bool isOutOfOrderFlag() const;
    virtual bool isEmpty() const;
    virtual bool isCompact() const;

    virtual void putOutOfOrderFlag(bool flag);

    inline double getKxQ0() const;
    inline double getKxQ1() const;

    virtual int getMemDataStart() const;
    virtual int getPreInts() const;

    void putCurMin(int curMin);
    void putHipAccum(double hipAccum);
    inline void putKxQ0(double kxq0);
    inline void putKxQ1(double kxq1);
    void putNumAtCurMin(int numAtCurMin);

    static int hllArrBytes(target_hll_type tgtHllType, int lgConfigK);
    static int hll4ArrBytes(int lgConfigK);
    static int hll6ArrBytes(int lgConfigK);
    static int hll8ArrBytes(int lgConfigK);

    virtual AuxHashMap<A>* getAuxHashMap() const;

    class const_iterator;
    virtual const_iterator begin(bool all = false) const;
    virtual const_iterator end() const;

    virtual A getAllocator() const;

  protected:
    void hipAndKxQIncrementalUpdate(uint8_t oldValue, uint8_t newValue);
    double getHllBitMapEstimate(int lgConfigK, int curMin, int numAtCurMin) const;
    double getHllRawEstimate(int lgConfigK, double kxqSum) const;

    double hipAccum;
    double kxq0;
    double kxq1;
    vector_u8<A> hllByteArr; //init by sub-classes
    int curMin; //always zero for Hll6 and Hll8, only tracked by Hll4Array
    int numAtCurMin; //interpreted as num zeros when curMin == 0
    bool oooFlag; //Out-Of-Order Flag

    friend class HllSketchImplFactory<A>;
};

template<typename A>
class HllArray<A>::const_iterator: public std::iterator<std::input_iterator_tag, uint32_t> {
public:
  const_iterator(const uint8_t* array, size_t array_slze, size_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset, bool all);
  const_iterator& operator++();
  bool operator!=(const const_iterator& other) const;
  uint32_t operator*() const;
private:
  const uint8_t* array;
  size_t array_size;
  size_t index;
  target_hll_type hll_type;
  const AuxHashMap<A>* exceptions;
  uint8_t offset;
  bool all;
  uint8_t value; // cached value to avoid computing in operator++ and in operator*()
  static inline uint8_t get_value(const uint8_t* array, size_t index, target_hll_type hll_type, const AuxHashMap<A>* exceptions, uint8_t offset);
};

}

#endif /* _HLLARRAY_HPP_ */
