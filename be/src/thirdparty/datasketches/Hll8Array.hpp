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

#ifndef _HLL8ARRAY_HPP_
#define _HLL8ARRAY_HPP_

#include "HllArray.hpp"

namespace datasketches {

template<typename A>
class Hll8Iterator;

template<typename A>
class Hll8Array final : public HllArray<A> {
  public:
    explicit Hll8Array(int lgConfigK, bool startFullSize);
    explicit Hll8Array(const Hll8Array& that);

    virtual ~Hll8Array();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual Hll8Array<A>* copy() const;

    inline uint8_t getSlot(int slotNo) const;
    inline void putSlot(int slotNo, uint8_t value);

    virtual HllSketchImpl<A>* couponUpdate(int coupon) final;
    void mergeList(const CouponList<A>& src);
    void mergeHll(const HllArray<A>& src);

    virtual int getHllByteArrBytes() const;

  private:
    inline void internalCouponUpdate(int coupon);
};

}

#endif /* _HLL8ARRAY_HPP_ */
