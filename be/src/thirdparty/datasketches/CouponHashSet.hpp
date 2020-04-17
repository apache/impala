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

#ifndef _COUPONHASHSET_HPP_
#define _COUPONHASHSET_HPP_

#include "CouponList.hpp"

namespace datasketches {

template<typename A = std::allocator<char>>
class CouponHashSet : public CouponList<A> {
  public:
    static CouponHashSet* newSet(const void* bytes, size_t len);
    static CouponHashSet* newSet(std::istream& is);
    explicit CouponHashSet(int lgConfigK, target_hll_type tgtHllType);
    explicit CouponHashSet(const CouponHashSet& that, target_hll_type tgtHllType);
    explicit CouponHashSet(const CouponHashSet& that);

    virtual ~CouponHashSet();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

  protected:
    
    virtual CouponHashSet* copy() const;
    virtual CouponHashSet* copyAs(target_hll_type tgtHllType) const;

    virtual HllSketchImpl<A>* couponUpdate(int coupon);

    virtual int getMemDataStart() const;
    virtual int getPreInts() const;

    friend class HllSketchImplFactory<A>;

  private:
    typedef typename std::allocator_traits<A>::template rebind_alloc<CouponHashSet<A>> chsAlloc;
    bool checkGrowOrPromote();
    void growHashSet(int srcLgCoupArrSize, int tgtLgCoupArrSize);
};

}

#endif /* _COUPONHASHSET_HPP_ */
