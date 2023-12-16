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

#include "runtime/runtime-filter.h"
#include "util/min-max-filter.h"

using namespace impala;

bool IR_ALWAYS_INLINE RuntimeFilter::Eval(
    void* val, const ColumnType& col_type) const noexcept {
  switch (filter_desc().type) {
    case TRuntimeFilterType::BLOOM: {
      if (bloom_filter_.Load() == BloomFilter::ALWAYS_TRUE_FILTER) return true;
      uint32_t h = RawValue::GetHashValueFastHash32(
          val, col_type, RuntimeFilterBank::DefaultHashSeed());
      return bloom_filter_.Load()->Find(h);
    }
    case TRuntimeFilterType::MIN_MAX: {
      // Min/max overlap does not deal with nulls (val==nullptr).
      if (LIKELY(val)) {
        MinMaxFilter* filter = get_min_max(); // get the loaded version.
        if (LIKELY(filter && !filter->AlwaysTrue())) {
          return filter->EvalOverlap(col_type, val, val);
        }
      }
      break;
    }
    case TRuntimeFilterType::IN_LIST: {
      InListFilter* filter = get_in_list_filter();
      if (LIKELY(filter && !filter->AlwaysTrue())) {
        return filter->Find(val, col_type);
      }
      break;
    }
  }
  return true;
}
