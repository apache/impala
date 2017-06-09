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

#include "runtime/raw-value.h"

using namespace impala;

bool IR_ALWAYS_INLINE RuntimeFilter::Eval(
    void* val, const ColumnType& col_type) const noexcept {
  // Safe to read bloom_filter_ concurrently with any ongoing SetBloomFilter() thanks
  // to a) the atomicity of / pointer assignments and b) the x86 TSO memory model.
  if (bloom_filter_ == BloomFilter::ALWAYS_TRUE_FILTER) return true;
  uint32_t h = RawValue::GetHashValue(val, col_type,
      RuntimeFilterBank::DefaultHashSeed());
  return bloom_filter_->Find(h);
}
