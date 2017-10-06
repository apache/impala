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

#include "util/bloom-filter.h"

#include "codegen/impala-ir.h"

using namespace impala;

void BloomFilter::InsertNoAvx2(const uint32_t hash) noexcept {
  always_false_ = false;
  const uint32_t bucket_idx = HashUtil::Rehash32to32(hash) & directory_mask_;
  BucketInsert(bucket_idx, hash);
}

void BloomFilter::InsertAvx2(const uint32_t hash) noexcept {
  always_false_ = false;
  const uint32_t bucket_idx = HashUtil::Rehash32to32(hash) & directory_mask_;
  BucketInsertAVX2(bucket_idx, hash);
}
