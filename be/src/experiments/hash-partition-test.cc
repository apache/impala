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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "util/hash-util.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;

// Test collision problem with multiple mod steps (IMPALA-219)
int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  CpuInfo::Init();

  int num_buckets1 = 16;
  int num_buckets2 = 1024;
  int num_values = num_buckets1 * num_buckets2;

  int num_collisions1 = 0;
  int num_collisions2 = 0;
  int num_empty2 = num_buckets2;
  vector<bool> buckets1;
  vector<bool> buckets2;
  buckets1.resize(num_buckets1);
  buckets2.resize(num_buckets2);

  // First test using the same hash fn both times
  for (int i = 0; i < num_values; ++i) {
    uint32_t hash1 = HashUtil::Hash(&i, sizeof(int), 0) >> 8;
    uint32_t hash2 = HashUtil::Hash(&i, sizeof(int), 1) >> 8;
    uint32_t bucket1_idx = hash1 % num_buckets1;
    if (buckets1[bucket1_idx]) ++num_collisions1;
    buckets1[bucket1_idx] = true;

    LOG(ERROR) << i << ":" << hash1 << ":" << hash2;
    // If they matched bucket 0, put it into buckets2
    if (bucket1_idx == 0) {
      uint32_t bucket2_idx = hash2 % num_buckets2;
      if (buckets2[bucket2_idx]) {
        ++num_collisions2;
      } else {
        buckets2[bucket2_idx] = true;
        --num_empty2;
      }
    }
  }

  LOG(ERROR) << "Same hash:" << endl
             << "  Bucket 1 Collisions: " << num_collisions1 << endl
             << "  Expected 1 Collisions: " << num_values - num_buckets1 << endl
             << "  Bucket 2 Collisions: " << num_collisions2 << endl
             << "  Bucket 2 Empties: " << num_empty2 << endl
             << "  Bucket 2 Total Values: " << num_values / num_buckets1;

  return 0;
}
