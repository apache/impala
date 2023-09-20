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

#include <algorithm>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/hash-util.h"

#include "common/names.h"

using namespace impala;

// Benchmark for testing internal representation of strings.  This is prototype
// code and should eventually be merged into StringValue

// In this case there are 10x as many short strings are long strings.
// Results:
// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// String Test:          Function                Rate          Comparison
// ----------------------------------------------------------------------
//              Normal Sequential               31.54                  1X
//             Compact Sequential               30.44             0.9651X
//                  Normal Random               16.09               0.51X
//                 Compact Random               23.13             0.7331X

// This is a string representation for compact strings.  If the string is
// sufficiently short (less than STORAGE_SIZE - 1 bytes), it will be stored
// inline.  Otherwise, it will be stored as the len/ptr.  This only supports
// strings up to 2^15 in length (only 2 bytes are used to store the len) and
// relies on the fact that the upper 16 bits of addresses are unused on x64.
template <int STORAGE_SIZE>
struct CompactStringValue {
 private:
  union {
    char bytes_[STORAGE_SIZE];
    struct {
      // The upper bit is used to encode whether this is stored inline or not.
      // If the bit is set, it is inlined.
      long is_inline_       : 1;
      long inline_len_      : 7;
      char inline_data_[STORAGE_SIZE - 1];
    };
    struct {
      long dummy_           : 1;            // Lines up with is_inline_
      long indirect_len_    : 15;
      long indirect_ptr_    : 48;
      // Rest of STORAGE_SIZE is unused.  This is a minimum of 8 bytes.
      // TODO: we could try to adapt this to support longer lengths if STORAGE_SIZE is
      // greater than 8.
    };
  };

 public:
  CompactStringValue(const char* str) {
    long len = strlen(str);
    if (len < STORAGE_SIZE) {
      memcpy(inline_data_, str, len);
      inline_len_ = len;
      is_inline_ = true;
    } else {
      indirect_ptr_ = reinterpret_cast<long>(str);
      indirect_len_ = len;
      is_inline_ = false;
    }
  }

  const char* ptr() const {
    if (is_inline_) return inline_data_;
    return reinterpret_cast<const char*>(indirect_ptr_);
  }

  int len() const {
    if (is_inline_) return inline_len_;
    return indirect_len_;
  }
};

struct TestData {
  vector<StringValue> normal_strings;
  vector<CompactStringValue<8>> compact_strings;
  vector<int> random_order;
  uint32_t hash_normal;
  uint32_t hash_compact;
  vector<string> string_data;
};

void TestNormalStringsSequential(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->hash_normal = 0;
    for (int j = 0; j < data->normal_strings.size(); ++j) {
      const StringValue& str = data->normal_strings[j];
      data->hash_normal = HashUtil::CrcHash(str.Ptr(), str.Len(), data->hash_normal);
    }
  }
}

void TestCompactStringsSequential(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->hash_compact = 0;
    for (int j = 0; j < data->compact_strings.size(); ++j) {
      const CompactStringValue<8>& str = data->compact_strings[j];
      data->hash_compact = HashUtil::CrcHash(str.ptr(), str.len(), data->hash_compact);
    }
  }
}

void TestNormalStringsRandom(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->hash_normal = 0;
    for (int j = 0; j < data->normal_strings.size(); ++j) {
      const StringValue& str = data->normal_strings[data->random_order[j]];
      data->hash_normal = HashUtil::CrcHash(str.Ptr(), str.Len(), data->hash_normal);
    }
  }
}

void TestCompactStringsRandom(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->hash_compact = 0;
    for (int j = 0; j < data->compact_strings.size(); ++j) {
      const CompactStringValue<8>& str = data->compact_strings[data->random_order[j]];
      data->hash_compact = HashUtil::CrcHash(str.ptr(), str.len(), data->hash_compact);
    }
  }
}

void AddTestString(TestData* data, const char* s) {
  data->compact_strings.push_back(s);
  data->normal_strings.push_back(StringValue(const_cast<char*>(s), strlen(s)));
}

// This creates more short strings than long strings.
void InitTestData(TestData* data, int num_small_strings, int num_large_strings) {
  for (int i = 0; i < num_small_strings; ++i) {
    data->string_data.push_back("small");
  }
  for (int i = 0; i < num_large_strings; ++i) {
    // We don't need this to be too large as to minimize the crc time.  It just
    // needs to be large enough to trigger then indirect string path.
    data->string_data.push_back("large-large-large");
  }

  for (int i = 0; i < num_small_strings; ++i) {
    AddTestString(data, data->string_data[i].c_str());
  }

  for (int i = 0; i < num_large_strings; ++i) {
    AddTestString(data, data->string_data[i + num_large_strings].c_str());
  }

  data->random_order.resize(data->normal_strings.size());
  for (int i = 0; i < data->random_order.size(); ++i) {
    data->random_order[i] = i;
  }
  random_shuffle(data->random_order.begin(), data->random_order.end());
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData data;
  InitTestData(&data, 10000, 100);

  Benchmark suite("String Test");
  suite.AddBenchmark("Normal Sequential", TestNormalStringsSequential, &data);
  suite.AddBenchmark("Compact Sequential", TestCompactStringsSequential, &data);
  suite.AddBenchmark("Normal Random", TestNormalStringsRandom, &data);
  suite.AddBenchmark("Compact Random", TestCompactStringsRandom, &data);
  cout << suite.Measure();

  if (data.hash_normal != data.hash_compact) {
    cout << "Uh oh - this is broken." << endl;
    return 1;
  }

  return 0;
}
