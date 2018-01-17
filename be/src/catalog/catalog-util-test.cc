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

#include "catalog/catalog-util.h"
#include "testutil/gtest-util.h"

using namespace impala;
using namespace std;

void CompressAndDecompress(const std::string& input) {
  string compressed;
  string decompressed;
  ASSERT_OK(CompressCatalogObject(reinterpret_cast<const uint8_t*>(input.data()),
      static_cast<uint32_t>(input.size()), &compressed));
  ASSERT_OK(DecompressCatalogObject(reinterpret_cast<const uint8_t*>(compressed.data()),
      static_cast<uint32_t>(compressed.size()), &decompressed));
  ASSERT_EQ(input.size(), decompressed.size());
  ASSERT_EQ(input, decompressed);
}


TEST(CatalogUtil, TestCatalogCompression) {
  CompressAndDecompress("");
  CompressAndDecompress("deadbeef");
  string large_string;
  uint32_t large_string_size = 0x7E000000; // LZ4_MAX_INPUT_SIZE
  large_string.reserve(large_string_size);
  for (uint32_t i = 0; i < large_string_size; ++i) {
    large_string.push_back(static_cast<char>(rand() % (1 + numeric_limits<char>::max())));
  }
  CompressAndDecompress(large_string);
}

IMPALA_TEST_MAIN();

