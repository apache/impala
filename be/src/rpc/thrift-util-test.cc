// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include "testutil/gtest-util.h"
#include "util/network-util.h"
#include "rpc/thrift-util.h"

#include "gen-cpp/RuntimeProfile_types.h"

#include "common/names.h"

namespace impala {

TEST(ThriftUtil, SimpleSerializeDeserialize) {
  // Loop over compact and binary protocols
  for (int i = 0; i < 2; ++i) {
    bool compact = (i == 0);
    ThriftSerializer serializer(compact);

    TCounter counter;
    counter.__set_name("Test");
    counter.__set_unit(TUnit::UNIT);
    counter.__set_value(123);

    vector<uint8_t> msg;
    EXPECT_OK(serializer.Serialize(&counter, &msg));

    uint8_t* buffer1 = NULL;
    uint8_t* buffer2 = NULL;
    uint32_t len1 = 0;
    uint32_t len2 = 0;

    EXPECT_OK(serializer.Serialize(&counter, &len1, &buffer1));

    EXPECT_EQ(len1, msg.size());
    EXPECT_TRUE(memcmp(buffer1, &msg[0], len1) == 0);

    // Serialize again and ensure the memory buffer is the same and being reused.
    EXPECT_OK(serializer.Serialize(&counter, &len2, &buffer2));

    EXPECT_EQ(len1, len2);
    EXPECT_TRUE(buffer1 == buffer2);

    TCounter deserialized_counter;
    EXPECT_OK(DeserializeThriftMsg(buffer1, &len1, compact, &deserialized_counter));
    EXPECT_EQ(len1, len2);
    EXPECT_TRUE(counter == deserialized_counter);
  }
}

TEST(ThriftUtil, TNetworkAddressComparator) {
  EXPECT_TRUE(TNetworkAddressComparator(MakeNetworkAddress("aaaa", 500),
                                        MakeNetworkAddress("zzzz", 500)));
  EXPECT_FALSE(TNetworkAddressComparator(MakeNetworkAddress("zzzz", 500),
                                         MakeNetworkAddress("aaaa", 500)));
  EXPECT_TRUE(TNetworkAddressComparator(MakeNetworkAddress("aaaa", 500),
                                        MakeNetworkAddress("aaaa", 501)));
  EXPECT_FALSE(TNetworkAddressComparator(MakeNetworkAddress("aaaa", 501),
                                         MakeNetworkAddress("aaaa", 500)));

  EXPECT_FALSE(TNetworkAddressComparator(MakeNetworkAddress("aaaa", 500),
                                         MakeNetworkAddress("aaaa", 500)));
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
