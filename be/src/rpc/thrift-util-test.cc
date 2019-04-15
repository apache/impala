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

#include "rpc/thrift-util.h"
#include "testutil/gtest-util.h"
#include "util/container-util.h"
#include "util/network-util.h"

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
    EXPECT_OK(serializer.SerializeToVector(&counter, &msg));

    uint8_t* buffer1 = NULL;
    uint8_t* buffer2 = NULL;
    uint32_t len1 = 0;
    uint32_t len2 = 0;

    EXPECT_OK(serializer.SerializeToBuffer(&counter, &len1, &buffer1));

    EXPECT_EQ(len1, msg.size());
    EXPECT_TRUE(memcmp(buffer1, msg.data(), len1) == 0);

    // Serialize again and ensure the memory buffer is the same and being reused.
    EXPECT_OK(serializer.SerializeToBuffer(&counter, &len2, &buffer2));

    EXPECT_EQ(len1, len2);
    EXPECT_TRUE(buffer1 == buffer2);

    TCounter deserialized_counter;
    EXPECT_OK(DeserializeThriftMsg(buffer1, &len1, compact, &deserialized_counter));
    EXPECT_EQ(len1, len2);
    EXPECT_EQ(counter, deserialized_counter);

    // Serialize to string
    std::string str;
    EXPECT_OK(serializer.SerializeToString(&counter, &str));
    EXPECT_EQ(len2, str.length());

    // Verifies that deserialization of 'str' works.
    TCounter deserialized_counter_2;
    EXPECT_OK(DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(str.data()), &len2,
        compact, &deserialized_counter_2));
    EXPECT_EQ(counter, deserialized_counter_2);
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

IMPALA_TEST_MAIN();
