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

#include "gen-cpp/Types_types.h"

#include "testutil/gtest-util.h"
#include "util/network-util.h"

namespace impala {

// NetAddrComp Tests: These tests assert the TNetworkAddressComparator sorts two
// TNetworkAddress objects correctly based on their host, port, and uds address fields.

// Assert where host fields are different.
TEST(NetworkUtil, NetAddrCompHostnameDiff) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("aaaa");
  first.__set_uds_address("uds");
  first.__set_port(0);

  second.__set_hostname("bbbb");
  second.__set_uds_address("uds");
  second.__set_port(0);

  ASSERT_TRUE(fixture(first, second));
  ASSERT_FALSE(fixture(second, first));
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(first), FromTNetworkAddress(second))
      < 0);
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(second), FromTNetworkAddress(first))
      > 0);
}

// Assert where host fields are equal but port is different.
TEST(NetworkUtil, NetAddrCompPortDiff) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_port(0);
  first.__set_uds_address("");

  second.__set_hostname("host");
  second.__set_port(1);
  second.__set_uds_address("");

  ASSERT_TRUE(fixture(first, second));
  ASSERT_FALSE(fixture(second, first));
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(first), FromTNetworkAddress(second))
      < 0);
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(second), FromTNetworkAddress(first))
      > 0);
}

// Assert where host and port fields are equal but uds address is different.
TEST(NetworkUtil, NetAddrCompUDSAddrDiff) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_port(0);
  first.__set_uds_address("aaaa");

  second.__set_hostname("host");
  second.__set_port(0);
  second.__set_uds_address("bbbb");

  ASSERT_TRUE(fixture(first, second));
  ASSERT_FALSE(fixture(second, first));
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(first), FromTNetworkAddress(second))
      < 0);
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(second), FromTNetworkAddress(first))
      > 0);
}

// Assert where all three comparison fields are equal.
TEST(NetworkUtil, NetAddrUDSAddrSame) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_port(0);
  first.__set_uds_address("uds");

  second.__set_hostname("host");
  second.__set_port(0);
  second.__set_uds_address("uds");

  ASSERT_FALSE(fixture(first, second));
  ASSERT_FALSE(fixture(second, first));
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(first), FromTNetworkAddress(second))
      == 0);
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(second), FromTNetworkAddress(first))
      == 0);
}

// Assert where host and port fields are equal first address does not have
// uds address set.
TEST(NetworkUtil, NetAddrOneMissUDSAddr) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_port(0);

  second.__set_hostname("host");
  second.__set_port(0);
  second.__set_uds_address("");

  ASSERT_TRUE(fixture(first, second));
  ASSERT_FALSE(fixture(second, first));
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(first), FromTNetworkAddress(second))
      < 0);
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(second), FromTNetworkAddress(first))
      > 0);
}

// Assert where host and port fields are equal and both address does not have
// uds address set.
TEST(NetworkUtil, NetAddrAllMissUDSAddr) {
  TNetworkAddressComparator fixture;
  TNetworkAddress first;
  TNetworkAddress second;

  first.__set_hostname("host");
  first.__set_port(0);

  second.__set_hostname("host");
  second.__set_port(0);

  ASSERT_FALSE(fixture(first, second));
  ASSERT_FALSE(fixture(second, first));
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(first), FromTNetworkAddress(second))
      == 0);
  ASSERT_TRUE(
      CompareNetworkAddressPB(FromTNetworkAddress(second), FromTNetworkAddress(first))
      == 0);
}

void CheckTranslation(TNetworkAddress thrift_address) {
  NetworkAddressPB proto_address = FromTNetworkAddress(thrift_address);
  TNetworkAddress thrift_address2 = FromNetworkAddressPB(proto_address);
  NetworkAddressPB proto_address2 = FromTNetworkAddress(thrift_address2);

  TNetworkAddressComparator fixture;
  ASSERT_FALSE(fixture(thrift_address, thrift_address2));
  ASSERT_FALSE(fixture(thrift_address2, thrift_address));
  ASSERT_TRUE(CompareNetworkAddressPB(proto_address, proto_address2) == 0);
  ASSERT_TRUE(CompareNetworkAddressPB(proto_address2, proto_address) == 0);
}

// Assert consistent translation between TNetworkAddress and NetworkAddressPB.
TEST(NetworkUtil, NetAddrTranslation) {
  TNetworkAddress addr;
  addr.__set_hostname("host");
  addr.__set_port(0);
  CheckTranslation(addr);
  addr.__set_uds_address("uds");
  CheckTranslation(addr);
}

} // namespace impala
