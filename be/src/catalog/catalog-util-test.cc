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

#include <gutil/strings/substitute.h>

#include "catalog/catalog-util.h"
#include "testutil/gtest-util.h"

using namespace impala;
using namespace std;
using namespace strings;

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

TEST(CatalogUtil, TestTPrivilegeFromObjectName) {
  vector<tuple<string, TPrivilegeLevel::type>> actions = {
      make_tuple("all", TPrivilegeLevel::ALL),
      make_tuple("insert", TPrivilegeLevel::INSERT),
      make_tuple("select", TPrivilegeLevel::SELECT),
      make_tuple("refresh", TPrivilegeLevel::REFRESH),
      make_tuple("create", TPrivilegeLevel::CREATE),
      make_tuple("alter", TPrivilegeLevel::ALTER),
      make_tuple("drop", TPrivilegeLevel::DROP),
      make_tuple("owner", TPrivilegeLevel::OWNER)
  };
  vector<tuple<string, bool>> grant_options = {
      make_tuple("true", true),
      make_tuple("false", false)
  };

  for (const auto& action: actions) {
    for (const auto& grant_option: grant_options) {
      TPrivilege server_privilege;
      ASSERT_OK(TPrivilegeFromObjectName(Substitute(
          "server=server1->action=$0->grantoption=$1",
          get<0>(action), get<0>(grant_option)), &server_privilege));
      ASSERT_EQ(TPrivilegeScope::SERVER, server_privilege.scope);
      ASSERT_EQ(get<1>(action), server_privilege.privilege_level);
      ASSERT_EQ(get<1>(grant_option), server_privilege.has_grant_opt);
      ASSERT_EQ("server1", server_privilege.server_name);

      TPrivilege uri_privilege;
      ASSERT_OK(TPrivilegeFromObjectName(Substitute(
          "server=server1->uri=/test-warehouse->action=$0->grantoption=$1",
          get<0>(action), get<0>(grant_option)), &uri_privilege));
      ASSERT_EQ(TPrivilegeScope::URI, uri_privilege.scope);
      ASSERT_EQ(get<1>(action), uri_privilege.privilege_level);
      ASSERT_EQ(get<1>(grant_option), uri_privilege.has_grant_opt);
      ASSERT_EQ("server1", uri_privilege.server_name);
      ASSERT_EQ("/test-warehouse", uri_privilege.uri);

      TPrivilege db_privilege;
      ASSERT_OK(TPrivilegeFromObjectName(Substitute(
          "server=server1->db=functional->action=$0->grantoption=$1",
          get<0>(action), get<0>(grant_option)), &db_privilege));
      ASSERT_EQ(TPrivilegeScope::DATABASE, db_privilege.scope);
      ASSERT_EQ(get<1>(action), db_privilege.privilege_level);
      ASSERT_EQ(get<1>(grant_option), db_privilege.has_grant_opt);
      ASSERT_EQ("server1", db_privilege.server_name);
      ASSERT_EQ("functional", db_privilege.db_name);

      TPrivilege table_privilege;
      ASSERT_OK(TPrivilegeFromObjectName(Substitute(
          "server=server1->db=functional->table=alltypes->action=$0->grantoption=$1",
          get<0>(action), get<0>(grant_option)), &table_privilege));
      ASSERT_EQ(TPrivilegeScope::TABLE, table_privilege.scope);
      ASSERT_EQ(get<1>(action), table_privilege.privilege_level);
      ASSERT_EQ(get<1>(grant_option), table_privilege.has_grant_opt);
      ASSERT_EQ("server1", table_privilege.server_name);
      ASSERT_EQ("functional", table_privilege.db_name);
      ASSERT_EQ("alltypes", table_privilege.table_name);

      TPrivilege column_privilege;
      ASSERT_OK(TPrivilegeFromObjectName(Substitute(
          "server=server1->db=functional->table=alltypes->column=id->action=$0->"
          "grantoption=$1", get<0>(action), get<0>(grant_option)), &column_privilege));
      ASSERT_EQ(TPrivilegeScope::COLUMN, column_privilege.scope);
      ASSERT_EQ(get<1>(action), column_privilege.privilege_level);
      ASSERT_EQ(get<1>(grant_option), column_privilege.has_grant_opt);
      ASSERT_EQ("server1", column_privilege.server_name);
      ASSERT_EQ("functional", column_privilege.db_name);
      ASSERT_EQ("alltypes", column_privilege.table_name);
      ASSERT_EQ("id", column_privilege.column_name);
    }
  }

  TPrivilege privilege;
  EXPECT_ERROR(TPrivilegeFromObjectName("abc=server1->action=select->grantoption=true",
      &privilege), TErrorCode::GENERAL);
  EXPECT_ERROR(TPrivilegeFromObjectName("server=server1->action=foo->grantoption=true",
      &privilege), TErrorCode::GENERAL);
  EXPECT_ERROR(TPrivilegeFromObjectName("server=server1->action=select->grantoption=foo",
      &privilege), TErrorCode::GENERAL);
  EXPECT_ERROR(TPrivilegeFromObjectName("", &privilege), TErrorCode::GENERAL);
  EXPECT_ERROR(TPrivilegeFromObjectName("SERVER=server1->action=select->grantoption=true",
      &privilege), TErrorCode::GENERAL);
  EXPECT_ERROR(TPrivilegeFromObjectName("server;server1->action=select->grantoption=true",
      &privilege), TErrorCode::GENERAL);
}

