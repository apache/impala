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

#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/Descriptors_types.h"
#include "rpc/thrift-util.h"
#include "runtime/descriptors.h"
#include "runtime/query-credentials.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

namespace {

const char* SECRET_KEY = "fs.s3a.secret.key";

CredentialEntry MakeCredential(const string& prefix, const string& secret,
    uint64_t expiry_ms = 0) {
  CredentialEntry cred;
  cred.prefix = prefix;
  cred.config = {{"fs.s3a.access.key", "test-access-key"}, {SECRET_KEY, secret}};
  cred.expiry_ms = expiry_ms;
  return cred;
}

TCredential MakeThriftCredential(const string& prefix, const string& secret) {
  TCredential cred;
  cred.__set_prefix(prefix);
  cred.__set_config({{"fs.s3a.access.key", "test-access-key"}, {SECRET_KEY, secret}});
  return cred;
}

/// Returns the descriptor of table 'db'.'name' at 'location'. It is an Iceberg table if
/// 'credentials' is not null, with 'credentials' as its vended credentials unless they
/// are empty.
TTableDescriptor MakeTableDescriptor(TableId id, const string& name,
    const string& location, const vector<TCredential>* credentials) {
  THdfsTable hdfs_table;
  hdfs_table.__set_hdfsBaseDir(location);
  hdfs_table.__set_nullPartitionKeyValue("__HIVE_DEFAULT_PARTITION__");
  hdfs_table.__set_nullColumnValue("\\N");
  TTableDescriptor tdesc;
  tdesc.__set_id(id);
  tdesc.__set_numClusteringCols(0);
  tdesc.__set_dbName("db");
  tdesc.__set_tableName(name);
  tdesc.__set_hdfsTable(hdfs_table);
  if (credentials == nullptr) {
    tdesc.__set_tableType(TTableType::HDFS_TABLE);
    return tdesc;
  }
  TIcebergPartitionSpec spec;
  spec.__set_spec_id(0);
  TIcebergTable iceberg_table;
  iceberg_table.__set_table_location(location);
  iceberg_table.__set_partition_spec({spec});
  iceberg_table.__set_default_partition_spec_id(0);
  iceberg_table.__set_format_version(2);
  if (!credentials->empty()) iceberg_table.__set_credentials(*credentials);
  tdesc.__set_tableType(TTableType::ICEBERG_TABLE);
  tdesc.__set_icebergTable(iceberg_table);
  return tdesc;
}

} // anonymous namespace

class QueryCredentialsTest : public testing::Test {
 protected:
  void SetUp() override {
    test_env_.reset(new TestEnv);
    ASSERT_OK(test_env_->Init());
    RuntimeState* runtime_state;
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state));
    query_state_ = runtime_state->query_state();
  }

  void TearDown() override {
    query_state_ = nullptr;
    test_env_.reset();
  }

  /// Returns the prefix of the credential that 'creds' resolves for 'path', or "" if no
  /// credential covers it.
  string FindPrefix(QueryCredentials* creds, const string& path) {
    CredentialEntryPtr entry = creds->FindCredential(path, query_state_);
    return entry == nullptr ? "" : entry->prefix;
  }

  /// Returns the secret of the credential that 'creds' resolves for 'path'.
  string FindSecret(QueryCredentials* creds, const string& path) {
    CredentialEntryPtr entry = creds->FindCredential(path, query_state_);
    return entry == nullptr ? "" : entry->config.at(SECRET_KEY);
  }

  /// Creates the descriptor table of 'tables' in 'pool_', like QueryState does, and
  /// registers the credentials of its tables with 'creds'.
  void RegisterDescriptorTable(const vector<TTableDescriptor>& tables,
      QueryCredentials* creds, vector<const HdfsTableDescriptor*>* hdfs_tables) {
    TDescriptorTable thrift_tbl;
    thrift_tbl.__set_tableDescriptors(tables);
    TDescriptorTableSerialized serialized;
    ThriftSerializer serializer(/* compact */ false);
    ASSERT_OK(serializer.SerializeToString(&thrift_tbl, &serialized.thrift_desc_tbl));
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_OK(DescriptorTbl::Create(&pool_, serialized, &desc_tbl));
    desc_tbl->GetHdfsTableDescs(hdfs_tables);
    for (const HdfsTableDescriptor* hdfs_table : *hdfs_tables) {
      ASSERT_OK(creds->RegisterTableCredentials(*hdfs_table));
    }
    desc_tbl->ReleaseResources();
  }

  boost::scoped_ptr<TestEnv> test_env_;
  QueryState* query_state_ = nullptr;
  ObjectPool pool_;
};

// A credential covers the paths below its prefix, and the longest covering prefix wins.
TEST_F(QueryCredentialsTest, PrefixMatching) {
  QueryCredentials creds;
  EXPECT_TRUE(creds.empty());
  EXPECT_EQ("", FindPrefix(&creds, "s3a://bucket/wh/db/t1/data/f.parq"));

  const string t1 = "s3a://bucket/wh/db/t1";
  const string t1_metadata = "s3a://bucket/wh/db/t1/metadata";
  const string t2 = "s3a://bucket/wh/db/t2/";
  ASSERT_OK(creds.RegisterVendedCredential("db", "t1", MakeCredential(t1, "s1")));
  ASSERT_OK(creds.RegisterVendedCredential(
      "db", "t1", MakeCredential(t1_metadata, "s2")));
  ASSERT_OK(creds.RegisterVendedCredential("db", "t2", MakeCredential(t2, "s3")));
  EXPECT_FALSE(creds.empty());

  EXPECT_EQ(t1, FindPrefix(&creds, "s3a://bucket/wh/db/t1/data/f.parq"));
  EXPECT_EQ(t1, FindPrefix(&creds, t1));
  EXPECT_EQ(t1, FindPrefix(&creds, "s3a://bucket/wh/db/t1/"));
  EXPECT_EQ(t1_metadata, FindPrefix(&creds, "s3a://bucket/wh/db/t1/metadata/v1.json"));
  EXPECT_EQ(t2, FindPrefix(&creds, "s3a://bucket/wh/db/t2/data/f.parq"));

  // A prefix only matches on a path component boundary.
  EXPECT_EQ(t1, FindPrefix(&creds, "s3a://bucket/wh/db/t1/metadata_old/v1.json"));
  EXPECT_EQ("", FindPrefix(&creds, "s3a://bucket/wh/db/t1_backup/data/f.parq"));
  EXPECT_EQ("", FindPrefix(&creds, "s3a://bucket/wh/db/t2_backup/data/f.parq"));
  EXPECT_EQ("", FindPrefix(&creds, "s3a://bucket/wh/db/t2"));

  // Paths that no credential covers.
  EXPECT_EQ("", FindPrefix(&creds, "s3a://bucket/wh/db/t3/data/f.parq"));
  EXPECT_EQ("", FindPrefix(&creds, "s3a://other-bucket/wh/db/t1/data/f.parq"));
  EXPECT_EQ("", FindPrefix(&creds, "hdfs://localhost:20500/wh/db/t1/data/f.parq"));
  EXPECT_EQ("", FindPrefix(&creds, "/wh/db/t1/data/f.parq"));
}

// Registering a credential again for a prefix keeps the one that expires later.
TEST_F(QueryCredentialsTest, KeepsLongerLivedCredential) {
  QueryCredentials creds;
  const string prefix = "s3a://bucket/wh/db/t";
  const string path = prefix + "/data/f.parq";

  ASSERT_OK(creds.RegisterVendedCredential("db", "t", MakeCredential(prefix, "a", 1000)));
  EXPECT_EQ("a", FindSecret(&creds, path));
  ASSERT_OK(creds.RegisterVendedCredential("db", "t", MakeCredential(prefix, "b", 2000)));
  EXPECT_EQ("b", FindSecret(&creds, path));
  ASSERT_OK(creds.RegisterVendedCredential("db", "t", MakeCredential(prefix, "c", 500)));
  EXPECT_EQ("b", FindSecret(&creds, path));
  ASSERT_OK(creds.RegisterVendedCredential("db", "t", MakeCredential(prefix, "d", 2000)));
  EXPECT_EQ("b", FindSecret(&creds, path));

  // A credential without an expiry outlives all the others.
  ASSERT_OK(creds.RegisterVendedCredential("db", "t", MakeCredential(prefix, "e", 0)));
  EXPECT_EQ("e", FindSecret(&creds, path));
  ASSERT_OK(creds.RegisterVendedCredential("db", "t", MakeCredential(prefix, "f", 3000)));
  EXPECT_EQ("e", FindSecret(&creds, path));
}

// The credentials of all the tables in the descriptor table are registered, whether an
// operator scans the table or not (e.g. the target table of an INSERT).
TEST_F(QueryCredentialsTest, RegisterTablesOfDescriptorTable) {
  const string scanned = "s3a://bucket/wh/db/scanned";
  const string target = "s3a://bucket/wh/db/target";
  const string no_creds = "s3a://bucket/wh/db/no_creds";
  const string plain = "s3a://bucket/wh/db/plain";

  TCredential scanned_cred = MakeThriftCredential(scanned, "s1");
  scanned_cred.__set_expires_at_ms(1234);
  const vector<TCredential> scanned_creds = {
      scanned_cred, MakeThriftCredential(scanned + "/metadata", "s2")};
  const vector<TCredential> target_creds = {MakeThriftCredential(target, "s3")};
  const vector<TCredential> none;

  QueryCredentials creds;
  vector<const HdfsTableDescriptor*> hdfs_tables;
  RegisterDescriptorTable(
      {MakeTableDescriptor(0, "target", target, &target_creds),
       MakeTableDescriptor(1, "scanned", scanned, &scanned_creds),
       MakeTableDescriptor(2, "no_creds", no_creds, &none),
       MakeTableDescriptor(3, "plain", plain, nullptr)},
      &creds, &hdfs_tables);
  ASSERT_EQ(4, hdfs_tables.size());
  for (const HdfsTableDescriptor* hdfs_table : hdfs_tables) {
    size_t expected = hdfs_table->name() == "scanned" ? 2 :
        hdfs_table->name() == "target" ? 1 : 0;
    EXPECT_EQ(expected, hdfs_table->StorageCredentials().size()) << hdfs_table->name();
  }

  EXPECT_FALSE(creds.empty());
  EXPECT_EQ("s1", FindSecret(&creds, scanned + "/data/f.parq"));
  EXPECT_EQ("s2", FindSecret(&creds, scanned + "/metadata/v1.json"));
  EXPECT_EQ("s3", FindSecret(&creds, target + "/data/f.parq"));
  EXPECT_EQ("", FindPrefix(&creds, no_creds + "/data/f.parq"));
  EXPECT_EQ("", FindPrefix(&creds, plain + "/data/f.parq"));

  // The expiry is carried through; a credential without one never expires.
  CredentialEntryPtr scanned_entry =
      creds.FindCredential(scanned + "/data/f.parq", query_state_);
  CredentialEntryPtr target_entry =
      creds.FindCredential(target + "/data/f.parq", query_state_);
  EXPECT_EQ(1234, scanned_entry->expiry_ms);
  EXPECT_EQ(0, target_entry->expiry_ms);
}

// A query whose tables have no vended credentials registers nothing, so the connection
// cache does not look for credentials at all.
TEST_F(QueryCredentialsTest, NoCredentialsInDescriptorTable) {
  const vector<TCredential> none;
  QueryCredentials creds;
  vector<const HdfsTableDescriptor*> hdfs_tables;
  RegisterDescriptorTable(
      {MakeTableDescriptor(0, "ice", "s3a://bucket/wh/db/ice", &none),
       MakeTableDescriptor(1, "plain", "hdfs://localhost:20500/wh/db/plain", nullptr)},
      &creds, &hdfs_tables);
  ASSERT_EQ(2, hdfs_tables.size());
  EXPECT_TRUE(creds.empty());
  EXPECT_EQ("", FindPrefix(&creds, "s3a://bucket/wh/db/ice/data/f.parq"));
}

} // namespace impala
