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

#include <fstream>
#include <memory>
#include <string>

#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/io/handle-cache.h"
#include "runtime/io/request-context.h"
#include "runtime/query-credentials.h"
#include "runtime/query-state.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "testutil/gtest-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/time.h"

#include "common/names.h"

using namespace impala::io;

namespace impala {

namespace {

/// Returns a credential vended for 'prefix'. The secret makes up its key material, so
/// two credentials with different secrets are different credentials for the prefix.
CredentialEntry MakeCredential(const string& prefix, const string& secret,
    uint64_t expiry_ms = 0) {
  CredentialEntry cred;
  cred.prefix = prefix;
  cred.config = {{"fs.s3a.access.key", "test-access-key"}, {"fs.s3a.secret.key", secret}};
  cred.expiry_ms = expiry_ms;
  return cred;
}

/// Creates a query in 'test_env' and returns its QueryState. If 'cred' is not null,
/// the query has that credential, vended for table 'db.tbl'.
QueryState* CreateQuery(TestEnv* test_env, int64_t query_id,
    const CredentialEntry* cred) {
  RuntimeState* runtime_state = nullptr;
  EXPECT_OK(test_env->CreateQueryState(query_id, nullptr, &runtime_state));
  QueryState* query_state = runtime_state->query_state();
  if (cred != nullptr) {
    EXPECT_OK(query_state->query_credentials()->RegisterVendedCredential(
        "db", "tbl", *cred));
  }
  return query_state;
}

/// Returns the connection that HdfsFsCache hands out to 'query_state' for 'path'.
hdfsFS GetConnection(const string& path, QueryState* query_state,
    HdfsFsCache::HdfsFsMap* local_cache = nullptr) {
  hdfsFS fs = nullptr;
  EXPECT_OK(HdfsFsCache::instance()->GetConnection(
      path, &fs, local_cache, nullptr, query_state));
  EXPECT_TRUE(fs != nullptr);
  return fs;
}

int64_t NumCachedFileHandles() {
  return ImpaladMetrics::IO_MGR_NUM_CACHED_FILE_HANDLES->GetValue();
}

} // anonymous namespace

void ValidateNameNode(const string& path, const string& expected_namenode,
    const string& expected_error) {
  string err;
  string namenode = HdfsFsCache::GetNameNodeFromPath(path, &err);
  if (err.empty()) {
    EXPECT_EQ(namenode, expected_namenode);
  } else {
    EXPECT_EQ(err, expected_error);
  }
}

TEST(HdfsFsCacheTest, Basic) {
  // Validate qualified paths for various FileSystems
  ValidateNameNode(string("hdfs://localhost:25000/usr/hive"),
      string("hdfs://localhost:25000/"), string(""));
  ValidateNameNode(string("hdfs://nameservice1/usr/hive"),
      string("hdfs://nameservice1/"), string(""));
  ValidateNameNode(string("s3a://hdfsbucket/usr/hive"),
      string("s3a://hdfsbucket/"), string(""));
  ValidateNameNode(string("hdfs://testserver/"), string("hdfs://testserver/"),
      string(""));

  // Validate local path
  ValidateNameNode(string("file:///usr/hive"), string("file:///"), string(""));
  ValidateNameNode(string("file:/usr/hive"), string("file:///"), string(""));

  // Validate unqualified path
  ValidateNameNode(string("/usr/hive"), string("default"), string(""));

  // Validate invalid path
  string path("://usr/hive");
  ValidateNameNode(string("://usr/hive"), string(""),
      Substitute("Path missing scheme: $0", path));
  path = string("hdfs://test_invalid_path");
  ValidateNameNode(path, string(""),
      Substitute("Path missing '/' after authority: $0", path));
}

// Without vended credentials a query gets the connection that every other user of the
// filesystem gets, and that connection is not affected by the vended ones.
TEST(HdfsFsCacheTest, VendedCredentialConnections) {
  TestEnv test_env;
  ASSERT_OK(test_env.Init());

  const string tbl = "file:///tmp/hdfs-fs-cache-test/db/tbl";
  const string tbl_file = tbl + "/data/f.parq";
  // Its name starts with the name of the table, but it is another location.
  const string other_file = "file:///tmp/hdfs-fs-cache-test/db/tbl_2/data/f.parq";

  hdfsFS default_fs = GetConnection(tbl_file, nullptr);
  EXPECT_EQ(default_fs, GetConnection(other_file, nullptr));

  QueryState* plain_query = CreateQuery(&test_env, 1, nullptr);
  EXPECT_EQ(default_fs, GetConnection(tbl_file, plain_query));

  // A vended credential gets its own connection for the paths it covers.
  const CredentialEntry cred = MakeCredential(tbl, "secret-1");
  QueryState* vended_query = CreateQuery(&test_env, 2, &cred);
  hdfsFS vended_fs = GetConnection(tbl_file, vended_query);
  EXPECT_NE(default_fs, vended_fs);
  EXPECT_EQ(vended_fs, GetConnection(tbl + "/metadata/v1.json", vended_query));
  EXPECT_EQ(default_fs, GetConnection(other_file, vended_query));

  // The queries that have the same credential share the connection.
  QueryState* same_cred_query = CreateQuery(&test_env, 3, &cred);
  EXPECT_EQ(vended_fs, GetConnection(tbl_file, same_cred_query));

  // Another credential for the same prefix (e.g. after a rotation) gets another one.
  const CredentialEntry rotated_cred = MakeCredential(tbl, "secret-2");
  QueryState* rotated_query = CreateQuery(&test_env, 4, &rotated_cred);
  hdfsFS rotated_fs = GetConnection(tbl_file, rotated_query);
  EXPECT_NE(default_fs, rotated_fs);
  EXPECT_NE(vended_fs, rotated_fs);

  // A local cache keeps the connections of a filesystem apart, too.
  HdfsFsCache::HdfsFsMap local_cache;
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(vended_fs, GetConnection(tbl_file, vended_query, &local_cache));
    EXPECT_EQ(default_fs, GetConnection(other_file, vended_query, &local_cache));
  }
  EXPECT_EQ(2, local_cache.size());
  HdfsFsCache::HdfsFsMap plain_local_cache;
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(default_fs, GetConnection(tbl_file, plain_query, &plain_local_cache));
    EXPECT_EQ(default_fs, GetConnection(other_file, plain_query, &plain_local_cache));
  }
  EXPECT_EQ(1, plain_local_cache.size());

  // The queries without credentials still get the shared connection.
  EXPECT_EQ(default_fs, GetConnection(tbl_file, plain_query));
  EXPECT_EQ(default_fs, GetConnection(tbl_file, nullptr));
}

// The file handle cache only hands out a handle on the connection it was opened on, and
// the handles of a vended connection are closed before the connection is disconnected.
TEST(HdfsFsCacheTest, VendedConnectionFileHandles) {
  TestEnv test_env;
  ASSERT_OK(test_env.Init());
  DiskIoMgr* io_mgr = test_env.exec_env()->disk_io_mgr();
  unique_ptr<RequestContext> reader = io_mgr->RegisterContext();

  const boost::filesystem::path tbl_dir = boost::filesystem::temp_directory_path()
      / boost::filesystem::unique_path("hdfs-fs-cache-test-%%%%-%%%%") / "tbl";
  boost::filesystem::create_directories(tbl_dir);
  const string tbl = "file://" + tbl_dir.string();
  string tbl_file = tbl + "/f.parq";
  {
    std::ofstream file((tbl_dir / "f.parq").string());
    file << "data";
  }
  const int64_t mtime = 1;

  // The credential is valid when the queries get their connection and expires soon
  // after, which makes the connection stale.
  const int64_t ttl_ms = 3000;
  const int64_t expiry_ms = UnixMillis() + ttl_ms;
  const CredentialEntry cred = MakeCredential(tbl, "secret-1", expiry_ms);
  QueryState* vended_query = CreateQuery(&test_env, 1, &cred);
  QueryState* other_vended_query = CreateQuery(&test_env, 2, &cred);
  hdfsFS default_fs = GetConnection(tbl_file, nullptr);
  hdfsFS vended_fs = GetConnection(tbl_file, vended_query);
  ASSERT_NE(default_fs, vended_fs);
  ASSERT_EQ(vended_fs, GetConnection(tbl_file, other_vended_query));

  const int64_t initial_num_handles = NumCachedFileHandles();
  {
    // The file was not opened yet.
    FileHandleCache::Accessor accessor;
    ASSERT_OK(io_mgr->GetCachedHdfsFileHandle(
        vended_fs, &tbl_file, mtime, reader.get(), &accessor));
    EXPECT_EQ(0, reader->cached_file_handles_hit_count());
    EXPECT_EQ(1, reader->cached_file_handles_miss_count());
  }
  // The released handle stays in the cache.
  ASSERT_EQ(initial_num_handles + 1, NumCachedFileHandles());
  {
    // Reading the file through another connection does not reuse the handle of the
    // vended one...
    FileHandleCache::Accessor accessor;
    ASSERT_OK(io_mgr->GetCachedHdfsFileHandle(
        default_fs, &tbl_file, mtime, reader.get(), &accessor));
    EXPECT_EQ(0, reader->cached_file_handles_hit_count());
    EXPECT_EQ(2, reader->cached_file_handles_miss_count());
  }
  ASSERT_EQ(initial_num_handles + 2, NumCachedFileHandles());
  {
    // ... while reading it through the same connection does, for both connections.
    FileHandleCache::Accessor vended_accessor;
    ASSERT_OK(io_mgr->GetCachedHdfsFileHandle(
        vended_fs, &tbl_file, mtime, reader.get(), &vended_accessor));
    EXPECT_EQ(1, reader->cached_file_handles_hit_count());
    FileHandleCache::Accessor default_accessor;
    ASSERT_OK(io_mgr->GetCachedHdfsFileHandle(
        default_fs, &tbl_file, mtime, reader.get(), &default_accessor));
    EXPECT_EQ(2, reader->cached_file_handles_hit_count());
    EXPECT_EQ(2, reader->cached_file_handles_miss_count());
    EXPECT_NE(vended_accessor.Get()->file(), default_accessor.Get()->file());
  }
  ASSERT_EQ(initial_num_handles + 2, NumCachedFileHandles());

  // The connection is kept while a query holds it, even after its credential expired.
  const int64_t remaining_ms = expiry_ms - UnixMillis();
  if (remaining_ms >= 0) SleepForMs(remaining_ms + 100);
  HdfsFsCache::instance()->ReleaseQueryConnections(vended_query->query_id());
  EXPECT_EQ(initial_num_handles + 2, NumCachedFileHandles());
  {
    FileHandleCache::Accessor accessor;
    ASSERT_OK(io_mgr->GetCachedHdfsFileHandle(
        vended_fs, &tbl_file, mtime, reader.get(), &accessor));
    EXPECT_EQ(3, reader->cached_file_handles_hit_count());
  }

  // Once the last query released it, the connection is disconnected and its cached file
  // handle is closed with it. The handle of the other connection is not affected.
  HdfsFsCache::instance()->ReleaseQueryConnections(other_vended_query->query_id());
  EXPECT_EQ(initial_num_handles + 1, NumCachedFileHandles());
  {
    FileHandleCache::Accessor accessor;
    ASSERT_OK(io_mgr->GetCachedHdfsFileHandle(
        default_fs, &tbl_file, mtime, reader.get(), &accessor));
    EXPECT_EQ(4, reader->cached_file_handles_hit_count());
    EXPECT_EQ(2, reader->cached_file_handles_miss_count());
  }

  // Clean up the handle of the shared connection, which is never disconnected.
  EXPECT_EQ(0, io_mgr->CloseCachedHdfsFileHandles(default_fs));
  EXPECT_EQ(initial_num_handles, NumCachedFileHandles());
  io_mgr->UnregisterContext(reader.get());
  boost::filesystem::remove_all(tbl_dir.parent_path());
}

}

