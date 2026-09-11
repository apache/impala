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

#pragma once

#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include "common/hdfs.h"
#include "common/status.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class QueryState;

/// A (process-wide) cache of hdfsFS objects.
/// These connections are shared across all threads and kept open until the process
/// terminates.
///
/// These connections are leaked, i.e. we never call hdfsDisconnect(). Calls to
/// hdfsDisconnect() by individual threads would terminate all other connections handed
/// out via hdfsConnect() to the same URI, and there is no simple, safe way to call
/// hdfsDisconnect() when process terminates (the proper solution is likely to create a
/// signal handler to detect when the process is killed, but we would still leak when
/// impalad crashes).
///
/// A connection built for a vended credential is configured with that credential's key
/// material and keyed by the credential's storage prefix plus a digest of the material,
/// so every query reading a prefix with the same credential shares one connection and a
/// rotated credential yields a new one. Unlike the connections above, these are private
/// instances (hdfsBuilderSetForceNewInstance()) and are closed once they are no longer
/// useful: a connection is disconnected and dropped from the cache when every query that
/// obtained it has finished (the connection may be held, e.g. by scan ranges, until the
/// fragment instances of the query are done) and either its credential has expired or no
/// query has asked for it for a while (which covers credentials without an expiry, e.g.
/// static keys, once a rotation superseded them). This bounds the cache to the
/// connections of credentials in use. The file handles that the DiskIoMgr's file handle
/// cache holds for such a connection are closed right before it is disconnected; that
/// cache keys the handles by connection, so a handle opened with one credential is never
/// handed to a query that reads the file with another one (or with none).
class HdfsFsCache {
 public:
  using HdfsFsMap = boost::unordered_map<std::string, hdfsFS>;

  static HdfsFsCache* instance() { return HdfsFsCache::instance_.get(); }

  /// Initializes the cache and creates the singleton instance.
  static Status Init();

  /// Get connection to the local filesystem.
  Status GetLocalConnection(hdfsFS* fs);

  /// Get a connection to the filesystem for 'path'.
  ///
  /// Credential resolution order:
  ///   1. If 'qs' is non-null and the query has vended credentials, the entry whose
  ///      storage prefix best matches 'path'. The connection is configured with that
  ///      entry's Hadoop config (key material and credential provider). Fails if that
  ///      credential has expired and could not be refreshed (see QueryCredentials).
  ///   2. Otherwise the process-global S3 credential from S3ConnCredentials::Get().
  ///   3. Otherwise libhdfs uses the core-site.xml defaults.
  ///
  /// 'local_cache' (caller-synchronized) avoids the process-wide lock on repeated
  /// calls for the same path.
  ///
  /// 'options' are optional per-call Hadoop config pairs layered on top of the
  /// credential (options win on conflict); they also widen the cache key.
  ///
  /// A connection missing from the cache is built outside of the cache lock; concurrent
  /// callers asking for the same connection wait for that build instead of starting
  /// their own.
  Status GetConnection(const std::string& path, hdfsFS* fs,
      HdfsFsMap* local_cache = nullptr,
      const std::map<std::string, std::string>* options = nullptr,
      QueryState* qs = nullptr);

  /// Signals that the query 'query_id' no longer holds any connection handed out by
  /// GetConnection() and does no I/O through them anymore. Called by QueryState once
  /// its fragment instances are done, and again when it is destroyed.
  /// Vended-credential connections that became stale (see the class comment) and have
  /// no other live user are disconnected.
  void ReleaseQueryConnections(const TUniqueId& query_id);

  /// Get NameNode info from path, set error message if path is not valid.
  /// Exposed as a static method for testing purpose.
  static std::string GetNameNodeFromPath(const std::string& path, std::string* err);

  /// Builds the connection cache key: the namenode, suffixed with 'credential_key' (the
  /// credential's prefix and material digest, see CredentialKey() in the .cc) and with a
  /// deterministic identity of 'options' when each is non-empty. Exposed so callers that
  /// pre-seed a local cache (tests) can compute the same key that GetConnection() uses.
  static std::string BuildCacheKey(const std::string& namenode,
      const std::string& credential_key,
      const std::map<std::string, std::string>* options);

 private:
  /// Bookkeeping for a connection built for a vended credential, see the class comment.
  struct VendedConnection {
    /// Storage prefix of the credential, for logging.
    std::string prefix;
    /// Expiry of the credential the connection was built with (epoch ms); 0 = never.
    uint64_t expiry_ms = 0;
    /// UnixMillis() of the last time a query obtained the connection from the cache.
    int64_t last_used_ms = 0;
    /// Queries that obtained the connection and have not been released yet.
    std::set<TUniqueId> queries;
  };

  /// Singleton instance. Instantiated in Init().
  static boost::scoped_ptr<HdfsFsCache> instance_;

  /// Connects to 'namenode' with 'cred_props' and 'options' (either may be null) applied
  /// to the connection's Hadoop config. Returns nullptr on failure. Does not touch the
  /// cache, so it is called without 'lock_' held.
  static hdfsFS Connect(const std::string& namenode,
      const std::map<std::string, std::string>* cred_props,
      const std::map<std::string, std::string>* options);

  /// Drops every stale vended connection that no live query uses from the cache and
  /// appends it to 'to_disconnect'; the caller disconnects them once 'lock_' is
  /// released. 'lock_' must be held.
  void EvictStaleVendedConnectionsLocked(std::vector<hdfsFS>* to_disconnect);

  /// Disconnects the vended connections 'conns' that were dropped from the cache, after
  /// closing the file handles the DiskIoMgr's file handle cache holds for them. Does
  /// JNI calls, so it must be called without 'lock_' held.
  static void DisconnectVendedConnections(const std::vector<hdfsFS>& conns);

  /// Protects all members below.
  std::mutex lock_;
  /// Signalled whenever a connection build finishes (successfully or not).
  std::condition_variable build_done_;
  HdfsFsMap fs_map_;
  /// Cache keys of connections currently being built outside of 'lock_'.
  std::set<std::string> in_flight_;
  /// Metadata of the vended-credential connections in 'fs_map_', by cache key.
  std::map<std::string, VendedConnection> vended_conns_;

  HdfsFsCache() { }
  HdfsFsCache(const HdfsFsCache&) = delete;
  HdfsFsCache& operator=(const HdfsFsCache&) = delete;
};

}
