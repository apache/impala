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

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "common/status.h"

namespace impala {

class HdfsTableDescriptor;
class QueryState;

/// A storage credential vended by an Iceberg REST catalog for one storage prefix.
/// Built from a TCredential by CredentialEntryFromThrift() (descriptors.cc).
struct CredentialEntry {
  /// Storage prefix the credential applies to, e.g. "s3a://bucket/warehouse/db/tbl".
  std::string prefix;
  /// Hadoop config properties holding the key material and the credential provider to
  /// use with it (fs.s3a.access.key, fs.s3a.secret.key, fs.s3a.session.token,
  /// fs.s3a.aws.credentials.provider), as translated by the frontend. HdfsFsCache
  /// applies them to the connection it builds for the prefix.
  std::map<std::string, std::string> config;
  /// Absolute expiry (epoch ms); 0 = never expires. Carried through from the catalog;
  /// not consulted by the lookup.
  uint64_t expiry_ms = 0;
};

/// Entries are immutable once registered; lookups hand out shared ownership so a later
/// replacement of the set element cannot invalidate callers.
using CredentialEntryPtr = std::shared_ptr<const CredentialEntry>;

/// Identifies a table uniquely within a query: (database, table) name pair.
using TableKey = std::pair<std::string, std::string>;

/// Orders entries by prefix, descending, so the first prefix-of-path hit in a forward
/// scan is the longest one (the extensions of a prefix sort before the prefix itself).
/// Transparent (is_transparent + the string_view overloads) so the set can be searched
/// by a bare prefix string, see the entries.find(prefix) calls in the .cc.
struct CredentialEntryByPrefix {
  using is_transparent = void;
  bool operator()(const CredentialEntryPtr& a, const CredentialEntryPtr& b) const {
    return a->prefix > b->prefix;
  }
  bool operator()(const CredentialEntryPtr& a, std::string_view b) const {
    return a->prefix > b;
  }
  bool operator()(std::string_view a, const CredentialEntryPtr& b) const {
    return a > b->prefix;
  }
};

/// Per-query bookkeeping for vended storage credentials.
///
/// Each QueryState owns one instance. The QueryState registers the credentials of every
/// table in the query's descriptor table (scanned tables and DML targets alike) before
/// it starts its fragment instances, so each backend that executes a part of the query
/// knows them from the start, whichever operators it runs. A coordinator that executes
/// no fragment instance registers the DML target's credentials before finalizing the
/// DML. HdfsFsCache::GetConnection() looks up the credential covering the path it is
/// asked for and builds (or reuses) a connection configured with that credential's key
/// material.
///
/// Thread-safety: all public methods are thread-safe. A single mutex guards the map.
class QueryCredentials {
 public:
  QueryCredentials() = default;
  QueryCredentials(const QueryCredentials&) = delete;
  QueryCredentials& operator=(const QueryCredentials&) = delete;

  /// Registers 'cred' for 'cred.prefix' under table ('table_db', 'table_name'). An
  /// existing entry for the same prefix is only replaced when 'cred' expires later.
  Status RegisterVendedCredential(const std::string& table_db,
      const std::string& table_name, const CredentialEntry& cred) WARN_UNUSED_RESULT;

  /// Registers the credentials vended for 'table' (see RegisterVendedCredential()). A
  /// no-op for a table without vended credentials.
  Status RegisterTableCredentials(const HdfsTableDescriptor& table) WARN_UNUSED_RESULT;

  /// Returns the entry whose prefix is the longest prefix of 'path' (on a path-component
  /// boundary), or nullptr.
  CredentialEntryPtr FindCredential(const std::string& path, QueryState* qs);

  /// True when no credential has been registered. Lock-free fast path for the
  /// connection cache.
  bool empty() const { return num_entries_.load(std::memory_order_acquire) == 0; }

 private:
  using EntrySet = std::set<CredentialEntryPtr, CredentialEntryByPrefix>;

  /// Longest-prefix match across all tables. 'lock_' must be held.
  CredentialEntryPtr FindBestMatchLocked(const std::string& path);

  mutable std::mutex lock_;
  std::map<TableKey, EntrySet> tables_;
  std::atomic<int64_t> num_entries_{0};
};

} // namespace impala
