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

#include "runtime/query-credentials.h"

#include <limits>
#include <mutex>

#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/query-state.h"

#include "common/names.h"

namespace impala {

namespace {

/// Expiry used for ordering: a credential that never expires sorts after all others.
int64_t EffectiveExpiry(const CredentialEntry& e) {
  return e.expiry_ms == 0 ? std::numeric_limits<int64_t>::max()
                          : static_cast<int64_t>(e.expiry_ms);
}

/// True when 'prefix' covers 'path': 'path' starts with 'prefix' and the match ends on
/// a path-component boundary, so "s3a://b/db/tbl" covers "s3a://b/db/tbl/f.parquet" but
/// not "s3a://b/db/tbl_backup/f.parquet".
bool PrefixCoversPath(const string& prefix, const string& path) {
  if (prefix.empty() || path.size() < prefix.size()) return false;
  if (path.compare(0, prefix.size(), prefix) != 0) return false;
  return path.size() == prefix.size() || prefix.back() == '/'
      || path[prefix.size()] == '/';
}

} // namespace

Status QueryCredentials::RegisterVendedCredential(const string& table_db,
    const string& table_name, const CredentialEntry& cred) {
  auto entry = std::make_shared<const CredentialEntry>(cred);
  {
    lock_guard<mutex> l(lock_);
    EntrySet& entries = tables_[TableKey{table_db, table_name}];
    auto it = entries.find(cred.prefix);
    if (it != entries.end()) {
      // Keep whichever credential lives longer.
      if (EffectiveExpiry(**it) >= EffectiveExpiry(cred)) return Status::OK();
      entries.erase(it);
      num_entries_.fetch_sub(1, std::memory_order_acq_rel);
    }
    entries.insert(entry);
    num_entries_.fetch_add(1, std::memory_order_acq_rel);
  }
  VLOG(1) << "Registered vended credential: table=" << table_db << "." << table_name
          << " prefix='" << cred.prefix << "' expiry_ms=" << cred.expiry_ms;
  return Status::OK();
}

Status QueryCredentials::RegisterTableCredentials(const HdfsTableDescriptor& table) {
  for (const CredentialEntry& cred : table.StorageCredentials()) {
    RETURN_IF_ERROR(RegisterVendedCredential(table.database(), table.name(), cred));
  }
  return Status::OK();
}

CredentialEntryPtr QueryCredentials::FindBestMatchLocked(const string& path) {
  CredentialEntryPtr best;
  size_t best_len = 0;
  for (const auto& [key, entries] : tables_) {
    // Entries are sorted by prefix descending: the first hit is this table's longest.
    for (const CredentialEntryPtr& e : entries) {
      if (!PrefixCoversPath(e->prefix, path)) continue;
      if (e->prefix.size() > best_len) {
        best = e;
        best_len = e->prefix.size();
      }
      break;
    }
  }
  return best;
}

CredentialEntryPtr QueryCredentials::FindCredential(const string& path, QueryState* qs) {
  DCHECK(qs != nullptr);
  if (empty()) return nullptr;
  lock_guard<mutex> l(lock_);
  return FindBestMatchLocked(path);
}

} // namespace impala
