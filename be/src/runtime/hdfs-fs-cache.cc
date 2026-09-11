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

#include "runtime/hdfs-fs-cache.h"

#include <map>
#include <mutex>
#include <string>
#include <vector>

#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/query-credentials.h"
#include "runtime/query-state.h"
#include "runtime/s3-conn-credentials.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/hash-util.h"
#include "util/hdfs-util.h"
#include "util/test-info.h"
#include "util/time.h"

#include "common/names.h"

using namespace strings;

namespace impala {

scoped_ptr<HdfsFsCache> HdfsFsCache::instance_;

namespace {

// A deterministic identity string for a set of config properties. The map is
// key-sorted so the result is order-independent. Returns "" for a null/empty set.
string PropsIdentity(const map<string, string>* props) {
  if (props == nullptr || props->empty()) return "";
  string id;
  for (const auto& kv : *props) {
    id += kv.first;
    id += '=';
    id += kv.second;
    id += '\n';
  }
  return id;
}

// The cache key suffix for a connection built with 'cred': its prefix plus a digest of
// its key material, so that a rotated credential for the same prefix maps to a new
// connection while no secret is embedded in the key.
string CredentialKey(const CredentialEntry& cred) {
  const string material = PropsIdentity(&cred.config);
  const uint64_t digest = HashUtil::FastHash64(material.data(), material.size(), 0);
  return cred.prefix + '\0' + std::to_string(digest);
}

// A vended connection that no query asked for in this long is closed (once no query
// holds it), even if its credential has not expired.
constexpr int64_t IDLE_VENDED_CONN_TIMEOUT_MS = 60 * 60 * 1000;

} // namespace

string HdfsFsCache::BuildCacheKey(const string& namenode, const string& credential_key,
    const map<string, string>* options) {
  string cache_key = namenode;
  if (!credential_key.empty()) cache_key += '\0' + credential_key;
  const string options_id = PropsIdentity(options);
  if (!options_id.empty()) cache_key += '\0' + options_id;
  return cache_key;
}

Status HdfsFsCache::Init() {
  DCHECK(HdfsFsCache::instance_.get() == NULL);
  HdfsFsCache::instance_.reset(new HdfsFsCache());
  return Status::OK();
}

hdfsFS HdfsFsCache::Connect(const string& namenode, const map<string, string>* cred_props,
    const map<string, string>* options) {
  hdfsBuilder* hdfs_builder = hdfsNewBuilder();
  hdfsBuilderSetNameNode(hdfs_builder, namenode.c_str());
  if (cred_props != nullptr || options != nullptr) {
    // Use a new instance of the filesystem object to be sure that it picks up the
    // configuration changes we're going to make. Without this call, a cached
    // filesystem object is used which is unaffected by calls to
    // hdfsBuilderConfSetStr(). This is unexpected behavior in the HDFS API, but is
    // unlikely to change.
    hdfsBuilderSetForceNewInstance(hdfs_builder);
    if (cred_props != nullptr) {
      for (const auto& kv : *cred_props) {
        hdfsBuilderConfSetStr(hdfs_builder, kv.first.c_str(), kv.second.c_str());
      }
    }
    if (options != nullptr) {
      for (const auto& kv : *options) {
        hdfsBuilderConfSetStr(hdfs_builder, kv.first.c_str(), kv.second.c_str());
      }
    }
  }
  return hdfsBuilderConnect(hdfs_builder);
}

Status HdfsFsCache::GetConnection(const string& path, hdfsFS* fs,
    HdfsFsMap* local_cache, const map<string, string>* options, QueryState* qs) {
  string err;
  const string& namenode = GetNameNodeFromPath(path, &err);
  if (!err.empty()) return Status(err);
  DCHECK(!namenode.empty());

  // Resolve the credential properties for this connection (see the header). 'entry'
  // keeps the vended credential alive while its config is applied below.
  string credential_key;
  CredentialEntryPtr entry;
  const map<string, string>* cred_props = nullptr;
  if (qs != nullptr && !qs->query_credentials()->empty()) {
    entry = qs->query_credentials()->FindCredential(path, qs);
    if (entry != nullptr) {
      credential_key = CredentialKey(*entry);
      cred_props = &entry->config;
    }
  }
  if (cred_props == nullptr) {
    const map<string, string>& global = S3ConnCredentials::Get();
    if (!global.empty()) cred_props = &global;
  }
  if (options != nullptr && options->empty()) options = nullptr;
  // Without a vended credential and options the key is the namenode itself, which is
  // the case for nearly every call, so do not build a copy of it.
  const string extended_key = credential_key.empty() && options == nullptr ?
      string() : BuildCacheKey(namenode, credential_key, options);
  const string& cache_key = extended_key.empty() ? namenode : extended_key;

  // First, check the local cache to avoid taking the global lock.
  if (local_cache != nullptr) {
    HdfsFsMap::iterator local_iter = local_cache->find(cache_key);
    if (local_iter != local_cache->end()) {
      *fs = local_iter->second;
      return Status::OK();
    }
  }

  // Otherwise, check the global cache. If the connection is missing and nobody else is
  // building it, claim the build and do it below without holding the lock: connecting
  // goes through JNI and may be slow (e.g. S3A client setup), and it must not block
  // lookups of unrelated connections.
  *fs = nullptr;
  vector<hdfsFS> to_disconnect;
  {
    unique_lock<mutex> l(lock_);
    while (true) {
      HdfsFsMap::iterator i = fs_map_.find(cache_key);
      if (i != fs_map_.end()) {
        *fs = i->second;
        break;
      }
      if (in_flight_.insert(cache_key).second) break;
      // Another thread is building this connection; wait for it and re-check.
      build_done_.wait(l);
    }
    if (*fs != nullptr && entry != nullptr) {
      VendedConnection& conn = vended_conns_[cache_key];
      conn.last_used_ms = UnixMillis();
      conn.queries.insert(qs->query_id());
    }
  }

  if (*fs == nullptr) {
    hdfsFS new_fs = Connect(namenode, cred_props, options);
    // The error state is thread-local and must be read before anything else runs.
    Status status;
    if (new_fs == nullptr) {
      status = Status(GetHdfsErrorMsg("Failed to connect to FS: ", namenode));
    }
    {
      lock_guard<mutex> l(lock_);
      in_flight_.erase(cache_key);
      build_done_.notify_all();
      if (new_fs != nullptr) {
        VLOG(1) << "Built hdfsFS for namenode '" << namenode << "' credential_prefix='"
                << (entry != nullptr ? entry->prefix : "")
                << "' num_options=" << (options != nullptr ? options->size() : 0);
        fs_map_.insert(make_pair(cache_key, new_fs));
        if (entry != nullptr) {
          VendedConnection& conn = vended_conns_[cache_key];
          conn.prefix = entry->prefix;
          conn.expiry_ms = entry->expiry_ms;
          conn.last_used_ms = UnixMillis();
          conn.queries.insert(qs->query_id());
          // The cache only grows here, so sweeping on insert bounds it.
          EvictStaleVendedConnectionsLocked(&to_disconnect);
        }
      }
    }
    DisconnectVendedConnections(to_disconnect);
    RETURN_IF_ERROR(status);
    *fs = new_fs;
  }
  DCHECK(*fs != NULL);
  // Populate the local cache for the next lookup.
  if (local_cache != nullptr) {
    local_cache->insert(make_pair(cache_key, *fs));
  }
  return Status::OK();
}

void HdfsFsCache::ReleaseQueryConnections(const TUniqueId& query_id) {
  vector<hdfsFS> to_disconnect;
  {
    lock_guard<mutex> l(lock_);
    bool released = false;
    for (auto& kv : vended_conns_) released |= kv.second.queries.erase(query_id) > 0;
    if (released) EvictStaleVendedConnectionsLocked(&to_disconnect);
  }
  DisconnectVendedConnections(to_disconnect);
}

void HdfsFsCache::DisconnectVendedConnections(const vector<hdfsFS>& conns) {
  if (conns.empty()) return;
  // Not available in some backend tests.
  io::DiskIoMgr* io_mgr = ExecEnv::GetInstance() == nullptr ?
      nullptr : ExecEnv::GetInstance()->disk_io_mgr();
  for (hdfsFS conn : conns) {
    // The file handle cache keeps the handles that the queries opened on the connection
    // (it keys them by connection, so no other query got them). Close them first: they
    // must not outlive the connection, and nobody can ask for them anymore.
    const size_t num_handles_in_use =
        io_mgr == nullptr ? 0 : io_mgr->CloseCachedHdfsFileHandles(conn);
    if (UNLIKELY(num_handles_in_use > 0)) {
      // Not expected: the queries that used the connection are done with their I/O. Leave
      // the connection open, like the connections of the other filesystems, rather than
      // closing it under a reader. It is not in the cache anymore, so its handles are not
      // reused and age out of the file handle cache.
      LOG(WARNING) << "Not disconnecting the hdfsFS of a vended credential: "
                   << num_handles_in_use << " of its file handles are in use.";
      continue;
    }
    hdfsDisconnect(conn);
  }
}

void HdfsFsCache::EvictStaleVendedConnectionsLocked(vector<hdfsFS>* to_disconnect) {
  const int64_t now_ms = UnixMillis();
  for (auto it = vended_conns_.begin(); it != vended_conns_.end();) {
    const VendedConnection& conn = it->second;
    const bool expired =
        conn.expiry_ms != 0 && static_cast<int64_t>(conn.expiry_ms) <= now_ms;
    const bool idle = conn.last_used_ms + IDLE_VENDED_CONN_TIMEOUT_MS <= now_ms;
    if (!(expired || idle) || !conn.queries.empty()) {
      ++it;
      continue;
    }
    HdfsFsMap::iterator fs_it = fs_map_.find(it->first);
    DCHECK(fs_it != fs_map_.end());
    if (fs_it != fs_map_.end()) {
      // The connection is a private instance (see Connect()), so closing it cannot
      // affect any other connection, and no query holds it anymore.
      VLOG(1) << "Dropping hdfsFS of " << (expired ? "expired" : "idle")
              << " vended credential for prefix '" << conn.prefix << "'";
      to_disconnect->push_back(fs_it->second);
      fs_map_.erase(fs_it);
    }
    it = vended_conns_.erase(it);
  }
}

Status HdfsFsCache::GetLocalConnection(hdfsFS* fs) {
  return GetConnection("file:///", fs);
}

string HdfsFsCache::GetNameNodeFromPath(const string& path, string* err) {
  string namenode;
  const string local_fs("file:/");
  size_t n = path.find("://");

  err->clear();
  if (n == string::npos) {
    if (path.compare(0, local_fs.length(), local_fs) == 0) {
      // Hadoop Path routines strip out consecutive /'s, so recognize 'file:/blah'.
      namenode = "file:///";
    } else {
      // Path is not qualified, so use the default FS.
      namenode = "default";
    }
  } else if (n == 0) {
    *err = Substitute("Path missing scheme: $0", path);
  } else {
    // Path is qualified, i.e. "scheme://authority/path/to/file".  Extract
    // "scheme://authority/".
    n = path.find('/', n + 3);
    if (n == string::npos) {
      *err = Substitute("Path missing '/' after authority: $0", path);
    } else {
      // Include the trailing '/' for local filesystem case, i.e. "file:///".
      namenode = path.substr(0, n + 1);
    }
  }
  return namenode;
}

}
