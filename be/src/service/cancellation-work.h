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

#include <vector>

#include "common/status.h"
#include "gen-cpp/Types_types.h"

namespace impala {

/// Categorisation of causes of cancellation.
enum class CancellationWorkCause {
  // The Impala Server terminated the query, e.g. because it exceeded a timeout or
  // resource limit.
  TERMINATED_BY_SERVER,
  // The query is being terminated because a backend failed. We can skip cancelling the
  // query if the fragment instances running on that backend all completed.
  BACKEND_FAILED
};

/// Work item for ImpalaServer::cancellation_thread_pool_.
/// This class needs to support move construction and assignment for use in ThreadPool.
class CancellationWork {
 public:
  // Empty constructor needed to make ThreadPool happy.
  CancellationWork()
    : cause_(CancellationWorkCause::TERMINATED_BY_SERVER), unregister_(false) {}

  // Construct a TERMINATED_BY_SERVER CancellationWork instance.
  static CancellationWork TerminatedByServer(
      const TUniqueId& query_id, const Status& error, bool unregister) {
    return CancellationWork(
        query_id, CancellationWorkCause::TERMINATED_BY_SERVER, error, {}, unregister);
  }

  // Construct a BACKEND_FAILURE CancellationWork instance.
  static CancellationWork BackendFailure(
      const TUniqueId& query_id, const std::vector<NetworkAddressPB>& failed_backends) {
    return CancellationWork(query_id, CancellationWorkCause::BACKEND_FAILED, Status::OK(),
        failed_backends, false);
  }

  const TUniqueId& query_id() const { return query_id_; }
  CancellationWorkCause cause() const { return cause_; }
  const Status& error() const {
    DCHECK_ENUM_EQ(cause_, CancellationWorkCause::TERMINATED_BY_SERVER);
    return error_;
  }
  const std::vector<NetworkAddressPB>& failed_backends() const {
    DCHECK_ENUM_EQ(cause_, CancellationWorkCause::BACKEND_FAILED);
    return failed_backends_;
  }
  bool unregister() const { return unregister_; }

 private:
  CancellationWork(const TUniqueId& query_id, CancellationWorkCause cause,
      const Status& error, const std::vector<NetworkAddressPB>& failed_backends,
      bool unregister)
    : query_id_(query_id),
      cause_(cause),
      error_(error),
      failed_backends_(failed_backends),
      unregister_(unregister) {
    DCHECK(cause_ != CancellationWorkCause::TERMINATED_BY_SERVER || !error.ok());
    DCHECK(cause_ != CancellationWorkCause::BACKEND_FAILED || !failed_backends.empty());
  }

  // ID of query to be cancelled.
  TUniqueId query_id_;

  // Cause of the expiration.
  CancellationWorkCause cause_;

  // If 'cause_' is TERMINATED_BY_SERVER, the error containing human-readable explanation
  // of the cancellation. Otherwise not used.
  Status error_;

  // If cause is BACKEND_FAILED, all of the backend that were detected to fail. Otherwise
  // not used.
  std::vector<NetworkAddressPB> failed_backends_;

  // If true, unregister the query after cancelling it.
  bool unregister_;
};
} // namespace impala
