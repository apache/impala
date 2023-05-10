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

#include <mutex>
#include <string>

#include "common/status.h"
#include "impala-server.h"
#include "gen-cpp/TCLIService_types.h"

namespace impala {

class ImpalaServer;

/// Child queries are used for implementing statements that consist of one or several
/// query statements (e.g., compute stats) that require independent query handles for
/// fetching results. Such queries are 'children' of a parent exec state in the sense
/// that they are executed in the same session and that child queries are cancelled if
/// the parent is cancelled (but not necessarily vice versa).
/// For simplicity and consistency, child queries are always executed via HiveServer2,
/// regardless of whether the parent session is Beeswax or HiveServer2.
//
/// Parent queries are expected to call ExecAndWait() of a child query in a
/// separate thread, and then join that thread to wait for child-query completion.
/// The parent ClientRequestState is independent of the child query's ClientRequestState,
/// with the exception that the child query selectively checks the parent's status
/// for failure/cancellation detection. Child queries should never call into their
/// parent's ClientRequestState to avoid deadlock.
//
/// TODO: Compute stats is the only stmt that requires child queries. Once the
/// CatalogService performs background stats gathering the concept of child queries
/// will likely become obsolete. Remove this class and all child-query related code.
class ChildQuery {
 public:
  ChildQuery(const std::string& query, ClientRequestState* parent_request_state,
      ImpalaServer* parent_server, RuntimeProfile* profile, ObjectPool* profile_pool)
    : query_(query),
      parent_request_state_(parent_request_state),
      parent_server_(parent_server),
      profile_(profile),
      profile_pool_(profile_pool),
      is_running_(false),
      is_cancelled_(false) {
    DCHECK(!query_.empty());
    DCHECK(parent_request_state_ != NULL);
    DCHECK(parent_server_ != NULL);
  }

  /// Allow child queries to be added to std collections.
  /// (std::mutex's operator= and copy c'tor are private)
  ChildQuery(const ChildQuery& other)
    : query_(other.query_),
      parent_request_state_(other.parent_request_state_),
      parent_server_(other.parent_server_),
      profile_(other.profile_),
      profile_pool_(other.profile_pool_),
      is_running_(other.is_running_),
      is_cancelled_(other.is_cancelled_) {}

  /// Allow child queries to be added to std collections.
  /// (std::mutex's operator= and copy c'tor are private)
  ChildQuery& operator=(const ChildQuery& other) {
    query_ = other.query_;
    parent_request_state_ = other.parent_request_state_;
    parent_server_ = other.parent_server_;
    is_running_ = other.is_running_;
    is_cancelled_ = other.is_cancelled_;
    return *this;
  }

  /// Executes this child query through HiveServer2 and fetches all its results.
  Status ExecAndFetch();

  /// Cancels and closes the given child query if it is running. Sets is_cancelled_.
  /// Child queries can be cancelled by the parent query through ClientRequestState::Cancel().
  /// Child queries should never cancel their parent to avoid deadlock (but the parent
  /// query may decide to cancel itself based on a non-OK status from a child query).
  /// Note that child queries have a different ClientRequestState than their parent query,
  /// so cancellation of a child query does not call into the parent's ClientRequestState.
  void Cancel();

  const apache::hive::service::cli::thrift::TTableSchema& result_schema() {
    return meta_resp_.schema;
  }

  const apache::hive::service::cli::thrift::TRowSet& result_data() {
    return fetch_resp_.results;
  }

  /// The key in the HS2 conf overlay which indicates to the executing ImpalaServer that
  /// this query is a child query.
  static const string PARENT_QUERY_OPT;

 private:
  /// Sets the query options from the parent query in child's HS2 request.
  /// TODO: Consider moving this function into a more appropriate place.
  void SetQueryOptions(
      apache::hive::service::cli::thrift::TExecuteStatementReq* exec_stmt_req);

  /// Returns Status::Cancelled if this child query has been cancelled, otherwise OK.
  /// Acquires lock_.
  Status IsCancelled();

  /// SQL string to be executed.
  std::string query_;

  /// Execution state of parent query. Used to synchronize and propagate parent
  /// cancellations/failures to this child query. Not owned.
  ClientRequestState* parent_request_state_;

  /// Parent Impala server used for executing this child query. Not owned.
  ImpalaServer* parent_server_;

  /// The profile for the query is retrieved after Close() and added as a child.
  RuntimeProfile* profile_;
  ObjectPool* profile_pool_;

  /// Result metadata and result rows of query.
  apache::hive::service::cli::thrift::TGetResultSetMetadataResp meta_resp_;
  apache::hive::service::cli::thrift::TFetchResultsResp fetch_resp_;

  /// HS2 query handle. Set in ExecChildQuery().
  apache::hive::service::cli::thrift::TOperationHandle hs2_handle_;

  /// Protects is_running_ and is_cancelled_ to ensure idempotent cancellations.
  std::mutex lock_;

  /// Indicates whether this query is running. False if the query has not started yet
  /// or if the query has finished either successfully or because of an error.
  bool is_running_;

  /// Indicates whether this child query has been cancelled. Set in Cancel().
  bool is_cancelled_;
};

/// Asynchronously executes a set of child queries in a separate thread.
///
/// ExecAsync() is called at most once per executor to execute a set of child queries
/// asynchronously. After ExecAsync() is called, either WaitForAll() or Cancel() must be
/// called to ensure that the child queries are no longer executing before destroying the
/// object.
class ChildQueryExecutor {
 public:
  ChildQueryExecutor();
  ~ChildQueryExecutor();

  /// Asynchronously executes 'child_queries' one by one in a new thread. 'child_queries'
  /// must be non-empty. May clear or modify the 'child_queries' arg. Can only be called
  /// once. Does nothing if Cancel() was already called.
  Status ExecAsync(std::vector<ChildQuery>&& child_queries) WARN_UNUSED_RESULT;

  /// Waits for all child queries to complete successfully or with an error. Returns a
  /// non-OK status if a child query fails. Returns OK if ExecAsync() was not called,
  /// Cancel() was called before an error occurred, or if all child queries finished
  /// successfully. If returning OK, populates 'completed_queries' with the completed
  /// queries. Any returned ChildQueries remain owned by the executor. Should not be
  /// called concurrently with ExecAsync(). After WaitForAll() returns, the object can
  /// safely be destroyed.
  Status WaitForAll(std::vector<ChildQuery*>* completed_queries);

  /// Cancels all child queries and prevents any more from starting. Returns once all
  /// child queries are cancelled, after which the object can safely be destroyed. Can
  /// be safely called concurrently with ExecAsync() or WaitForAll().
  void Cancel();

 private:
  /// Serially executes the queries in child_queries_ by calling the child query's
  /// ExecAndWait(). This function blocks until all queries complete and is run
  /// in 'child_queries_thread_'.
  /// Sets 'child_queries_status_'.
  void ExecChildQueries();

  /// Protects all fields below.
  /// Should not be held at the same time as 'ChildQuery::lock_'.
  SpinLock lock_;

  /// True if cancellation of child queries has been initiated and no more child queries
  /// should be started.
  bool is_cancelled_;

  /// True if 'child_queries_thread_' is in the process of executing child queries.
  /// Set to false by 'child_queries_thread_' just before it exits. 'is_running_' must
  /// be false when ChildQueryExecutor is destroyed: once execution is started,
  /// WaitForAll() or Cancel() must be called to ensure the thread exits.
  bool is_running_;

  /// List of child queries to be executed. Not modified after it is initially populated,
  /// so safe to read without holding 'lock_' if 'is_running_' or 'is_cancelled_' is
  /// true, or 'child_queries_thread_' is non-NULL.
  std::vector<ChildQuery> child_queries_;

  /// Thread to execute 'child_queries_' in. Immutable after the first time it is set or
  /// after 'is_cancelled_' is true.
  std::unique_ptr<Thread> child_queries_thread_;

  /// The status of the child queries. The status is OK iff all child queries complete
  /// successfully. Otherwise, status contains the error of the first child query that
  /// failed (child queries are executed serially and abort on the first error).
  /// Immutable after 'child_queries_thread_' exits
  Status child_queries_status_;
};
}
