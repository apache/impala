// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_SERVICE_CHILD_QUERY_H
#define IMPALA_SERVICE_CHILD_QUERY_H

#include <string>
#include <boost/thread.hpp>

#include "common/status.h"
#include "impala-server.h"
#include "gen-cpp/TCLIService_types.h"

namespace impala {

class ImpalaServer;

// Child queries are used for implementing statements that consist of one or several
// query statements (e.g., compute stats) that require independent query handles for
// fetching results. Such queries are 'children' of a parent exec state in the sense
// that they are executed in the same session and that child queries are cancelled if
// the parent is cancelled (but not necessarily vice versa).
// For simplicity and consistency, child queries are always executed via HiveServer2,
// regardless of whether the parent session is Beeswax or HiveServer2.
//
// Parent queries are expected to call ExecAndWait() of a child query in a
// separate thread, and then join that thread to wait for child-query completion.
// The parent QueryExecState is independent of the child query's QueryExecState,
// with the exception that the child query selectively checks the parent's status
// for failure/cancellation detection. Child queries should never call into their
// parent's QueryExecState to avoid deadlock.
//
// TODO: Compute stats is the only stmt that requires child queries. Once the
// CatalogService performs background stats gathering the concept of child queries
// will likely become obsolete. Remove this class and all child-query related code.
class ChildQuery {
 public:
  ChildQuery(const std::string& query, ImpalaServer::QueryExecState* parent_exec_state,
      ImpalaServer* parent_server)
    : query_(query),
      parent_exec_state_(parent_exec_state),
      parent_server_(parent_server),
      is_running_(false),
      is_cancelled_(false) {
    DCHECK(!query_.empty());
    DCHECK(parent_exec_state_ != NULL);
    DCHECK(parent_server_ != NULL);
  }

  // Allow child queries to be added to std collections.
  // (boost::mutex's operator= and copy c'tor are private)
  ChildQuery(const ChildQuery& other)
    : query_(other.query_),
      parent_exec_state_(other.parent_exec_state_),
      parent_server_(other.parent_server_),
      is_running_(other.is_running_),
      is_cancelled_(other.is_cancelled_) {}

  // Allow child queries to be added to std collections.
  // (boost::mutex's operator= and copy c'tor are private)
  ChildQuery& operator=(const ChildQuery& other) {
    query_ = other.query_;
    parent_exec_state_ = other.parent_exec_state_;
    parent_server_ = other.parent_server_;
    is_running_ = other.is_running_;
    is_cancelled_ = other.is_cancelled_;
    return *this;
  }

  // Executes this child query through HiveServer2 and fetches all its results.
  Status ExecAndFetch();

  // Cancels and closes the given child query if it is running. Sets is_cancelled_.
  // Child queries can be cancelled by the parent query through QueryExecState::Cancel().
  // Child queries should never cancel their parent to avoid deadlock (but the parent
  // query may decide to cancel itself based on a non-OK status from a child query).
  // Note that child queries have a different QueryExecState than their parent query,
  // so cancellation of a child query does not call into the parent's QueryExecState.
  void Cancel();

  const apache::hive::service::cli::thrift::TTableSchema& result_schema() {
    return meta_resp_.schema;
  }

  const apache::hive::service::cli::thrift::TRowSet& result_data() {
    return fetch_resp_.results;
  }

  // The key in the HS2 conf overlay which indicates to the executing ImpalaServer that
  // this query is a child query.
  static const string PARENT_QUERY_OPT;

 private:
  // Sets the query options from the parent query in child's HS2 request.
  // TODO: Consider moving this function into a more appropriate place.
  void SetQueryOptions(const TQueryOptions& parent_options,
      apache::hive::service::cli::thrift::TExecuteStatementReq* exec_stmt_req);

  // Returns Status::Cancelled if this child query has been cancelled, otherwise OK.
  // Acquires lock_.
  Status IsCancelled();

  // SQL string to be executed.
  std::string query_;

  // Execution state of parent query. Used to synchronize and propagate parent
  // cancellations/failures to this child query. Not owned.
  ImpalaServer::QueryExecState* parent_exec_state_;

  // Parent Impala server used for executing this child query. Not owned.
  ImpalaServer* parent_server_;

  // Result metadata and result rows of query.
  apache::hive::service::cli::thrift::TGetResultSetMetadataResp meta_resp_;
  apache::hive::service::cli::thrift::TFetchResultsResp fetch_resp_;

  // HS2 query handle. Set in ExecChildQuery().
  apache::hive::service::cli::thrift::TOperationHandle hs2_handle_;

  // Protects is_running_ and is_cancelled_ to ensure idempotent cancellations.
  boost::mutex lock_;

  // Indicates whether this query is running. False if the query has not started yet
  // or if the query has finished either successfully or because of an error.
  bool is_running_;

  // Indicates whether this child query has been cancelled. Set in Cancel().
  bool is_cancelled_;
};

}

#endif
