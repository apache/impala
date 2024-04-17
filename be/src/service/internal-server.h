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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "gen-cpp/Query_types.h"
#include "gen-cpp/Types_types.h"
#include "rpc/thrift-server.h"

namespace impala {

  typedef std::shared_ptr<std::vector<std::string>> query_results;
  typedef std::vector<std::pair<std::string, std::string>> results_columns;

  /// Enables Impala coordinators to submit queries to themselves.
  ///
  /// Internally, this class directly calls the methods on ImpalaServer that are called by
  /// the Beeswax and HS2 servers. Thus, it does not strictly adhere to either protocol.
  /// Since Impala requires sessions to have a defined type, sessions created by
  /// InternalServer show up as Beeswax sessions.
  ///
  /// Since this class directly calls ImpalaServer methods, it bypasses all authentication
  /// methods.
  ///
  /// Usage:
  ///   The easiest way to use this class is to call the `ExecuteAndFetchAllText` function
  ///   which runs the provided sql, returns all the results, and closes the query and
  ///   session. This function is useful for running create/insert/update queries that do
  ///   not return many results.
  ///
  ///   The next way is to call the `SubmitAndWait` function. This function runs the
  ///   provided sql and blocks until results become available. Retrieving the results can
  ///   be done by leveraging the query id returned by this method. The `CloseQuery` and
  ///   `CloseSession` functions must be called by clients once all results are read.
  ///
  ///   The lowest level function is `SubmitQuery`. This function starts a query running
  ///   and returns immediately. Opening a connection/session, waiting for results,
  ///   retrieving results, and closing the query/session is left up to the client.
  class InternalServer {
    public:
      virtual ~InternalServer() {}

      using QueryOptionMap = std::map<TImpalaQueryOptions::type, string>;

      /// Creates and registers a new connection and session.
      ///
      /// Parameters:
      ///   `user_name`      Specifies the username that will be reported as running this
      ///                    query.
      ///   `new_session_id` Output parameter that will be populated with the id of the
      ///                    newly created session.
      ///   `query_opts`     Optional, contains query options that will apply to all
      ///                    queries executed during this session.
      ///
      /// Return:
      ///   `impala::Status` Indicates the result of opening the new session.
      virtual Status OpenSession(const std::string& user_name, TUniqueId& new_session_id,
          const QueryOptionMap& query_opts = {}) = 0;

      /// Closes a given session cleaning up all associated resources.
      ///
      /// Parameters:
      ///   `session_id` Id of the session that will be closed.
      ///
      /// Return:
      ///   `bool` Indicates if the provided session id was for a known session created
      ///          by the `OpenSession` function.
      virtual bool CloseSession(const impala::TUniqueId& session_id) = 0;

      /// Executes a given query cleaning up after the query has completed. Results are
      /// never retrieved.
      ///
      /// Parameters:
      ///   `user_name`     Specifies the username that will be reported as running this
      ///                   query.
      ///   `sql`           Text of the sql query/ddl/dml to run.
      ///   `query_opts`    Optional, contains query options that will apply to all
      ///                   queries executed during this session.
      ///   `persist_in_db` Optional boolean indicating if the query data should be
      ///                   written to the completed queries table after it is closed.
      ///                   Defaults to `true`.
      ///   `query_id`      Optional output parameter, if specified, it will be
      ///                   overwritten with the id of the query that was executed. Since
      ///                   the query is closed by this function, the query id is
      ///                   informational only.
      ///
      /// Return:
      ///   `impala::Status` Indicates the result of submitting the query and waiting for
      ///                    it to return.
      virtual Status ExecuteIgnoreResults(const std::string& user_name,
          const std::string& sql, const QueryOptionMap& query_opts = {},
          const bool persist_in_db = true, TUniqueId* query_id = nullptr) = 0;

      /// Creates a new session under the specified user and submits a query under that
      /// session. No authentication is performed. Blocks until result rows are available.
      /// Then, populates all result rows. Finally, cleans up the query and session.
      ///
      /// Intended for use as a convenience method when query results are small.
      ///
      /// Parameters:
      ///   `user_name` Specifies the username that will be reported as running this
      ///               query.
      ///   `sql`       Text of the sql query/ddl/dml to run.
      ///   `results`   Output parameter containing all result rows from the query. If
      ///               this vector has existing elements, they will be left in place with
      ///               result rows added at the end of the vector.
      ///   `columns`   Optional output parameter where each element is a pair with the
      ///               first element being the name of the column and the second element
      ///               being the column type. Existing elements in the vector will be
      ///               left in place with column pairs appended to the end of the vector.
      ///               If this parameter is `nullptr`, then the list of columns is not
      ///               generated and this parameter's value will remain `nullptr`.
      ///   `query_id`  Optional output parameter, if specified, it will be overwritten
      ///               with the id of the query that was executed. Since the query is
      ///               closed by this function, the query id is informational only.
      ///
      /// Return:
      ///   `impala::Status` indicating the result of submitting the query and waiting for
      ///   it to return.
      virtual Status ExecuteAndFetchAllText(const std::string& user_name,
          const std::string& sql, query_results& results,
          results_columns* columns = nullptr, TUniqueId* query_id = nullptr) = 0;

      /// Creates a new session under the specified user and submits a query under that
      /// session. No authentication is performed. Blocks until result rows are available.
      ///
      /// After retrieving the results, clients must call `CloseQuery` and `CloseSession`
      /// to properly close and clean up the query and session.
      ///
      /// Parameters:
      ///   `user_name`      Specifies the username that will be reported as running this
      ///                    query.
      ///   `sql`            Text of the sql query/ddl/dml to run.
      ///   `new_session_id` Output parameter that will be set to the id of the
      ///                          newly created session.
      ///   `new_query_id`   Output parameter that will be set to the id of the
      ///                          newly started query.
      ///   `query_opts`     Optional, contains query options that will apply to all
      ///                    queries executed by this session opened by this function.
      ///   `persist_in_db`  Optional boolean indicating if the query data should be
      ///                    written to the completed queries table after it is closed.
      ///                    Defaults to `true`.
      ///
      /// Return:
      ///   `impala::Status` Indicates the result of submitting and waiting for the query.
      virtual Status SubmitAndWait(const std::string& user_name, const std::string& sql,
          TUniqueId& new_session_id, TUniqueId& new_query_id,
          const QueryOptionMap& query_opts = {}, const bool persist_in_db = true) = 0;

      /// Waits until the given query has results available.
      ///
      /// Parameters:
      ///   `query_id` Input/output parameter that contains the id of a query to wait for
      ///              it to produce results. The value of this parameter will change if
      ///              the query was automatically retried by the coordinator.
      virtual Status WaitForResults(TUniqueId& query_id) = 0;

      /// Submits a query to the current Impala coordinator under the provided session and
      /// sets the query as in-flight before returning.
      ///
      /// Parameters:
      ///   `sql`           Text of the sql query/ddl/dml to run.
      ///   `session_id`    Id of the session that will run the query.
      ///   `new_query_id`  Output parameter that will be set to the id of the newly
      ///                   started query.
      ///   `persist_in_db` Optional boolean indicating if the query data should be
      ///                   written to the completed queries table after it is closed.
      ///                   Defaults to `true`.
      ///
      /// Return:
      ///   `impala::Status` Indicates the result of submitting the query.
      virtual Status SubmitQuery(const std::string& sql,
          const impala::TUniqueId& session_id, TUniqueId& new_query_id,
          const bool persist_in_db = true) = 0;

      /// Retrieves all result rows for a given query. The query must have already been
      /// submitted and one of the Wait methods called on the query to ensure results are
      /// available.
      ///
      /// Note: Assumes the query represented by `query_id` was successful as this
      ///       function does not check that the query status is a successful status.
      ///
      /// Parameters:
      ///   `query_id`      Id of a query that was submitted and has results available.
      ///   `query_results` Output parameter containing all result rows from the query. If
      ///                   this vector has existing elements, they will be left in place
      ///                   with result rows added at the end of the vector.
      ///   `columns`       Optional output parameter where each element is a pair with
      ///                   the first element being the name of the column and the second
      ///                   element being the column type. Existing elements in the vector
      ///                   will be left in place with column pairs appended to the end of
      ///                   the vector. If this parameter is `nullptr`, then the list of
      ///                   columns is not generated and this parameter's value will
      ///                   remain `nullptr`.
      /// Return:
      ///   `impala::Status` Indicates the result of fetching rows.
      virtual Status FetchAllRows(const TUniqueId& query_id, query_results& results,
          results_columns* columns = nullptr) = 0;

      /// Closes and cleans up the query and its associated session.
      ///
      /// Parameters:
      ///   `query_id` Query that was submitted and has finished.
      virtual void CloseQuery(const TUniqueId& query_id) = 0;

      /// Populates the provided list with all connections currently managed by the
      /// internal server.
      ///
      /// Parameters:
      ///   `connection_contexts` Input/output parameter that will have all internal
      ///                         server connections added to the end.
      virtual void GetConnectionContextList(
        ThriftServer::ConnectionContextList* connection_contexts) = 0;

  }; // InternalServer class

} // namespace impala
