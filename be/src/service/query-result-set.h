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

#ifndef IMPALA_SERVICE_QUERY_RESULT_SET_H
#define IMPALA_SERVICE_QUERY_RESULT_SET_H

#include "common/status.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Results_types.h"
#include "gen-cpp/TCLIService_types.h"
#include "runtime/runtime-state.h"
#include "runtime/types.h"

#include <vector>
#include <sstream>

namespace impala {

class RowBatch;
class ScalarExprEvaluator;
class TupleRow;

/// Wraps a client-API specific result representation, and implements the logic required
/// to translate into that format from Impala's row format.
///
/// Subclasses implement AddRows() / AddOneRow() to specialise that logic.
class QueryResultSet {
 public:
  QueryResultSet() {}
  virtual ~QueryResultSet() {}

  /// Add 'num_rows' rows to the result set, obtained by evaluating 'expr_evals' over
  /// the rows in 'batch' starting at start_idx. Batch must contain at least
  /// ('start_idx' + 'num_rows') rows.
  virtual Status AddRows(const std::vector<ScalarExprEvaluator*>& expr_evals,
      RowBatch* batch, int start_idx, int num_rows) = 0;

  /// Add the TResultRow to this result set. When a row comes from a DDL/metadata
  /// operation, the row in the form of TResultRow.
  virtual Status AddOneRow(const TResultRow& row) = 0;

  /// Copies rows in the range [start_idx, start_idx + num_rows) from the other result
  /// set into this result set. Returns the number of rows added to this result set.
  /// Returns 0 if the given range is out of bounds of the other result set.
  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) = 0;

  /// Returns the approximate size of this result set in bytes.
  int64_t ByteSize() { return ByteSize(0, size()); }

  /// Returns the approximate size of the given range of rows in bytes.
  virtual int64_t ByteSize(int start_idx, int num_rows) = 0;

  /// Returns the size of this result set in number of rows.
  virtual size_t size() = 0;

  /// Returns a result set suitable for Beeswax-based clients. If 'stringift_map_keys' is
  /// true, converts map keys to strings; see IMPALA-11778.
  static QueryResultSet* CreateAsciiQueryResultSet(
      const TResultSetMetadata& metadata, std::vector<std::string>* rowset,
      bool stringify_map_keys);

  /// Returns a result set suitable for HS2-based clients. If 'rowset' is nullptr, the
  /// returned object will allocate and manage its own rowset. If 'stringift_map_keys' is
  /// true, converts map keys to strings; see IMPALA-11778.
  static QueryResultSet* CreateHS2ResultSet(
      apache::hive::service::cli::thrift::TProtocolVersion::type version,
      const TResultSetMetadata& metadata,
      apache::hive::service::cli::thrift::TRowSet* rowset, bool stringify_map_keys,
      int expected_result_count);

protected:
  /// Wrapper to call ComplexValueWriter::CollectionValueToJSON() or
  /// ComplexValueWriter::StructValToJSON() for a given complex column. expr_eval must be
  /// a SlotRef on a complex-typed (collection or struct) slot. If 'stringify_map_keys' is
  /// true, converts map keys to strings; see IMPALA-11778.
  static void PrintComplexValue(ScalarExprEvaluator* expr_eval, const TupleRow* row,
      std::stringstream *stream, const ColumnType& type, bool stringify_map_keys);
};
}

#endif
