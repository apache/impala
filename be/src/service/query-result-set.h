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

#include <vector>

namespace impala {

/// Stores client-ready query result rows returned by
/// QueryExecState::FetchRows(). Subclasses implement AddRows() / AddOneRow() to
/// specialise how Impala's row batches are converted to client-API result
/// representations.
class QueryResultSet {
 public:
  QueryResultSet() {}
  virtual ~QueryResultSet() {}

  /// Add a single row to this result set. The row is a vector of pointers to values,
  /// whose memory belongs to the caller. 'scales' contains the scales for decimal values
  /// (# of digits after decimal), with -1 indicating no scale specified or the
  /// corresponding value is not a decimal.
  virtual Status AddOneRow(
      const std::vector<void*>& row, const std::vector<int>& scales) = 0;

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
};
}

#endif
