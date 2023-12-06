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

#include "exec/scan-node.h"

namespace impala {

struct QueryStateExpanded;

// SystemTableScanner is the generic interface for different implementations of
// system table scanning.
class SystemTableScanner {
 public:
  static Status CreateScanner(RuntimeState* state, RuntimeProfile* profile,
    TSystemTableName::type table_name, std::unique_ptr<SystemTableScanner>* scanner);

  virtual ~SystemTableScanner() = default;

  /// Start scan, load data needed
  virtual Status Open() = 0;

  /// Fill the next row batch by fetching more data from system table data source.
  virtual Status MaterializeNextTuple(
      MemPool* pool, Tuple* tuple, const TupleDescriptor* tuple_desc_) = 0;

  bool eos() const noexcept { return eos_; }

 protected:
  SystemTableScanner(RuntimeState* state, RuntimeProfile* profile)
      : state_(state), profile_(profile), eos_(false) {}

  /// Write a string value to a STRING slot, allocating memory from 'pool'. Returns
  /// an error if memory cannot be allocated without exceeding a memory limit.
  Status WriteStringSlot(const char* data, int len, MemPool* pool, void* slot);
  Status WriteStringSlot(const std::string& str, MemPool* pool, void* slot);

  RuntimeState* const state_;

  RuntimeProfile* const profile_;

  /// if true, nothing left to return in getNext() in SystemTableScanNode
  bool eos_;
};

class QueryScanner : public SystemTableScanner {
 public:
  QueryScanner(RuntimeState* state, RuntimeProfile* profile);

  /// Start scan, load list of query IDs into active_query_ids_.
  virtual Status Open();

  /// Fill the next row batch by fetching query state from ImpalaServer.
  virtual Status MaterializeNextTuple(
      MemPool* pool, Tuple* tuple, const TupleDescriptor* tuple_desc_);

 private:
  /// Snapshot of query state for queries that are active during Open.
  std::deque<std::shared_ptr<QueryStateExpanded>> query_records_;

  /// Time spent in Open collecting active query state.
  RuntimeProfile::Counter* active_query_collection_timer_;

  /// Time spent in Open collecting completed but not yet written query state.
  RuntimeProfile::Counter* pending_query_collection_timer_;
};

} /* namespace impala */
