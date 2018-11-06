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

#include <gtest/gtest.h>
#include <kudu/client/client.h>

#include "exec/scan-node.h"
#include "runtime/descriptors.h"

namespace impala {

class KuduScanner;

/// Base class for the two Kudu scan node implementations. Contains the code that is
/// independent of whether the rows are materialized by scanner threads (KuduScanNode)
/// or by the thread calling GetNext (KuduScanNodeMt). This class is not thread safe
/// for concurrent access. Subclasses are responsible for implementing thread safety.
/// TODO: This class can be removed when the old single threaded implementation is
/// removed.
class KuduScanNodeBase : public ScanNode {
 public:
  KuduScanNodeBase(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
  ~KuduScanNodeBase();

  virtual Status Prepare(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos)
      override = 0;
 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const override;

  /// Returns the total number of scan tokens
  int NumScanTokens() { return scan_tokens_.size(); }

  /// Returns whether there are any scan tokens remaining. Not thread safe.
  bool HasScanToken();

  /// Returns the next scan token. Returns NULL if there are no more scan tokens.
  /// Not thread safe, access must be synchronized.
  const std::string* GetNextScanToken();

  const TupleDescriptor* tuple_desc() const { return tuple_desc_; }

 private:
  friend class KuduScanner;

  /// Tuple id resolved in Prepare() to set tuple_desc_.
  const TupleId tuple_id_;

  /// Descriptor of tuples read from Kudu table.
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// Pointer to the KuduClient, which is stored on the QueryState and shared between
  /// scanners and fragment instances.
  kudu::client::KuduClient* client_ = nullptr;

  /// Kudu table reference. Shared between scanner threads for KuduScanNode.
  kudu::client::sp::shared_ptr<kudu::client::KuduTable> table_;

  /// Set of scan tokens to be deserialized into Kudu scanners.
  std::vector<std::string> scan_tokens_;

  /// The next index in 'scan_tokens_' to be assigned.
  int next_scan_token_idx_ = 0;

  RuntimeProfile::Counter* kudu_round_trips_ = nullptr;
  RuntimeProfile::Counter* kudu_remote_tokens_ = nullptr;
  RuntimeProfile::Counter* kudu_client_time_ = nullptr;
  static const std::string KUDU_ROUND_TRIPS;
  static const std::string KUDU_REMOTE_TOKENS;
  static const std::string KUDU_CLIENT_TIME;

  kudu::client::KuduClient* kudu_client() { return client_; }
  RuntimeProfile::Counter* kudu_round_trips() const { return kudu_round_trips_; }
  RuntimeProfile::Counter* kudu_client_time() const { return kudu_client_time_; }
};
} // namespace impala
