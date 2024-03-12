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

#include <string>

#include "exec/exec-node.h"
#include "runtime/tuple-cache-mgr.h"

namespace impala {

class TupleFileReader;
class TupleFileWriter;

class TupleCachePlanNode : public PlanNode {
 public:
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~TupleCachePlanNode(){}
};

/// Node that caches rows produced by a child node.
///
/// If the subtree_hash_ matches an existing cache entry, returns result rows from the
/// cache rather than from the child. Otherwise reads results from the child, writes them
/// to cache, and returns them.

class TupleCacheNode : public ExecNode {
 public:
  TupleCacheNode(ObjectPool* pool, const TupleCachePlanNode& pnode,
      const DescriptorTbl& descs);
  ~TupleCacheNode();

  Status Prepare(RuntimeState* state) override;
  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  void Close(RuntimeState* state) override;
  void DebugString(int indentation_level, std::stringstream* out) const override;
private:
  const std::string subtree_hash_;

  /// Number of results that were found in the tuple cache
  RuntimeProfile::Counter* num_hits_counter_ = nullptr;
  /// Number of results that were too large for the cache
  RuntimeProfile::Counter* num_halted_counter_ = nullptr;
  /// Number of results that skip the cache due to a tombstone
  RuntimeProfile::Counter* num_skipped_counter_ = nullptr;

  /// Whether any RowBatch from a cache file has been returned to a caller
  /// It is possible to recover from an error reading a cache file if no
  /// cached RowBatch has been returned to a caller.
  bool cached_rowbatch_returned_to_caller_ = false;

  void ReleaseResult();

  /// Reader/Writer for caching
  TupleCacheMgr::UniqueHandle handle_;
  std::unique_ptr<TupleFileReader> reader_;
  std::unique_ptr<TupleFileWriter> writer_;
};

}
