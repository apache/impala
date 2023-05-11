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
class TupleTextFileWriter;

class TupleCachePlanNode : public PlanNode {
 public:
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~TupleCachePlanNode(){}
};

/// Node that caches rows produced by a child node.
///
/// If the combined_key_ matches an existing cache entry, returns result rows from the
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
  // Fragment instance cache key. This is calculated at runtime by combining information
  // about the input nodes for this fragment. It currently focuses on hashing the
  // scan ranges from scan nodes. In future, it will need to handle exchanges.
  uint32_t fragment_instance_key_;

  // This is a string containing the compile time key and the fragment_instance_key_.
  // This combination is unique for a given fragment instance.
  std::string combined_key_;

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

  // Construct the fragment instance part of the cache key by hashing information about
  // inputs to this fragment (e.g. scan ranges).
  void ComputeFragmentInstanceKey(const RuntimeState *state);

  /// Reader/Writer for caching
  TupleCacheMgr::UniqueHandle handle_;
  std::unique_ptr<TupleFileReader> reader_;
  std::unique_ptr<TupleFileWriter> writer_;
  std::unique_ptr<TupleTextFileWriter> debug_dump_text_writer_;
  std::unique_ptr<TupleTextFileWriter> debug_dump_text_writer_ref_;

  /// Helper function to generate the path for debug dumping the tuple cache.
  /// If sub_dir_full_path is not nullptr, the subdirectory path will be returned,
  /// allowing the caller to create the subdirectory if necessary.
  string GetDebugDumpPath(const RuntimeState* state, const string& fragment_id,
      string* sub_dir_full_path = nullptr) const;

  /// Helper function to verify the correctness of the debug tuple cache.
  Status VerifyAndMoveDebugCache(RuntimeState* state);
};

}
