// Copyright 2013 Cloudera Inc.
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


#ifndef IMPALA_SORTING_SORTER_H_
#define IMPALA_SORTING_SORTER_H_

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "disk-writer.h"
#include "sorted-merger.h"
#include "blocked-vector.h"
#include "exprs/expr.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/thread-resource-mgr.h"
#include "util/key-normalizer.h"

namespace impala {

class Expr;
class RowBatch;
class RowDescriptor;
class Tuple;
class TupleDescripor;
class TupleRow;

// Contains the external interface for the Sorter.
// The internal helpers can be found in sorter-internal.h.
class Sorter {
 public:
  // Construct a sorter to sort an arbitrarily large input of rows into a
  // stream of tuples described by output_tuple_desc.
  // During the sort input phase, incoming rows are copied and flattened into output
  // tuples by populating each slot of the output tuple with the values of
  // the output_slot_exprs (output_tuple_desc.slots()[i] is populated with the
  // value of output_slot_exprs[i]).
  // Tuple comparison is done based on the evaluation of the expressions in
  // sort_exprs_lhs/rhs (which references the output tuple), in order.
  // The bool vector is_asc determines whether a particular comparison is done in
  // ascending or descending order.
  // The nulls_first vector determines, for each expr, if NULL values come before
  // or after all other values.
  // If 'remove_dups' is true, removes duplicates during the merging steps.
  // The Sorter will use no more than 'mem_limit' bytes for its buffer space
  // and no more than 'sort_key_size' bytes for its inlined sort key.
  // 'block_size' determines the size of each block we allocate, and is the unit of
  // merging. (We need 4 blocks per run for tuples with string data, 2 otherwise, as
  // we utilize simple double buffering per run.)
  // We always normalize the sort keys into a single memcmp-able buffer, called
  // the normalized sort key. If 'extract_keys' is true, we will extract out and sort the
  // normalized sort keys on their own, otherwise we will append the normalized key to
  // each tuple. Extracting keys provides significantly better speed when the key is
  // large (wrt the payload) and when we have to go to disk. The main benefit of
  // extracting keys is that we avoid writing the normalized keys out to disk, but this
  // is only costly if the normalized keys themselves are relatively large.
  // 'run_size_prop' governs the size of our initial sorted runs, as a proportion of
  // the given memory limit (approximately, run_size_prop * mem_limit). A value of 0.5
  // is recommended when there is a lot of data (significantly more than our mem_limit)
  // or when parallelization is off. A value of around 0.33 or 0.25 (or less) can provide
  // much better performance when the data fits in memory or nearly so.
  // If 'parallelize_run_building' is on, we will opportunistically spawn a new thread
  // to sort and serialize our runs.
  // TODO: Avoid using lhs/rhs when the exprs don't hold results internally
  Sorter(DiskWriter* writer,
      DiskIoMgr* io_mgr,
      DiskIoMgr::ReaderContext* reader,
      ThreadResourceMgr::ResourcePool* resource_pool,
      const TupleDescriptor& output_tuple_desc,
      const std::vector<Expr*>& output_slot_exprs,
      const std::vector<Expr*>& sort_exprs_lhs,
      const std::vector<Expr*>& sort_exprs_rhs,
      const std::vector<bool>& is_asc,
      const std::vector<bool>& nulls_first,
      bool remove_dups,
      uint32_t sort_key_size,
      uint64_t mem_limit,
      int64_t block_size = 8 * 1024 * 1024,
      bool extract_keys = false,
      float run_size_prop = 0.5f,
      bool parallelize_run_building = true);

  ~Sorter();

  // Add batch to the sorter's stream of input rows. Must not be called after GetNext()
  // has been called.
  Status AddBatch(RowBatch* batch);

  // Remove next batch of output tuples/rows from sorter's stream of sorted tuples.
  Status GetNext(RowBatch* batch, bool* eos);

  // Statistics about the Sorter's execution.
  struct Stats {
    RuntimeProfile::Counter* phase1_tuple_ingest_time;
    RuntimeProfile::Counter* phase1_sort_time;
    RuntimeProfile::Counter* phase1_resort_tuples_time;
    RuntimeProfile::Counter* phase1_prepare_aux_time;
    RuntimeProfile::Counter* phase2_initial_num_runs;
    RuntimeProfile::Counter* phase2_read_blocks_time;
    RuntimeProfile::Counter* phase2_supplier_get_next_time;
    RuntimeProfile::Counter* phase2_merge_time;
    RuntimeProfile::Counter* phase2_merge_output_time;
    RuntimeProfile::Counter* phase2_num_runs_merged;
//    RuntimeProfile::Counter* bytes_written; // TODO: Resurrect
    RuntimeProfile::Counter* bytes_read;
    RuntimeProfile::Counter* total_bytes_ingested;
    RuntimeProfile::Counter* bytes_ingested_tuples;
    RuntimeProfile::Counter* bytes_ingested_extracted_keys;
    RuntimeProfile::Counter* bytes_ingested_aux;
    RuntimeProfile::Counter* scan_range_blocks_read;
    RuntimeProfile::Counter* scan_range_buffer_misses;

    Stats(RuntimeProfile* profile) {
      phase1_tuple_ingest_time =
          ADD_TIMER(profile, "Phase 1 - Tuple Ingest Time");
      phase1_sort_time =
          ADD_TIMER(profile, "Phase 1 - Sort Time");
      phase1_resort_tuples_time =
          ADD_TIMER(profile, "Phase 1 - Re-sort Tuples");
      phase1_prepare_aux_time =
          ADD_TIMER(profile, "Phase 1 - Aux Preparation");
      phase2_initial_num_runs =
          ADD_COUNTER(profile, "Phase 2 - Initial Num Runs", TCounterType::UNIT);
      phase2_read_blocks_time =
          ADD_TIMER(profile, "Phase 2 - Read Blocks");
      phase2_supplier_get_next_time =
          ADD_TIMER(profile, "Phase 2 - RBS::GetNext()");
      phase2_merge_time =
          ADD_TIMER(profile, "Phase 2 - Merge");
      phase2_merge_output_time =
          ADD_TIMER(profile, "Phase 2 - Merge Output");
      phase2_num_runs_merged =
          ADD_COUNTER(profile, "Phase 2 - Runs Merged", TCounterType::UNIT);
      bytes_read =
          ADD_COUNTER(profile, "Bytes Read", TCounterType::BYTES);
      total_bytes_ingested =
          ADD_COUNTER(profile, "Bytes Ingested", TCounterType::BYTES);
      bytes_ingested_tuples =
          ADD_COUNTER(profile, "Bytes Ingested Tuples", TCounterType::BYTES);
      bytes_ingested_extracted_keys =
          ADD_COUNTER(profile, "Bytes Ingested Extracted", TCounterType::BYTES);
      bytes_ingested_aux =
          ADD_COUNTER(profile, "Bytes Ingested Aux", TCounterType::BYTES);
      scan_range_blocks_read =
          ADD_COUNTER(profile, "Scan Range Blocks Read", TCounterType::UNIT);
      scan_range_buffer_misses =
          ADD_COUNTER(profile, "Scan Range Blocks Missed", TCounterType::UNIT);
    }
  };

  const Sorter::Stats& stats() const { return stats_; }

  const RuntimeProfile* profile() const { return profile_.get(); }

 private:
  class SortTuple;
  class SortTupleComparator;
  struct Run;
  class RunBuilder;
  class RunBatchSupplier;
  class BufferDiskManager;

  // Copies the auxiliary (string) slot from the tuple into consecutive memory
  // in the given pool. We will enqueue Blocks as they are filled in the pool.
  // If we have to enqueue a Block and block_priority is not NULL, then block_priority
  // is used to assign a priority to that Block on the disk queue, and block_priority
  // is then incremented.
  void CopyAuxString(Tuple* tuple, SlotDescriptor* slot, BlockMemPool* aux_pool,
      int* block_priority = NULL);

  // Creates a sorted run and adds it to the runs_ vector.
  void CreateSortedRun(RunBuilder* run_builder, bool release_thread_token);

  // Sorts the given run in a new thread (if available and parallelize_run_builders_ is
  // true), or else does it in the original thread.
  boost::thread* SortRunInThread(RunBuilder* run_builder);

  // Merges sorted runs together until there are few enough for the final merge step,
  // and prepares merger_ with the final runs.
  void MultilevelMerge();

  MemTracker* mem_tracker() { return mem_tracker_.get(); }

  TupleDescriptor output_tuple_desc_;
  boost::scoped_ptr<RowDescriptor> output_row_desc_;
  const std::vector<Expr*> output_slot_exprs_;
  const std::vector<Expr*> sort_exprs_lhs_;
  const std::vector<Expr*> sort_exprs_rhs_;
  const std::vector<bool> is_asc_;
  const std::vector<bool> nulls_first_;
  const bool remove_dups_;
  const bool extract_keys_;
  const float run_size_prop_;
  const bool parallelize_run_building_;

  // The number of bytes of our normalized sort key.
  uint32_t sort_key_size_;

  boost::scoped_ptr<KeyNormalizer> key_normalizer_;

  // Limit for how much memory we can use.
  // Technically, this only influences the size of the buffer_pool_, so we can and will
  // allocate somewhat more memory than this, but it is largely a constant amount.
  // (For instance, we allocate Blocks, ReservationContexts, and other housekeeping
  // objects.)
  boost::scoped_ptr<MemTracker> mem_tracker_;

  // The current list of sorted runs (sorted tuple and auxiliary data).
  std::deque<Run*> runs_;

  // Used to queue up buffers to be sent to disk as memory becomes constrained.
  boost::scoped_ptr<DiskWriter::BufferManager> disk_manager_;

  // Pool which contains a fixed number of buffers used throughout the Sorter,
  // BlockedVectors, and StreamReaders. This ensures that we maintain our memory limit.
  boost::scoped_ptr<BufferPool> buffer_pool_;

  DiskIoMgr* io_mgr_;

  DiskIoMgr::ReaderContext* reader_;

  boost::scoped_ptr<SortedMerger> final_merger_;

  // The current RunBuilder, containing a set of unsorted, normalized tuples.
  RunBuilder* run_builder_;

  // Batch used during intermediary merge steps.
  boost::scoped_ptr<RowBatch> intermediate_batch_;

  // If we have only one run, this batch supplier is used to return rows without
  // going through the merger.
  boost::scoped_ptr<RunBatchSupplier> single_run_batch_supplier_;

  // Used to determine if this is the first time GetNext() has been called.
  bool started_merging_;

  // True if there are string slots in the input tuples.
  bool has_aux_data_;

  // Number of bytes for each output tuple, simply equal to output_tuple_desc.byte_size().
  size_t tuple_size_;

  // Resource pool for opportunistically constructing RunBuilder threads.
  ThreadResourceMgr::ResourcePool* resource_pool_;

  // All RunBuilder threads started in parallel, to be joined on at the end.
  boost::thread_group run_builder_threads_;

  // Lock for runs_, used when runs are built in parallel.
  boost::mutex runs_lock_;

  // Number of run builders currently executing in parallel.
  // Since each RunBuilder requires 1 reserved auxiliary buffer to make progress,
  // this value is conveniently equal to the number of reserved buffers.
  AtomicInt<int> num_inflight_run_builders_;

  // Used by the RuntimeProfile and for allocation of Blocks.
  boost::scoped_ptr<ObjectPool> obj_pool_;

  boost::scoped_ptr<RuntimeProfile> profile_;

  // Contains all RuntimeProfile counters.
  Stats stats_;
};

}

#endif
