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

#include "sorter.h"
#include "sorter-internal.h"

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "stream-reader.h"
#include "row-batch-supplier.h"
#include "sort-util.h"
#include "common/atomic.h"
#include "exprs/expr.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "util/key-normalizer.inline.h"
#include "util/runtime-profile.h"
#include "common/object-pool.h"
#include "util/tuple-row-compare.h"

using namespace boost;
using namespace std;

namespace impala {

Sorter::Sorter(
    DiskWriter* writer,
    DiskIoMgr* io_mgr,
    DiskIoMgr::ReaderContext* reader,
    ThreadResourceMgr::ResourcePool* resource_pool,
    const TupleDescriptor& output_tuple_desc,
    const vector<Expr*>& output_slot_exprs,
    const vector<Expr*>& sort_exprs_lhs,
    const vector<Expr*>& sort_exprs_rhs,
    const vector<bool>& is_asc,
    const vector<bool>& nulls_first,
    bool remove_dups, uint32_t sort_key_size, uint64_t mem_limit,
    int64_t block_size, bool extract_keys, float run_size_prop,
    bool parallelize_run_building)
    : output_tuple_desc_(output_tuple_desc),
      output_slot_exprs_(output_slot_exprs),
      sort_exprs_lhs_(sort_exprs_lhs),
      sort_exprs_rhs_(sort_exprs_rhs),
      is_asc_(is_asc),
      nulls_first_(nulls_first),
      remove_dups_(remove_dups),
      extract_keys_(extract_keys),
      run_size_prop_(run_size_prop),
      parallelize_run_building_(parallelize_run_building),
      sort_key_size_(sort_key_size),
      io_mgr_(io_mgr),
      reader_(reader),
      started_merging_(false),
      tuple_size_(output_tuple_desc.byte_size()),
      resource_pool_(resource_pool),
      num_inflight_run_builders_(0),
      obj_pool_(new ObjectPool()),
      profile_(new RuntimeProfile(obj_pool_.get(), "sorter")),
      stats_(profile_.get()) {

  DCHECK_EQ(sort_exprs_lhs.size(), sort_exprs_rhs.size());
  DCHECK_EQ(sort_exprs_lhs.size(), is_asc.size());

  has_aux_data_ = (output_tuple_desc_.string_slots().size() > 0);

  // In order to sort any amount of data, we need at least 3 runs worth of blocks,
  // 2 input and 1 output. Each run is double buffered and may need an extra pair of
  // buffers if we have string data. This logic is reflected in MultilevelMerge().
  int num_buffers_per_run = (has_aux_data_ ? 4 : 2);
  DCHECK_GE(mem_limit, 3 * num_buffers_per_run * block_size);

  mem_tracker_.reset(new MemTracker(mem_limit));

  buffer_pool_.reset(new BufferPool(mem_limit/block_size, block_size));
  disk_manager_.reset(new DiskWriter::BufferManager(buffer_pool_.get(), writer));
  key_normalizer_.reset(
      new KeyNormalizer(sort_exprs_lhs_, sort_key_size_, is_asc_, nulls_first_));

  run_builder_ = new RunBuilder(this, buffer_pool_.get());

  vector<TupleDescriptor*> tuple_descs;
  tuple_descs.push_back(&output_tuple_desc_);
  vector<bool> nullable_tuples(1, false);
  output_row_desc_.reset(new RowDescriptor(tuple_descs, nullable_tuples));
  final_merger_.reset(new SortedMerger(*output_row_desc_.get(), sort_exprs_lhs,
                                 sort_exprs_rhs, is_asc, nulls_first, remove_dups,
                                 mem_limit));
}

Sorter::~Sorter() {
}

// RunBuilder
Sorter::RunBuilder::RunBuilder(Sorter* sorter, BufferPool* buffer_pool)
    : sorter_(sorter),
      tuple_pool_(new BlockMemPool(sorter_->obj_pool_.get(), buffer_pool)),
      aux_pool_(new BlockMemPool(sorter_->obj_pool_.get(), buffer_pool)),
      first_sort_expr_over_budget_(sorter_->sort_exprs_lhs_.size()) {
  unsorted_aux_pool_.reset(new MemPool(sorter->mem_tracker()));

  if (sorter_->extract_keys_) {
    tuples_ = BlockedVector<Tuple>(tuple_pool_.get(), sorter_->tuple_size_);
    sort_tuple_pool_.reset(new BlockMemPool(sorter_->obj_pool_.get(), buffer_pool));
    sort_tuples_ = BlockedVector<SortTuple>(sort_tuple_pool_.get(),
        sizeof(SortTuple) + sorter_->sort_key_size_);
  } else {
    tuples_ = BlockedVector<Tuple>(tuple_pool_.get(),
        sorter_->sort_key_size_ + sorter_->tuple_size_);
  }
}

Sorter::RunBuilder::~RunBuilder() {
  unsorted_aux_pool_->FreeAll();
}

void Sorter::RunBuilder::AddRow(TupleRow* row) {
  Tuple* tuple = tuples_.AllocateElement();

  memset(tuple, 0, sorter_->output_tuple_desc_.num_null_bytes());
  TupleRow* output_row = reinterpret_cast<TupleRow*>(&tuple);

  // Evaluate the output_slot_exprs and place the results in the sort tuples.
  for (int j = 0; j < sorter_->output_slot_exprs_.size(); ++j) {
    SlotDescriptor* slot_desc = sorter_->output_tuple_desc_.slots()[j];
    void* src = sorter_->output_slot_exprs_[j]->GetValue(row);
    if (src != NULL) {
      void* dst = tuple->GetSlot(slot_desc->tuple_offset());
      RawValue::Write(src, dst, slot_desc->type(), unsorted_aux_pool_.get());
    } else {
      tuple->SetNull(slot_desc->null_indicator_offset());
    }
  }

  uint8_t* sort_key_dst;
  SortTuple* sort_tuple;
  if (sorter_->extract_keys_) {
    sort_tuple = sort_tuples_.AllocateElement();
    sort_tuple->tuple_idx_ = tuples_.size() - 1;
    sort_key_dst = sort_tuple->SortKey();
  } else {
    sort_key_dst = reinterpret_cast<uint8_t*>(tuple->GetSlot(sorter_->tuple_size_));
  }

  // Construct the normalized sort key.
  int key_idx_over_budget;
  bool over_budget = sorter_->key_normalizer_->NormalizeKey(
      output_row, sort_key_dst, &key_idx_over_budget);
  if (over_budget) {
    first_sort_expr_over_budget_ = min(key_idx_over_budget, first_sort_expr_over_budget_);
  }
}

void Sorter::RunBuilder::DoSort(vector<Expr*>* incomplete_sort_exprs_lhs,
                                vector<Expr*>* incomplete_sort_exprs_rhs,
                                vector<bool>* is_asc,
                                vector<bool>* nulls_first) {
  if (sorter_->extract_keys_) {
    int sort_tuple_size = sorter_->sort_key_size_ + sizeof(SortTuple);

    if (incomplete_sort_exprs_lhs == NULL) {
      SortUtil<SortTuple>::SortNormalized(sort_tuples_.Begin(), sort_tuples_.End(),
          sort_tuple_size, sizeof(SortTuple), sorter_->sort_key_size_);
    } else {
      function<bool (SortTuple*, SortTuple*)> sort_key_cmp = SortTupleComparator(&tuples_,
          *incomplete_sort_exprs_lhs, *incomplete_sort_exprs_rhs, *is_asc,
          *nulls_first);

      SortUtil<SortTuple>::SortNormalized(sort_tuples_.Begin(), sort_tuples_.End(),
          sort_tuple_size, sizeof(SortTuple), sorter_->sort_key_size_, &sort_key_cmp);
    }
  } else {
    size_t sort_tuple_size = sorter_->tuple_size_ + sorter_->sort_key_size_;

    if (incomplete_sort_exprs_lhs == NULL) {
      SortUtil<Tuple>::SortNormalized(tuples_.Begin(), tuples_.End(),
          sort_tuple_size, sorter_->tuple_size_, sorter_->sort_key_size_);
    } else {
      function<bool (Tuple*, Tuple*)> sort_key_cmp = TupleRowComparator(
          *incomplete_sort_exprs_lhs, *incomplete_sort_exprs_rhs, *is_asc,
          *nulls_first);

      SortUtil<Tuple>::SortNormalized(tuples_.Begin(), tuples_.End(),
          sort_tuple_size, sorter_->tuple_size_, sorter_->sort_key_size_, &sort_key_cmp);
    }
  }
}

void Sorter::RunBuilder::Sort() {
  SCOPED_TIMER(sorter_->stats().phase1_sort_time);

  bool complete_key = (first_sort_expr_over_budget_ == sorter_->sort_exprs_lhs_.size());
  if (complete_key) {
    DoSort(NULL, NULL, NULL, NULL);
  } else {
    // Use comparator which falls back on comparing evaluated sort exprs if keys equal.
    // Be sure to only compare sort exprs that are not fully in the normalized key.
    vector<Expr*> incomplete_sort_exprs_lhs(
        sorter_->sort_exprs_lhs_.begin() + first_sort_expr_over_budget_,
        sorter_->sort_exprs_lhs_.end());
    vector<Expr*> incomplete_sort_exprs_rhs(
        sorter_->sort_exprs_rhs_.begin() + first_sort_expr_over_budget_,
        sorter_->sort_exprs_rhs_.end());
    vector<bool> is_asc(
        sorter_->is_asc_.begin() + first_sort_expr_over_budget_, sorter_->is_asc_.end());
    vector<bool> nulls_first(
        sorter_->nulls_first_.begin() + first_sort_expr_over_budget_,
        sorter_->nulls_first_.end());
    DoSort(&incomplete_sort_exprs_lhs, &incomplete_sort_exprs_rhs, &is_asc, &nulls_first);
  }
}

// Swaps two elements within a BlockedVector.
template <class T>
static void MemSwap(BlockedVector<T> bv, int idx1, int idx2, void* swap_buffer) {
  memcpy(swap_buffer, bv[idx1], bv.element_size());
  memcpy(bv[idx1], bv[idx2], bv.element_size());
  memcpy(bv[idx2], swap_buffer, bv.element_size());
}

static void UpdateAllocationStats(Sorter* sorter,
    uint64_t tuple_bytes, uint64_t aux_bytes, uint64_t sort_tuple_bytes) {
  COUNTER_UPDATE(sorter->stats().bytes_ingested_tuples, tuple_bytes);
  COUNTER_UPDATE(sorter->stats().bytes_ingested_aux, aux_bytes);
  COUNTER_UPDATE(sorter->stats().bytes_ingested_extracted_keys, sort_tuple_bytes);
  COUNTER_UPDATE(sorter->stats().total_bytes_ingested,
                 tuple_bytes + aux_bytes + sort_tuple_bytes);
}

Sorter::Run* Sorter::RunBuilder::BuildSortedRun() {
  LOG(INFO) << "Compacting run with " << tuples_.size() << " tuples "
      << "(" << tuples_.bytes_allocated() << " bytes)";

  UpdateAllocationStats(sorter_,
      tuple_pool_->bytes_allocated(),
      unsorted_aux_pool_->total_allocated_bytes(),
      (sorter_->extract_keys_ ? sort_tuple_pool_->bytes_allocated() : 0));

  Sort();

  uint8_t swap_buffer[sorter_->tuple_size_];

  // If we extracted the sort keys, we have to sort the tuples according to
  // the order of the keys.
  if (sorter_->extract_keys_) {
    SCOPED_TIMER(sorter_->stats().phase1_resort_tuples_time);
    BlockedVector<SortTuple>::Iterator it = sort_tuples_.Begin();
    BlockedVector<SortTuple>::Iterator end = sort_tuples_.End();
    // This is the Robinson In-Place Tuple Sort algorithm. See: henry@cloudera.com.
    for (int i = 0; it != end; ++it, ++i) {
      SortTuple* sort_tuple = *it;
      int tuple_idx = sort_tuple->tuple_idx_;
      int sort_tuple_idx = i;

      // TODO: don't set to -1, set to identity
      if (tuple_idx == sort_tuple_idx) continue;
      if (tuple_idx == -1) continue;
      while (sort_tuples_[tuple_idx]->tuple_idx_ != -1) {
        sort_tuple->tuple_idx_ = -1;
        MemSwap(tuples_, tuple_idx, sort_tuple_idx, swap_buffer);

        sort_tuple_idx = tuple_idx;
        sort_tuple = sort_tuples_[sort_tuple_idx];
        tuple_idx = sort_tuple->tuple_idx_;
      }
    }

    const vector<Block*>& sort_tuple_blocks = sort_tuples_.pool()->blocks();
    for (int i = 0; i < sort_tuple_blocks.size(); ++i) {
      bool released = sort_tuple_blocks[i]->ReleaseBufferIfUnpinned();
      DCHECK(released);
    }
  }

  // Copy over all aux data into a set of sorted blocks.
  // We copy the Aux data backwards so that we can immediately release the buffers
  // as we go, and end up with the front buffer still in memory.
  // TODO: Can delay this until we know we're memory-constrained, avoiding doing it at all
  // if everything fits in memory (and also avoiding subsequently changing the string
  // offsets back to pointers on read).
  // This can save up to 10% of the total sorter time for fully in-mem workloads.
  const vector<Block*>& tuple_blocks = tuples_.pool()->blocks();
  int block_priority = 0;
  if (sorter_->has_aux_data_) {
    SCOPED_TIMER(sorter_->stats().phase1_prepare_aux_time);
    // NB: Backwards!
    BlockedVector<Tuple>::Iterator it = tuples_.End();
    BlockedVector<Tuple>::Iterator begin = tuples_.Begin();
    uint64_t last_block_index = it.block_index();
    while (it != begin) {
      --it;
      if (it.block_index() != last_block_index) {
        sorter_->disk_manager_->EnqueueBlock(tuple_blocks[last_block_index],
            ++block_priority);
        last_block_index = it.block_index();
      }

      Tuple* tuple = *it;
      // NB: Also backwards!
      const vector<SlotDescriptor*>& string_slots =
          sorter_->output_tuple_desc_.string_slots();
      for (int i = string_slots.size() - 1; i >= 0; --i) {
        sorter_->CopyAuxString(tuple, string_slots[i], aux_pool_.get(), &block_priority);
      }
    }

    if (!tuple_blocks.empty()) {
      sorter_->disk_manager_->EnqueueBlock(tuple_blocks[it.block_index()],
          ++block_priority);
    }

    if (aux_pool_->num_blocks() > 0) {
      sorter_->disk_manager_->EnqueueBlock(aux_pool_->blocks().back(), ++block_priority);
    }
  } else {
    int block_priority = 0;
    for (int i = tuple_blocks.size() - 1; i >= 0; --i) {
      sorter_->disk_manager_->EnqueueBlock(tuple_blocks[i], ++block_priority);
    }
  }

  size_t tuple_size = sorter_->tuple_size_ +
      (sorter_->extract_keys_ ? 0 : sorter_->sort_key_size_);
  return new Run(tuple_blocks, aux_pool_->blocks(), tuple_size, true);
}

// RunBatchSupplier
Sorter::RunBatchSupplier::RunBatchSupplier(Sorter* sorter, BufferPool* buffer_pool,
    Run* run)
    : sorter_(sorter),
      buffer_pool_(buffer_pool),
      run_(run),
      tuple_size_(run->tuple_size_),
      cur_tuple_block_(NULL),
      cur_aux_block_(NULL) {
  tuple_blocks_iterator_ = run->tuple_blocks_.begin();
}

Sorter::RunBatchSupplier::~RunBatchSupplier() {
  delete run_;
}

void Sorter::RunBatchSupplier::Prepare() {
  tuple_reader_.reset(MakeStreamReader(run_->tuple_blocks_, false));
  tuple_reader_->Prepare();

  aux_reader_.reset(MakeStreamReader(run_->aux_blocks_, run_->aux_backwards_));
  aux_reader_->Prepare();
}

StreamReader* Sorter::RunBatchSupplier::MakeStreamReader(
    const vector<Block*>& blocks, bool backwards) {
  vector<Block*> on_disk_blocks;
  if (!backwards) {
    for (int i = 0; i < blocks.size(); ++i)  {
      if (!blocks[i]->in_mem()) on_disk_blocks.push_back(blocks[i]);
    }
  } else {
    for (int i = blocks.size() - 1; i >= 0; --i)  {
      if (!blocks[i]->in_mem()) on_disk_blocks.push_back(blocks[i]);
    }
  }

  return new StreamReader(sorter_->io_mgr_, sorter_->reader_, buffer_pool_,
      on_disk_blocks, sorter_->stats().scan_range_blocks_read,
      sorter_->stats().scan_range_buffer_misses);
}

void Sorter::RunBatchSupplier::AcquireBuffer(Block* block, StreamReader* reader) {
  block->Pin();
  if (block->in_mem()) return;

  {
    SCOPED_TIMER(sorter_->stats().phase2_read_blocks_time);
    reader->ReadBlock(block);
  }
  COUNTER_UPDATE(sorter_->stats().bytes_read, block->len());
  block->DoNotPersist(); // we will never need this block persisted (again)
}

void Sorter::RunBatchSupplier::ReleaseBuffer(Block* block) {
  block->Unpin();
  block->ReleaseBufferIfUnpinned();
}

Status Sorter::RunBatchSupplier::GetNext(RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(sorter_->stats().phase2_supplier_get_next_time);
  if (cur_tuple_block_ == NULL) {
    cur_tuple_block_ = *tuple_blocks_iterator_;
    cur_tuple_index_ = 0;
    AcquireBuffer(cur_tuple_block_, tuple_reader_.get());
  }

  const vector<SlotDescriptor*>& string_slots =
      sorter_->output_tuple_desc_.string_slots();

  while (!row_batch->IsFull() && tuple_blocks_iterator_ != run_->tuple_blocks_.end()) {
    DCHECK(cur_tuple_block_->in_mem());
    Tuple* tuple = reinterpret_cast<Tuple*>(
        cur_tuple_block_->buf_desc()->buffer + cur_tuple_index_ * tuple_size_);

    ++cur_tuple_index_;

    for (int j = 0; j < string_slots.size(); ++j) {
      SlotDescriptor* slot_desc = string_slots[j];
      if (tuple->IsNull(slot_desc->null_indicator_offset())) continue;

      // Note: ptr is currently an offset.
      StringValue* value = reinterpret_cast<StringValue*>(
        tuple->GetSlot(slot_desc->tuple_offset()));

      int64_t offset = reinterpret_cast<int64_t>(value->ptr);
      int aux_index  = offset / buffer_pool_->buffer_size();
      int aux_offset = offset % buffer_pool_->buffer_size();
      DCHECK_LT(aux_index, run_->aux_blocks_.size());

      Block* aux_block = run_->aux_blocks_[aux_index];

      if (aux_block != cur_aux_block_) {
        if (cur_aux_block_ != NULL) ReleaseBuffer(cur_aux_block_);
        cur_aux_block_ = aux_block;
        AcquireBuffer(cur_aux_block_, aux_reader_.get());
      }

      // TODO: Could only turn ptr into offset on non-mem resident things,
      // then only do this logic once per block if it's in mem (rather than per tuple)
      value->ptr = reinterpret_cast<char*>(aux_block->buf_desc()->buffer + aux_offset);
      DCHECK_EQ(buffer_pool_->buffer_size(), aux_block->buf_desc()->size);
    }

    // TODO: Can avoid DeepCopying if we know all our data fits in memory.
    // This complicates the who-owns-my-memory semantics though.
    int row_idx = row_batch->AddRow();
    TupleRow* output_row = row_batch->GetRow(row_idx);
    TupleRow* input_row = reinterpret_cast<TupleRow*>(&tuple);
    input_row->DeepCopy(output_row, sorter_->output_row_desc_->tuple_descriptors(),
                        row_batch->tuple_data_pool(), false);
    row_batch->CommitLastRow();
    // Move on to the next tuple block as necessary, releasing buffers from this one.
    if (cur_tuple_index_ == cur_tuple_block_->len() / tuple_size_) {
      ReleaseBuffer(cur_tuple_block_);

      cur_tuple_index_ = 0;
      ++tuple_blocks_iterator_;
      if (tuple_blocks_iterator_ == run_->tuple_blocks_.end()) {
        cur_tuple_block_ = NULL;
        break;
      }

      cur_tuple_block_ = *tuple_blocks_iterator_;
      AcquireBuffer(cur_tuple_block_, tuple_reader_.get());
    }
    DCHECK_NE(cur_tuple_index_, cur_tuple_block_->len() / tuple_size_);
  }

  *eos = (tuple_blocks_iterator_ == run_->tuple_blocks_.end());
  if (*eos && cur_tuple_block_ != NULL) ReleaseBuffer(cur_tuple_block_);
  if (*eos && cur_aux_block_ != NULL) ReleaseBuffer(cur_aux_block_);

  return Status::OK;
}

// Sorter
void Sorter::CreateSortedRun(RunBuilder* run_builder, bool release_thread_token) {
  Run* run = run_builder->BuildSortedRun();
  {
    lock_guard<mutex> lock(runs_lock_);
    runs_.push_back(run);
  }
  delete run_builder;
  --num_inflight_run_builders_;
  if (release_thread_token) resource_pool_->ReleaseThreadToken(false);
}

thread* Sorter::SortRunInThread(RunBuilder* run_builder) {
  ++num_inflight_run_builders_;
  if (parallelize_run_building_ && resource_pool_->TryAcquireThreadToken()) {
    run_builder_->ReserveAuxBuffer();
    function<void ()> f(bind(&Sorter::CreateSortedRun, this, run_builder_, true));
    return new thread(f);
  }
  CreateSortedRun(run_builder, false);
  return NULL;
}

Status Sorter::AddBatch(RowBatch* batch) {
  MonotonicStopWatch tuple_ingest_time;
  tuple_ingest_time.Start();

  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    run_builder_->AddRow(row);

    int target_run_buffers =
        run_size_prop_ * (buffer_pool_->num_buffers() - num_inflight_run_builders_);
    if (run_builder_->buffers_needed() > target_run_buffers) {
      tuple_ingest_time.Stop();
      thread* t = SortRunInThread(run_builder_);
      if (t != NULL) run_builder_threads_.add_thread(t);
      run_builder_ = new RunBuilder(this, buffer_pool_.get());
      tuple_ingest_time.Start();
    }
  }

  COUNTER_UPDATE(stats_.phase1_tuple_ingest_time, tuple_ingest_time.ElapsedTime());
  return Status::OK;
}

void Sorter::CopyAuxString(Tuple* tuple, SlotDescriptor* slot, BlockMemPool* aux_pool,
    int* block_priority) {
  if (tuple->IsNull(slot->null_indicator_offset())) return;

  StringValue* value = reinterpret_cast<StringValue*>(
    tuple->GetSlot(slot->tuple_offset()));

  if (aux_pool->WouldExpand(value->len) && aux_pool->num_blocks() > 0) {
    disk_manager_->EnqueueBlock(aux_pool->blocks().back(),
                                block_priority == NULL ? 0 : ++*block_priority);
  }

  uint8_t* cur_ptr = aux_pool->Allocate(value->len);
  memcpy(cur_ptr, value->ptr, value->len);

  // Change the pointer to an offset.
  int num_blocks = aux_pool->num_blocks();
  value->ptr = reinterpret_cast<char*>(
      cur_ptr - aux_pool->BlockBegin(num_blocks - 1)
      + (num_blocks - 1) * buffer_pool_->buffer_size());
}

void Sorter::MultilevelMerge() {
  COUNTER_SET(stats_.phase2_initial_num_runs, (int64_t) runs_.size());
  intermediate_batch_.reset(new RowBatch(*output_row_desc_.get(), 256, mem_tracker()));

  // Require double buffering per run.
  int num_buffers_per_run = (has_aux_data_ ? 4 : 2);
  // Require double buffering for the output run.
  int num_buffers_out = num_buffers_per_run;

  // Inner (i.e., non-final) merges must maintain some output buffers.
  int runs_merged_inner =
      (buffer_pool_->num_buffers() - num_buffers_out) / num_buffers_per_run;
  // Final merge can use all available buffers.
  int runs_merged_last = (buffer_pool_->num_buffers()) / num_buffers_per_run;
  DCHECK_LE(runs_merged_inner, runs_merged_last);

  while (runs_.size() > runs_merged_last) {
    COUNTER_UPDATE(stats_.phase2_num_runs_merged, runs_merged_inner);
    // Merge each set of 'runs_merged_inner' runs into one.
    SortedMerger merger(*output_row_desc_.get(), sort_exprs_lhs_,
                        sort_exprs_rhs_, is_asc_, nulls_first_, remove_dups_,
                        mem_tracker()->limit());

    for (int i = 0; i < runs_merged_inner; ++i) {
      RunBatchSupplier* supplier =
          new RunBatchSupplier(this, buffer_pool_.get(), runs_.back());
      supplier->Prepare();
      merger.AddRun(supplier);
      // Take from back to get most recent Runs, more likely to be in memory.
      runs_.pop_back();
    }

    BlockMemPool tuple_pool(obj_pool_.get(), buffer_pool_.get());
    BlockMemPool aux_pool(obj_pool_.get(), buffer_pool_.get());

    bool eos = false;
    while (!eos) {
      {
        SCOPED_TIMER(stats().phase2_merge_time);
        intermediate_batch_->Reset();
        merger.GetNext(intermediate_batch_.get(), &eos);
      }

      {
        SCOPED_TIMER(stats().phase2_merge_output_time);
        for (int k = 0; k < intermediate_batch_->num_rows(); ++k) {
          Tuple* tuple = intermediate_batch_->GetRow(k)->GetTuple(0);

          const vector<SlotDescriptor*>& str_slots = output_tuple_desc_.string_slots();
          for (int i = 0; i < str_slots.size(); ++i) {
            CopyAuxString(tuple, str_slots[i], &aux_pool);
          }

          if (tuple_pool.num_blocks() > 0 && tuple_pool.WouldExpand(tuple_size_)) {
            disk_manager_->EnqueueBlock(tuple_pool.blocks().back(), 0);
          }
          tuple_pool.Insert(tuple, tuple_size_);
        }
      }
    }

    if (tuple_pool.num_blocks() > 0) {
      disk_manager_->EnqueueBlock(tuple_pool.blocks().back(), 0);
    }
    if (aux_pool.num_blocks() > 0) {
      disk_manager_->EnqueueBlock(aux_pool.blocks().back(), 0);
    }

    // Tuples coming out of the merger have stripped out any normalized sort key,
    // so the tuple size is guaranteed to be tuple_size_.
    // Push to the front since we take Runs off the back.
    runs_.push_front(
      new Run(tuple_pool.blocks(), aux_pool.blocks(), tuple_size_));
  }

  COUNTER_UPDATE(stats_.phase2_num_runs_merged, runs_.size());
  for (int i = 0; i < runs_.size(); ++i) {
    RunBatchSupplier* supplier = new RunBatchSupplier(this, buffer_pool_.get(), runs_[i]);
    supplier->Prepare();
    final_merger_->AddRun(supplier);
  }
}

Status Sorter::GetNext(RowBatch* row_batch, bool *eos) {
  if (run_builder_ != NULL && !run_builder_->empty()) {
    CreateSortedRun(run_builder_, false);
    run_builder_ = NULL;
    run_builder_threads_.join_all();
  }

  bool use_merger = runs_.size() > 1 || remove_dups_;

  if (!started_merging_) {
    if (use_merger) {
      MultilevelMerge();
    } else {
      if (runs_.size() == 0) {
        *eos = true;
        return Status::OK;
      }

      single_run_batch_supplier_.reset(
          new RunBatchSupplier(this, buffer_pool_.get(), runs_.back()));
      single_run_batch_supplier_->Prepare();
    }

    started_merging_ = true;
    disk_manager_->StopFlushingBuffers();
  }

  if (use_merger) {
    SCOPED_TIMER(stats().phase2_merge_time);
    return final_merger_->GetNext(row_batch, eos);
  } else {
    return single_run_batch_supplier_->GetNext(row_batch, eos);
  }
}

}
