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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/random.hpp>
#include <boost/random/normal_distribution.hpp>
#include <boost/random/variate_generator.hpp>
#include <gtest/gtest.h>

#include "sorter.h"
#include "sort-util.h"
#include "sorted-merger.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "exprs/expr.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/tuple-row.h"
#include "runtime/types.h"
#include "testutil/desc-tbl-builder.h"

using namespace std;
using namespace boost;

namespace impala {

class SorterTest : public testing::Test {
 public:
  static const int BATCH_CAPACITY = 100;  // rows

  SorterTest() : writer_(),
      runtime_state_(TUniqueId(), TUniqueId(), TQueryContext(), "", NULL) {
    resource_pool_.reset(resource_mgr_.RegisterPool());
    Reset();
  }

  ~SorterTest() {
    writer_->Cancel();
    io_mgr_->UnregisterContext(reader_);
  }

  void Reset() {
    if (writer_.get() != NULL) writer_->Cancel();
    if (io_mgr_.get() != NULL) io_mgr_->UnregisterContext(reader_);

    writer_.reset(new DiskWriter());
    writer_->Init();
    io_mgr_.reset(new DiskIoMgr());
    Status status = io_mgr_->Init(&mem_tracker_);
    DCHECK(status.ok());
    status = io_mgr_->RegisterContext(NULL, &reader_);
    DCHECK(status.ok());
  }

  // Initializes all data to 0s.
  RowBatch* CreateRowBatch(RowDescriptor* row_desc) {
    MemTracker* mem_tracker = new MemTracker(1024*1024*512);
    RowBatch* batch = obj_pool_.Add(new RowBatch(*row_desc, BATCH_CAPACITY, mem_tracker));

    vector<TupleDescriptor*> tuple_descs = row_desc->tuple_descriptors();
    vector<uint8_t*> tuple_mem;

    for (int i = 0; i < tuple_descs.size(); ++i) {
      int byte_size = tuple_descs[i]->byte_size();
      tuple_mem.push_back(reinterpret_cast<uint8_t*>(
          batch->tuple_data_pool()->Allocate(BATCH_CAPACITY * byte_size)));
      bzero(tuple_mem.back(), BATCH_CAPACITY * byte_size);
    }

    for (int i = 0; i < BATCH_CAPACITY; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);

      for (int j = 0; j < tuple_descs.size(); ++j) {
        int byte_size = tuple_descs[j]->byte_size();
        row->SetTuple(j, reinterpret_cast<Tuple*>(tuple_mem[j] + i*byte_size));
      }

      batch->CommitLastRow();
    }

    return batch;
  }

  // Simply allocates enough memory to hold a RowBatch for the given tuples.
  RowBatch* CreateEmptyOutputBatch(RowDescriptor* output_row_desc,
      TupleDescriptor* output_tuple_desc) {
    MemTracker* mem_tracker = new MemTracker(1024*1024*512);
    RowBatch* batch = obj_pool_.Add(
        new RowBatch(*output_row_desc, BATCH_CAPACITY, mem_tracker));

    int byte_size = output_tuple_desc->byte_size();
    uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(
        batch->tuple_data_pool()->Allocate(BATCH_CAPACITY * byte_size));
    bzero(tuple_mem, BATCH_CAPACITY * byte_size);
    return batch;
  }

  // Makes a SlotRef referencing the given slot in the given tuple in the given row.
  inline SlotRef* MakeSlotRef(RowDescriptor* row_desc, int tuple_index, int slot_index) {
    TupleDescriptor* tuple_desc = row_desc->tuple_descriptors()[tuple_index];
    SlotDescriptor* slot_desc = tuple_desc->slots()[slot_index];
    SlotRef* slot_ref = obj_pool_.Add(new SlotRef(slot_desc));
    slot_ref->Prepare(&runtime_state_, *row_desc);
    return slot_ref;
  }

  // Makes the given slot NULL'd.
  void NullSlot64(RowBatch* batch, int row_index, int tuple_index, int slot_index,
      RowDescriptor* row_desc) {
    TupleRow* row = batch->GetRow(row_index);
    Tuple* tuple = row->GetTuple(tuple_index);
    TupleDescriptor* tuple_desc = row_desc->tuple_descriptors()[tuple_index];
    SlotDescriptor* slot_desc = tuple_desc->slots()[slot_index];
    *((int64_t*) tuple->GetSlot(slot_desc->tuple_offset())) = 0;
    tuple->SetNull(slot_desc->null_indicator_offset());
  }

  // Retrives all the sorter's results, and validates consecutive tuples by calling the
  // validation function with the current tuple's and previous tuple's values.
  // By nature, this will retrieve exactly the first two slots of the tuple.
  template <class Executor, typename first_type, typename second_type>
  void ValidateTwoSlotResults(Executor* executor, int num_rows_expected,
      RowDescriptor* output_row_desc, TupleDescriptor* output_tuple_desc,
      void (*validation_func)(int, first_type, second_type, first_type, second_type)) {

    RowBatch* result_batch;
    bool eos = false;
    int absolute_index = 0;
    first_type last_first_val;
    second_type last_second_val;
    do {
      result_batch = CreateEmptyOutputBatch(output_row_desc, output_tuple_desc);
      EXPECT_TRUE(executor->GetNext(result_batch, &eos).ok());

      uint32_t first_slot_offset =  output_tuple_desc->slots()[0]->tuple_offset();
      uint32_t second_slot_offset = output_tuple_desc->slots()[1]->tuple_offset();
      for (int i = 0; i < result_batch->num_rows(); ++i, ++absolute_index) {
        TupleRow* row = result_batch->GetRow(i);
        first_type cur_first_val  =
            *((first_type*) row->GetTuple(0)->GetSlot(first_slot_offset));
        second_type cur_second_val  =
            *((second_type*) row->GetTuple(0)->GetSlot(second_slot_offset));

        if (absolute_index > 0) {
          validation_func(absolute_index, cur_first_val, cur_second_val,
              last_first_val, last_second_val);
        }

        DCHECK_LT(absolute_index, num_rows_expected);

        last_first_val = cur_first_val;
        last_second_val = cur_second_val;
      }
    } while (!eos);

    ASSERT_EQ(num_rows_expected, absolute_index);
  }

  // Creates input and output row descriptors given a desc_tbl which contains the
  // actual tuples. Assumes TupleId=0 is the input tuple and TupleId=1 is the output,
  // and that the TupleRow has only one Tuple.
  void CreateDescriptors(DescriptorTbl* desc_tbl, RowDescriptor** input_row_desc,
      RowDescriptor** output_row_desc, TupleDescriptor** output_tuple_desc) {
    runtime_state_.set_desc_tbl(desc_tbl);
    vector<bool> nullable_tuples(1, false);

    vector<TTupleId> input_tuple_id(1, (TTupleId) 0);
    *input_row_desc = obj_pool_.Add(new RowDescriptor(*desc_tbl, input_tuple_id,
        nullable_tuples));

    vector<TTupleId> output_tuple_id(1, (TTupleId) 1);
    *output_row_desc = obj_pool_.Add(new RowDescriptor(*desc_tbl, output_tuple_id,
        nullable_tuples));
    *output_tuple_desc = (*output_row_desc)->tuple_descriptors()[0];
  }

  // TwoInt Dataset -
  //   Input: TupleRow with 1 Tuples, with 2 BigInt Slots.
  //          The first int has a value i+1 (for the i-th tuple)
  //          The second has a value 10-i/5, so it is not unique and becomes negative
  //   Sorting: Sort by the *second* column, then by the first.
  //   Output: A single Tuple with both BigInts, in the same order as the input.

  // Initializes a Sorter* object for the given parameters in a TwoInt dataset.
  void SetupTwoIntDataExecutor(bool first_ascending, bool second_ascending,
      RowDescriptor** input_row_desc, RowDescriptor** output_row_desc,
      TupleDescriptor** output_tuple_desc, vector<bool>* sort_ascending,
      int* sort_key_size, vector<Expr*>* output_slot_exprs,
      vector<Expr*>* sort_exprs_lhs, vector<Expr*>* sort_exprs_rhs) {

    DescriptorTblBuilder builder(&obj_pool_);
    builder.DeclareTuple() << TYPE_BIGINT << TYPE_BIGINT; // input schema
    builder.DeclareTuple() << TYPE_BIGINT << TYPE_BIGINT; // output schema
    DescriptorTbl* desc_tbl = builder.Build();
    CreateDescriptors(desc_tbl, input_row_desc, output_row_desc, output_tuple_desc);

    *sort_key_size = (*output_tuple_desc)->byte_size() + 2 /* NULL bytes */;

    // Output slots are simply the first and second columns in order.
    output_slot_exprs->push_back(MakeSlotRef(*input_row_desc, 0, 0));
    output_slot_exprs->push_back(MakeSlotRef(*input_row_desc, 0, 1));

    // We sort by the second column, then the first.
    sort_exprs_lhs->push_back(MakeSlotRef(*output_row_desc, 0, 1));
    sort_exprs_lhs->push_back(MakeSlotRef(*output_row_desc, 0, 0));
    sort_exprs_rhs->push_back(MakeSlotRef(*output_row_desc, 0, 1));
    sort_exprs_rhs->push_back(MakeSlotRef(*output_row_desc, 0, 0));

    sort_ascending->push_back(first_ascending);
    sort_ascending->push_back(second_ascending);
  }

  Sorter* SetupTwoIntDataSort(bool nulls_first, bool remove_dups, uint64_t mem_limit,
      bool first_ascending, bool second_ascending, RowDescriptor** input_row_desc,
      RowDescriptor** output_row_desc, TupleDescriptor** output_tuple_desc,
      int64_t block_size = 1024 * 1024) {
    vector<bool> sort_ascending;
    int sort_key_size;
    vector<Expr*> output_slot_exprs;
    vector<Expr*> sort_exprs_lhs;
    vector<Expr*> sort_exprs_rhs;
    SetupTwoIntDataExecutor(first_ascending, second_ascending, input_row_desc,
        output_row_desc, output_tuple_desc, &sort_ascending, &sort_key_size,
        &output_slot_exprs, &sort_exprs_lhs, &sort_exprs_rhs);
    return
      new Sorter(writer_.get(), io_mgr_.get(), reader_, resource_pool_.get(),
                 **output_tuple_desc, output_slot_exprs, sort_exprs_lhs, sort_exprs_rhs,
                 sort_ascending, vector<bool>(sort_ascending.size(), nulls_first),
                 remove_dups, sort_key_size, mem_limit, block_size);
  }

  SortedMerger* SetupTwoIntDataMerger(bool nulls_first, bool remove_dups,
      uint64_t mem_limit, bool first_ascending, bool second_ascending,
      RowDescriptor** input_row_desc, RowDescriptor** output_row_desc,
      TupleDescriptor** output_tuple_desc) {
    vector<bool> sort_ascending;
    int sort_key_size;
    vector<Expr*> output_slot_exprs;
    vector<Expr*> sort_exprs_lhs;
    vector<Expr*> sort_exprs_rhs;
    SetupTwoIntDataExecutor(first_ascending, second_ascending, input_row_desc,
        output_row_desc, output_tuple_desc, &sort_ascending, &sort_key_size,
        &output_slot_exprs, &sort_exprs_lhs, &sort_exprs_rhs);
    return
        new SortedMerger(**output_row_desc, sort_exprs_lhs, sort_exprs_rhs,
            sort_ascending, vector<bool>(sort_ascending.size(), nulls_first),
            remove_dups, mem_limit);
  }

  // Produces a RowBatch with 2 columns of data, the first unique and increasing and
  // the second non-unique and decreasing (below zero).
  int PopulateTwoIntData(RowDescriptor* row_desc, int start_index, RowBatch** batch) {
    SlotDescriptor* first_slot =  row_desc->tuple_descriptors()[0]->slots()[0];
    SlotDescriptor* second_slot = row_desc->tuple_descriptors()[0]->slots()[1];

    *batch = CreateRowBatch(row_desc);

    for (int i = start_index; i < start_index+BATCH_CAPACITY; ++i) {
      TupleRow* row = (*batch)->GetRow(i-start_index);
      *((int64_t*) row->GetTuple(0)->GetSlot(first_slot->tuple_offset()))  = i+1;
      *((int64_t*) row->GetTuple(0)->GetSlot(second_slot->tuple_offset())) = 10-i/5;
    }

    return BATCH_CAPACITY;
  }

  // FloatTimeDataset -
  //   Input: TupleRow with 1 Tuple with a Float and Timestamp slot.
  //          The float takes values sampled from a normal distribution (mean=0, var=1).
  //          The timestamp is sampled from a uniform distribution as a single int64.
  //          All samples are taken deterministically between runs (i.e., constant seed).
  //   Output: A single Tuple with the same slots as the original.

  // Initializes a Sorter* object for the given parameters in a FloatTime dataset.
  Sorter* SetupFloatTimeDataSort(bool nulls_first, bool remove_dups, uint64_t mem_limit,
      bool first_ascending, bool second_ascending, bool sort_first_by_float,
      RowDescriptor** input_row_desc, RowDescriptor** output_row_desc,
      TupleDescriptor** output_tuple_desc, int64_t block_size = 1024 * 1024) {

    DescriptorTblBuilder builder(&obj_pool_);
    builder.DeclareTuple() << TYPE_FLOAT << TYPE_TIMESTAMP; // input schema
    builder.DeclareTuple() << TYPE_FLOAT << TYPE_TIMESTAMP; // output schema
    DescriptorTbl* desc_tbl = builder.Build();
    CreateDescriptors(desc_tbl, input_row_desc, output_row_desc, output_tuple_desc);

    int sort_key_size = (*output_tuple_desc)->byte_size() + 2 /* NULL bytes */;

    // Output slots are simply the first and second columns in order.
    vector<Expr*> output_slot_exprs;
    output_slot_exprs.push_back(MakeSlotRef(*input_row_desc, 0, 0));
    output_slot_exprs.push_back(MakeSlotRef(*input_row_desc, 0, 1));

    vector<Expr*> sort_exprs_lhs;
    vector<Expr*> sort_exprs_rhs;
    if (sort_first_by_float) {
      sort_exprs_lhs.push_back(MakeSlotRef(*output_row_desc, 0, 0));
      sort_exprs_lhs.push_back(MakeSlotRef(*output_row_desc, 0, 1));
      sort_exprs_rhs.push_back(MakeSlotRef(*output_row_desc, 0, 0));
      sort_exprs_rhs.push_back(MakeSlotRef(*output_row_desc, 0, 1));
    } else {
      sort_exprs_lhs.push_back(MakeSlotRef(*output_row_desc, 0, 1));
      sort_exprs_lhs.push_back(MakeSlotRef(*output_row_desc, 0, 0));
      sort_exprs_rhs.push_back(MakeSlotRef(*output_row_desc, 0, 1));
      sort_exprs_rhs.push_back(MakeSlotRef(*output_row_desc, 0, 0));
    }

    vector<bool> sort_ascending;
    sort_ascending.push_back(first_ascending);
    sort_ascending.push_back(second_ascending);

    return
      new Sorter(writer_.get(), io_mgr_.get(), reader_, resource_pool_.get(),
                 **output_tuple_desc, output_slot_exprs, sort_exprs_lhs, sort_exprs_rhs,
                 sort_ascending, vector<bool>(sort_ascending.size(), nulls_first),
                 remove_dups, sort_key_size, mem_limit, block_size);
  }

  // Produces a RowBatch with 2 columns of data, a float and a timestamp.
  // Distribution described above, as part of the FloatTimeDataset.
  int PopulateFloatTimeData(RowDescriptor* row_desc, int start_index, RowBatch** batch) {
    mt19937 engine(start_index+539);
    normal_distribution<double> float_dist(0.0, 1.0);
    variate_generator<mt19937&, normal_distribution<double> > float_rng(engine,
        float_dist);
    uniform_int<int> int_dist(0, 946684800); // 1/1/2000

    SlotDescriptor* first_slot  = row_desc->tuple_descriptors()[0]->slots()[0];
    SlotDescriptor* second_slot = row_desc->tuple_descriptors()[0]->slots()[1];

    *batch = CreateRowBatch(row_desc);

    for (int i = start_index; i < start_index+BATCH_CAPACITY; ++i) {
      TupleRow* row = (*batch)->GetRow(i-start_index);
      float float_val = (float) float_rng();
      *((float*) row->GetTuple(0)->GetSlot(first_slot->tuple_offset())) = float_val;

      uint32_t time = int_dist(engine);
      TimestampValue ts((int32_t) time);
      *((TimestampValue*) row->GetTuple(0)->GetSlot(second_slot->tuple_offset())) = ts;
    }

    return BATCH_CAPACITY;
  }

  // PrefixDataset -
  //   Input: TupleRow with 1 Tuple with a TinyInt and String slot.
  //          The int takes values sampled from a uniform distribution between 0 and 5.
  //          The string is composed of 'A' and 'B' values with a length uniformly
  //          distributed between 2 and 20.
  //          The sort key is restricted to 5 bytes (2 bytes used by tinyint).
  //   Sorting: Sort first by the int, then by the string.
  //   Output: A single Tuple with the same columns as the input.

  // Initializes a Sorter* object for the given parameters in a Prefix dataset.
  Sorter* SetupPrefixDataSort(bool nulls_first, bool remove_dups, uint64_t mem_limit,
      bool first_ascending, bool second_ascending,
      RowDescriptor** input_row_desc, RowDescriptor** output_row_desc,
      TupleDescriptor** output_tuple_desc, int64_t block_size = 1024 * 1024) {

    DescriptorTblBuilder builder(&obj_pool_);
    builder.DeclareTuple() << TYPE_TINYINT << TYPE_STRING; // input schema
    builder.DeclareTuple() << TYPE_TINYINT << TYPE_STRING; // output schema
    DescriptorTbl* desc_tbl = builder.Build();
    CreateDescriptors(desc_tbl, input_row_desc, output_row_desc, output_tuple_desc);

    int sort_key_size = 5;

    // Output slots are simply the first and second columns in order.
    vector<Expr*> output_slot_exprs;
    output_slot_exprs.push_back(MakeSlotRef(*input_row_desc, 0, 0));
    output_slot_exprs.push_back(MakeSlotRef(*input_row_desc, 0, 1));

    // Sort by the first column, then the second.
    vector<Expr*> sort_exprs_lhs;
    sort_exprs_lhs.push_back(MakeSlotRef(*output_row_desc, 0, 0));
    sort_exprs_lhs.push_back(MakeSlotRef(*output_row_desc, 0, 1));
    vector<Expr*> sort_exprs_rhs;
    sort_exprs_rhs.push_back(MakeSlotRef(*output_row_desc, 0, 0));
    sort_exprs_rhs.push_back(MakeSlotRef(*output_row_desc, 0, 1));

    vector<bool> sort_ascending;
    sort_ascending.push_back(first_ascending);
    sort_ascending.push_back(second_ascending);

    return
      new Sorter(writer_.get(), io_mgr_.get(), reader_, resource_pool_.get(),
                 **output_tuple_desc, output_slot_exprs, sort_exprs_lhs, sort_exprs_rhs,
                 sort_ascending, vector<bool>(sort_ascending.size(), nulls_first),
                 remove_dups, sort_key_size, mem_limit, block_size);
  }

  // Produces a RowBatch with 2 columns of data, an int and a string.
  // String data is allocated individually, so it may not be consecutive.
  // Distribution described above, as part of the PrefixDataset.
  int PopulatePrefixData(RowDescriptor* row_desc, int start_index, RowBatch** batch) {
    mt19937 engine(start_index+539);
    uniform_int<int> int_dist(0, 5);
    uniform_int<int> strlen_dist(2, 20);
    uniform_int<int> char_dist(65, 66); // A and B

    SlotDescriptor* first_slot  = row_desc->tuple_descriptors()[0]->slots()[0];
    SlotDescriptor* second_slot = row_desc->tuple_descriptors()[0]->slots()[1];

    *batch = CreateRowBatch(row_desc);

    for (int i = start_index; i < start_index+BATCH_CAPACITY; ++i) {
      TupleRow* row = (*batch)->GetRow(i-start_index);
      int8_t int_val = (int8_t) int_dist(engine);
      *((int8_t*) row->GetTuple(0)->GetSlot(first_slot->tuple_offset())) = int_val;

      int strlen = strlen_dist(engine);
      char* string_mem = (char*) (*batch)->tuple_data_pool()->Allocate(strlen);
      for (int j = 0; j < strlen; ++j) {
        string_mem[j] = (char) char_dist(engine);
      }

      StringValue str(string_mem, strlen);
      *((StringValue*) row->GetTuple(0)->GetSlot(second_slot->tuple_offset())) = str;
    }

    return BATCH_CAPACITY;
  }

 private:
  ObjectPool obj_pool_;
  MemTracker mem_tracker_;
  scoped_ptr<DiskWriter> writer_;
  scoped_ptr<DiskIoMgr> io_mgr_;
  DiskIoMgr::RequestContext* reader_;
  ThreadResourceMgr resource_mgr_;
  scoped_ptr<ThreadResourceMgr::ResourcePool> resource_pool_;
  RuntimeState runtime_state_;
};

void BasicTestValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  //printf("<%ld / %ld>\n", last_unique_val, last_group_val);
  // Since we're sorting first on a decreasing-but-not-unique column and then on an
  // increasing column, we expect to see the first column in little ascending runs
  // of 5 each, but each run should be overall decreasing.
  if (index % 5 == 0) {
    ASSERT_LT(cur_unique_val, last_unique_val);
  } else {
    ASSERT_GT(cur_unique_val, last_unique_val);
  }
}

TEST_F(SorterTest, BasicSort) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = true;
  const bool second_key_ascending = true;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupTwoIntDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
      &output_tuple_desc));

  RowBatch* batch;
  int num_rows = 0;
  num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);
  num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      BasicTestValidator);
}

void NullsFirstValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  // Second column is descending, which should mean the first column is
  // always ascending, save for the initial NULL value in the second column.
  if (index == 1) {
    ASSERT_EQ(133, last_unique_val);
    ASSERT_EQ(1, cur_unique_val);
  } else {
    ASSERT_GT(cur_unique_val, last_unique_val);
  }
}

TEST_F(SorterTest, NullsFirst) {
  const bool nulls_first = true;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = false;
  const bool second_key_ascending = true;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupTwoIntDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
      &output_tuple_desc));

  RowBatch* batch;
  int num_rows = 0;
  DCHECK_EQ(BATCH_CAPACITY, 100);
  num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);
  num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
  NullSlot64(batch, 32, 0, 1, input_row_desc);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      NullsFirstValidator);
}

void NullsLastValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  // Data will be reversed so the val will appear *ascending*, except
  // in runs of 5 where it will internally be descending.
  // Last value should be NULL, which causes a hiccup after its associated
  // value 133.
  if (index == 199) {
    ASSERT_EQ(133, cur_unique_val);
  } else {
    if ( (index % 5 == 0 && index < 132)
         || (index % 5 == 4 && index >= 132)) {
      ASSERT_GT(cur_unique_val, last_unique_val);
    } else {
      ASSERT_LT(cur_unique_val, last_unique_val);
    }
  }
}

TEST_F(SorterTest, NullsLast) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = false;
  const bool second_key_ascending = false;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupTwoIntDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
      &output_tuple_desc));

  RowBatch* batch;
  int num_rows = 0;
  DCHECK_EQ(BATCH_CAPACITY, 100);
  num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);
  num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
  NullSlot64(batch, 32, 0, 1, input_row_desc);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      NullsLastValidator);
}

void TestDuplicationValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  // Data will be in reverse order so the val will appear *descending* everywhere.
  // Every odd index should be the same.
  if (index % 2 == 1) {
    ASSERT_EQ(cur_unique_val, last_unique_val);
  } else {
    ASSERT_LT(cur_unique_val, last_unique_val);
  }
}

TEST_F(SorterTest, Duplication) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = true;
  const bool second_key_ascending = false;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupTwoIntDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
      &output_tuple_desc));

  RowBatch* batch;
  int num_rows = 0;
  num_rows += PopulateTwoIntData(input_row_desc, 0, &batch);
  sorter->AddBatch(batch);
  // Induce duplicates by restarting indexing at 0 (instead of num_rows)
  num_rows += PopulateTwoIntData(input_row_desc, 0 /* induce duplicates! */, &batch);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      TestDuplicationValidator);
}

void TestNoDuplicationValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  // Data will be in reverse order so the val will appear *descending* everywhere.
  ASSERT_LT(cur_unique_val, last_unique_val);
}

TEST_F(SorterTest, NoDuplication) {
  const bool nulls_first = false;
  const bool remove_dups = true;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = true;
  const bool second_key_ascending = false;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupTwoIntDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
      &output_tuple_desc));

  RowBatch* batch;
  int num_rows = 0;
  num_rows += PopulateTwoIntData(input_row_desc, 0, &batch);
  sorter->AddBatch(batch);
  // Induce duplicates by restarting indexing at 0 (instead of num_rows)
  num_rows += PopulateTwoIntData(input_row_desc, 0 /* induce duplicates! */, &batch);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows / 2, output_row_desc, output_tuple_desc,
      TestNoDuplicationValidator);
}

void TestFloatsValidator(int index, float cur_float, TimestampValue cur_timestamp,
    float last_float, TimestampValue last_timestamp) {
  // We sort ascending by floats.
  ASSERT_GE(cur_float, last_float);
}

TEST_F(SorterTest, Floats) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = true;
  const bool second_key_ascending = false;
  const bool sort_first_by_float = true;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupFloatTimeDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, sort_first_by_float,
      &input_row_desc, &output_row_desc, &output_tuple_desc));

  RowBatch* batch;
  int num_rows = 0;
  num_rows += PopulateFloatTimeData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);
  num_rows += PopulateFloatTimeData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      TestFloatsValidator);
}

uint32_t TimestampToDate(TimestampValue timestamp) {
    return timestamp.date().day()
        | (timestamp.date().month() << 5)
        | (timestamp.date().year() << 9);
}

void TestTimestampsValidator(int index, float cur_float, TimestampValue cur_timestamp,
    float last_float, TimestampValue last_timestamp) {
  // We sort descending by timestamps.
  uint32_t last_date = TimestampToDate(last_timestamp);
  uint32_t cur_date = TimestampToDate(cur_timestamp);
  if (cur_date == last_date) {
    ASSERT_LE(cur_timestamp.time_of_day().total_nanoseconds(),
        last_timestamp.time_of_day().total_nanoseconds());
  } else {
    ASSERT_LT(cur_date, last_date);
  }
}

TEST_F(SorterTest, Timestamps) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = false;
  const bool second_key_ascending = false;
  const bool sort_first_by_float = false /* sort by timestamps! */;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupFloatTimeDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, sort_first_by_float,
      &input_row_desc, &output_row_desc, &output_tuple_desc));

  RowBatch* batch;
  int num_rows = 0;
  num_rows += PopulateFloatTimeData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);
  num_rows += PopulateFloatTimeData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      TestTimestampsValidator);
}

void TestPrefixValidator(int index, uint8_t cur_int, StringValue cur_str,
    uint8_t last_int, StringValue last_str) {
  // We sort ascending by ints, then timestamps.
  ASSERT_GE(cur_int, last_int);
  if (cur_int == last_int) {
    ASSERT_GE(RawValue::Compare(&cur_str, &last_str, TYPE_STRING), 0);
  }
}

TEST_F(SorterTest, PrefixStrings) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = true;
  const bool second_key_ascending = true;
  const int block_size = 128;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupPrefixDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending,
      &input_row_desc, &output_row_desc, &output_tuple_desc, block_size));

  RowBatch* batch;
  int num_rows = 0;
  num_rows += PopulatePrefixData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);
  num_rows += PopulatePrefixData(input_row_desc, num_rows, &batch);
  sorter->AddBatch(batch);

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      TestPrefixValidator);
}

TEST_F(SorterTest, SortUtil) {
  typedef uint32_t Element;
  int element_size = sizeof(Element);

  // Test increasing sizes, since the sort algorithm changes behavior based on
  // number of elements.
  for (int num_elements = 0; num_elements < 100; ++num_elements) {
    vector<Element> v;
    BufferPool buffer_pool(10, 1024);
    ObjectPool obj_pool;
    BlockMemPool block_mem_pool(&obj_pool, &buffer_pool);
    BlockedVector<Element> bv(&block_mem_pool, element_size);

    for (int i = 0; i < num_elements; ++i) {
      Element element = rand();
      v.push_back(element);
      Element normalized_element = BitUtil::ByteSwap(element);
      bv.Insert(&normalized_element);
    }

    sort(v.begin(), v.end());
    SortUtil<Element>::SortNormalized(bv.Begin(), bv.End(),
        element_size, 0, element_size);

    // Compare results
    for (int i = 0; i < num_elements; ++i) {
      ASSERT_EQ(v[i], BitUtil::ByteSwap(*bv[i]));
    }
  }
}

// SortedMerger Tests
// Supplies a set of RowBatches, one at a time. Assumes each row has 1 tuple.
class BatchSupplier : public RowBatchSupplier {
  vector<RowBatch*> batches_;
  int index_;

 public:
  BatchSupplier(vector<RowBatch*> batches) : batches_(batches), index_(0) {
  }

  Status GetNext(RowBatch* row_batch, bool* eos) {
    if (index_ < batches_.size()) {
      RowBatch* batch = batches_[index_];

      for (int i = 0; i < batch->num_rows(); ++i) {
        DCHECK(!row_batch->AtCapacity()) << "Merge batch is too small for test";
        Tuple* tuple = batch->GetRow(i)->GetTuple(0);
        int row_idx = row_batch->AddRow();
        row_batch->GetRow(row_idx)->SetTuple(0, tuple);
        row_batch->CommitLastRow();
      }

      ++index_;
    }

    *eos = (index_ >= batches_.size());

    return Status::OK;
  }
};

void BasicMergeValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  // Merger should preserve original order (group val decreasing, unique val increasing).
  ASSERT_GT(cur_unique_val, last_unique_val);
  ASSERT_LE(cur_group_val, last_group_val);
}

TEST_F(SorterTest, BasicMerge) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = false;
  const bool second_key_ascending = true;
  int num_runs = 5;

  ObjectPool obj_pool;
  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<SortedMerger> merger(SetupTwoIntDataMerger(nulls_first, remove_dups,
      mem_limit, first_key_ascending, second_key_ascending,
      &input_row_desc, &output_row_desc, &output_tuple_desc));

  int num_rows = 0;
  for (int i = 0; i < num_runs; ++i) {
    vector<RowBatch*> batches;
    RowBatch* batch;
    num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
    batches.push_back(batch);
    num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
    batches.push_back(batch);

    merger->AddRun(new BatchSupplier(batches));
  }

  ValidateTwoSlotResults(merger.get(), num_rows, output_row_desc, output_tuple_desc,
      BasicMergeValidator);
}

void DuplicateMergeValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  // Duplicates are not removed, so every set of 5 values should be equivalent.
  if (index % 5 == 0) {
    ASSERT_GT(cur_unique_val, last_unique_val);
    ASSERT_LE(cur_group_val, last_group_val);
  } else {
    ASSERT_EQ(cur_unique_val, last_unique_val);
    ASSERT_EQ(cur_group_val, last_group_val);
  }
}

TEST_F(SorterTest, DuplicateMerge) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = false;
  const bool second_key_ascending = true;
  int num_runs = 5;

  ObjectPool obj_pool;
  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<SortedMerger> merger(SetupTwoIntDataMerger(nulls_first, remove_dups,
      mem_limit, first_key_ascending, second_key_ascending,
      &input_row_desc, &output_row_desc, &output_tuple_desc));

  int num_rows = 0;
  for (int i = 0; i < num_runs; ++i) {
    vector<RowBatch*> batches;
    RowBatch* batch;
    num_rows = 0; // restarting the indexing will cause all runs to be the same
    num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
    batches.push_back(batch);
    num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
    batches.push_back(batch);

    merger->AddRun(new BatchSupplier(batches));
  }

  ValidateTwoSlotResults(merger.get(), num_runs * num_rows,
      output_row_desc, output_tuple_desc,
      DuplicateMergeValidator);
}

void NoDuplicateMergeValidator(int index, int64_t cur_unique_val, int64_t cur_group_val,
    int64_t last_unique_val, int64_t last_group_val) {
  // Duplicates should be removed, so every value should be unique
  ASSERT_GT(cur_unique_val, last_unique_val);
  ASSERT_LE(cur_group_val, last_group_val);
}

TEST_F(SorterTest, NoDuplicateMerge) {
  const bool nulls_first = false;
  const bool remove_dups = true;
  const int mem_limit = 1024*1024*512;
  const bool first_key_ascending = false;
  const bool second_key_ascending = true;
  int num_runs = 5;

  ObjectPool obj_pool;
  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<SortedMerger> merger(SetupTwoIntDataMerger(nulls_first, remove_dups,
      mem_limit, first_key_ascending, second_key_ascending,
      &input_row_desc, &output_row_desc, &output_tuple_desc));

  int num_rows = 0;
  for (int i = 0; i < num_runs; ++i) {
    vector<RowBatch*> batches;
    RowBatch* batch;
    num_rows = 0; // restarting the indexing will cause all runs to be the same
    num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
    batches.push_back(batch);
    num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
    batches.push_back(batch);

    merger->AddRun(new BatchSupplier(batches));
  }

  ValidateTwoSlotResults(merger.get(), num_rows, output_row_desc, output_tuple_desc,
      NoDuplicateMergeValidator);
}

TEST_F(SorterTest, ExternalSort) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 10000;
  const bool first_key_ascending = true;
  const bool second_key_ascending = true;
  const int block_size = 128;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;

  for (int merge_levels = 0; merge_levels < 4; ++merge_levels) {
    Reset();
    scoped_ptr<Sorter> sorter(SetupTwoIntDataSort(nulls_first, remove_dups, mem_limit,
        first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
        &output_tuple_desc, block_size));

    RowBatch* batch;
    int num_rows = 0;
    for (int i = 0; i < pow(12, merge_levels); ++i) {
      num_rows += PopulateTwoIntData(input_row_desc, num_rows, &batch);
      sorter->AddBatch(batch);
    }

    ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
        BasicTestValidator);
  }
}

// Test external sort with auxiliary (string) data
TEST_F(SorterTest, AuxExternalSort) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 6000;
  const bool first_key_ascending = true;
  const bool second_key_ascending = true;
  const int block_size = 128;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;

  for (int merge_levels = 0; merge_levels < 4; ++merge_levels) {
    Reset();
    scoped_ptr<Sorter> sorter(SetupPrefixDataSort(nulls_first, remove_dups, mem_limit,
        first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
        &output_tuple_desc, block_size));

    RowBatch* batch;
    int num_rows = 0;
    for (int i = 0; i < pow(8, merge_levels); ++i) {
      num_rows += PopulatePrefixData(input_row_desc, num_rows, &batch);
      sorter->AddBatch(batch);
    }

    ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
        TestPrefixValidator);
  }
}

// Test an extremely memory-constrained sort.
// Can only merge 2 runs at a time, and requires ~9 merge levels.
TEST_F(SorterTest, MemoryConstrained) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const bool first_key_ascending = true;
  const bool second_key_ascending = true;
  const int block_size = 128;

  // Only enough memory for 3 runs (2 input, 1 output; double buffered with aux data).
  const int mem_limit = 3 * 2 * 2 * block_size;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;

  scoped_ptr<Sorter> sorter(SetupPrefixDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
      &output_tuple_desc, block_size));

  RowBatch* batch;
  int num_rows = 0;
  for (int i = 0; i < 100; ++i) {
    num_rows += PopulatePrefixData(input_row_desc, num_rows, &batch);
    sorter->AddBatch(batch);
  }

  ValidateTwoSlotResults(sorter.get(), num_rows, output_row_desc, output_tuple_desc,
      TestPrefixValidator);
}

TEST_F(SorterTest, NoData) {
  const bool nulls_first = false;
  const bool remove_dups = false;
  const int mem_limit = 512 * 1024 * 1024;
  const bool first_key_ascending = true;
  const bool second_key_ascending = true;

  RowDescriptor* input_row_desc;
  RowDescriptor* output_row_desc;
  TupleDescriptor* output_tuple_desc;
  scoped_ptr<Sorter> sorter(SetupPrefixDataSort(nulls_first, remove_dups, mem_limit,
      first_key_ascending, second_key_ascending, &input_row_desc, &output_row_desc,
      &output_tuple_desc));

  ValidateTwoSlotResults(sorter.get(), 0, output_row_desc, output_tuple_desc,
      TestPrefixValidator);
}

}

int main(int argc, char **argv) {
  impala::InitCommonRuntime(argc, argv, false);
  //  ::testing::GTEST_FLAG(filter) = "*ExternalSort*";
  ::testing::InitGoogleTest(&argc, argv);
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
