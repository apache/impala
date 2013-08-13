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

#include <algorithm>
#include <boost/scoped_ptr.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/variate_generator.hpp>
#include <stdio.h>

#include "blocked-vector.h"
#include "sort-util.h"
#include "sorter.h"
#include "common/object-pool.h"
#include "runtime/mem-limit.h"
#include "runtime/mem-pool.h"
#include "runtime/row-batch.h"
#include "runtime/thread-resource-mgr.h"
#include "testutil/desc-tbl-builder.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/hash-util.h"
#include "util/stopwatch.h"
#include "util/runtime-profile.h"

using namespace boost;
using namespace impala;
using namespace std;

// Benchmark for testing the speed of various kinds of STL sorts versus Impala's sort.
// The STL sort results provide a very good lower bound for sorting fully uniform data.
// We try three methods of laying out data for the STL sort:
//   Indirect: Sort only pointers to the actual data. This is what Impala does right now.
//   Flat: Sort over the actual data, including both key and payload.
//   Key-Only: Sort over only the key (with an additional pointer to the actual data).
//
// Impala was tested in two modes: Flat Impala and Key-Only Impala are identical to
// their STL counterparts except using the Impala NormalizedSort. Just "Impala"
// refers to the full Impala Sorter, which includes the whole process from normalizing
// keys to constructing output row batches. The sort used for "Impala" is the same as
// Flat Impala, though.
//
// Each sort has 3 phases:
//   Insert: For STL, this means copying the test data into appropriate buffers.
//           For Impala, this means calling AddBatch with a populated RowBatch, which
//           will copy the tuple and normalize it.
//   Sort: Time it takes for the sort to return.
//   Output: Time it takes to produce full output tuples placed consecutively in memory.
//           The Flat STL sort gets this for free by being totally in-place.

// Results:
// Machine Info: Intel(R) Core(TM) i7-3770 CPU @ 3.40GHz
// ===== TPCH-1 =====
// Key size: 2 bytes
// Payload size: 60 bytes
// Num rows: 10000k
// Essential data size: 591 MB
//        Function  Insert (s)    Sort (s)  Output (s)   Total (s)  Comparison
// ---------------------------------------------------------------------------
//    Indirect STL     0.03015       4.401      0.4528       4.884          1X
//        Flat STL       0.114       4.115           0       4.229      0.866X
//     Flat Impala      0.1198       3.542           0       3.662     0.7498X
//    Key-Only STL     0.07212        1.76      0.4881        2.32      0.475X
// Key-Only Impala     0.05333       3.255      0.4912       3.799     0.7779X
//          Impala       1.392       4.218     0.03794       5.647      1.156X

// ===== TPCH-3 =====
// Key size: 24 bytes
// Payload size: 12 bytes
// Num rows: 10000k
// Essential data size: 343 MB
//        Function  Insert (s)    Sort (s)  Output (s)   Total (s)  Comparison
// ---------------------------------------------------------------------------
//    Indirect STL     0.03583       6.259      0.3281       6.622          1X
//        Flat STL     0.06828       4.048           0       4.117     0.6216X
//     Flat Impala      0.0726       4.645           0       4.718     0.7124X
//    Key-Only STL      0.1048       3.671       0.352       4.128     0.6233X
// Key-Only Impala     0.05781       4.555      0.3856       4.999     0.7548X
//          Impala       1.056        4.63     0.02132       5.706     0.8617X

// ===== TPCH-16 =====
// Key size: 47 bytes
// Payload size: 0 bytes
// Num rows: 10000k
// Essential data size: 448 MB
//        Function  Insert (s)    Sort (s)  Output (s)   Total (s)  Comparison
// ---------------------------------------------------------------------------
//    Indirect STL     0.01729       6.674      0.4128       7.104          1X
//        Flat STL      0.0931       5.242           0       5.335     0.7509X
//     Flat Impala     0.08989       4.746           0       4.836     0.6807X
//    Key-Only STL      0.1923       4.823      0.4656       5.481     0.7715X
// Key-Only Impala     0.09221       4.995      0.4746       5.562     0.7828X
//          Impala       2.599       5.107     0.03461       7.741       1.09X

// ===== KEY-10% =====
// Key size: 4 bytes
// Payload size: 36 bytes
// Num rows: 10000k
// Essential data size: 381 MB
//        Function  Insert (s)    Sort (s)  Output (s)   Total (s)  Comparison
// ---------------------------------------------------------------------------
//    Indirect STL     0.03502       5.473      0.3254       5.834          1X
//        Flat STL     0.07269       3.149           0       3.222     0.5523X
//     Flat Impala     0.07867       4.206           0       4.284     0.7345X
//    Key-Only STL      0.1191       2.531       3.732       6.382      1.094X
// Key-Only Impala     0.04082       4.807      0.3086       5.157      0.884X
//          Impala       4.692       4.459     0.02559       9.176      1.573X


static bool EXTRACT_KEYS;

// Returns the size of the field type.member
#define MEMBER_SIZE(type, member) sizeof(((type *)0)->member)

template <int key_size, int payload_size>
struct SortTuple {
  char key[key_size];
  char payload[payload_size];
};

template <typename sort_tuple_t>
struct SortTupleKeyPtr {
  char key[MEMBER_SIZE(sort_tuple_t, key)];
  sort_tuple_t* sort_tuple;
};

template <typename sort_tuple_t>
bool cmp(sort_tuple_t x, sort_tuple_t y) {
  return memcmp(x.key, y.key, sizeof(x.key)) < 0;
}

template <typename sort_tuple_t>
bool ptr_cmp(sort_tuple_t* x, sort_tuple_t* y) {
  return memcmp(x->key, y->key, sizeof(x->key)) < 0;
}

struct TestData {
  // Contains all the bytes needed for the sort.
  // Every sort subroutine copies this data before use.
  void* source_data;
  int norm_key_len;
};

struct Timer {
  MonotonicStopWatch insert_timer;
  MonotonicStopWatch sort_timer;
  MonotonicStopWatch output_timer;
};

class ScopedStopWatch {
 public:
  ScopedStopWatch(MonotonicStopWatch& sw) : sw_(&sw) {
    sw_->Start();
  }

  ~ScopedStopWatch() {
    sw_->Stop();
  }

 private:
  // Disable copy constructor and assignment
  ScopedStopWatch(const ScopedStopWatch& timer);
  ScopedStopWatch& operator=(const ScopedStopWatch& timer);

  MonotonicStopWatch* sw_;
};

ObjectPool obj_pool_;

// Memory limit for all row batches and the sorter.
static uint64_t ROW_BATCH_MEM_LIMIT = 1024L * 1024 * 1024 * 3;

// Initializes all data to 0s.
RowBatch* CreateRowBatch(RowDescriptor* row_desc, int batch_size) {
  vector<MemLimit*> mem_limits;
  mem_limits.push_back(new MemLimit(ROW_BATCH_MEM_LIMIT));
  RowBatch* batch = obj_pool_.Add(new RowBatch(*row_desc, batch_size, mem_limits));

  vector<TupleDescriptor*> tuple_descs = row_desc->tuple_descriptors();
  vector<uint8_t*> tuple_mem;

  for (int i = 0; i < tuple_descs.size(); ++i) {
    int byte_size = tuple_descs[i]->byte_size();
    tuple_mem.push_back(reinterpret_cast<uint8_t*>(
        batch->tuple_data_pool()->Allocate(batch_size * byte_size)));
    bzero(tuple_mem.back(), batch_size * byte_size);
  }

  for (int i = 0; i < batch_size; ++i) {
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
    TupleDescriptor* output_tuple_desc, int batch_size) {
  vector<MemLimit*> mem_limits;
  mem_limits.push_back(new MemLimit(ROW_BATCH_MEM_LIMIT));
  RowBatch* batch = obj_pool_.Add(
      new RowBatch(*output_row_desc, batch_size, mem_limits));

  int byte_size = output_tuple_desc->byte_size();
  uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(
      batch->tuple_data_pool()->Allocate(batch_size * byte_size));
  bzero(tuple_mem, batch_size * byte_size);
  return batch;
}

// A SlotRef which directly references the tuple offset, for efficiency purposes.
// TODO(aaron.davidson): Completely inline this, to simulate codegen.
class OffsetSlotRef : public Expr {
 public:
  static void* ComputeFn(Expr* expr, TupleRow* row) {
    OffsetSlotRef* ref = static_cast<OffsetSlotRef*>(expr);
    Tuple* t = row->GetTuple(0);
    return t->GetSlot(ref->offset_);
  }

  OffsetSlotRef(TupleDescriptor* tuple_desc, int slot_idx)
    : Expr(tuple_desc->slots()[slot_idx]->type(), false),
      offset_(tuple_desc->slots()[slot_idx]->tuple_offset()) {
    compute_fn_ = ComputeFn;
  }

 private:
  int offset_;
};

// Creates a row descriptor with only 1 tuple, whose TupleId is 0.
RowDescriptor* CreateRowDescriptor(DescriptorTbl* desc_tbl) {
  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  return obj_pool_.Add(new RowDescriptor(*desc_tbl, tuple_id, nullable_tuples));
}

template <typename sort_tuple_t>
void InitTestData(TestData* data, int num_rows) {
  boost::mt19937 gen;

  uint32_t* source_data = (uint32_t*) calloc(num_rows, sizeof(sort_tuple_t));
  for (int i = 0; i < num_rows * sizeof(sort_tuple_t) / 4; ++i) {
    source_data[i] = gen();
  }
  data->source_data = source_data;
  data->norm_key_len = -1;
  return;
}

// Data is inlined into the vector -- i.e., vector<SortTuple>.
template <typename sort_tuple_t>
void TestFlatSTL(TestData* data, Timer* timer, uint64_t num_rows) {
  vector<sort_tuple_t> tuples;

  {
    ScopedStopWatch stop_watch(timer->insert_timer);
    tuples.reserve(num_rows);

    for (int i = 0; i < num_rows; ++i) {
      sort_tuple_t tuple;
      memcpy(&tuple, (sort_tuple_t*) data->source_data + i, sizeof(sort_tuple_t));
      tuples.push_back(tuple);
    }
  }

  {
    ScopedStopWatch stop_watch(timer->sort_timer);
    sort(tuples.begin(), tuples.end(), cmp<sort_tuple_t>);
  }
}

template <typename sort_tuple_t>
bool my_cmp(void* x_arg, void* y_arg) {
  sort_tuple_t* x = (sort_tuple_t*) x_arg;
  sort_tuple_t* y = (sort_tuple_t*) y_arg;
  return memcmp(x->key, y->key, sizeof(x->key)) < 0;
}

// Data is inlined into the vector -- i.e., vector<SortTuple>.
template <typename sort_tuple_t>
void TestFlatImpala(TestData* data, Timer* timer, uint64_t num_rows) {
  MemPool mem_pool(NULL);
  BufferPool buffer_pool(512, 1024*1024*8);
  ObjectPool obj_pool;
  BlockMemPool block_mem_pool(&obj_pool, &buffer_pool);
  BlockedVector<sort_tuple_t> tuples(&block_mem_pool, sizeof(sort_tuple_t));
  {
    ScopedStopWatch stop_watch(timer->insert_timer);
    for (int i = 0; i < num_rows; ++i) {
      tuples.Insert((sort_tuple_t*) data->source_data + i);
    }
  }

  {
    ScopedStopWatch stop_watch(timer->sort_timer);
    SortUtil<sort_tuple_t>::SortNormalized(tuples.Begin(), tuples.End(),
        sizeof(sort_tuple_t), 0, MEMBER_SIZE(sort_tuple_t, key));
  }

  // Perform verification that the sort worked...
  for (int i = 1; i < num_rows; ++i) {
    sort_tuple_t* x = tuples[i-1];
    sort_tuple_t* y = tuples[i];
    DCHECK_LE(memcmp(x->key, y->key, sizeof(x->key)), 0);
  }
}

// Data is all external to the vector -- i.e., vector<SortTuple*>
template <typename sort_tuple_t>
void TestIndirectSTL(TestData* data, Timer* timer, uint64_t num_rows) {
  vector<sort_tuple_t*> tuples;

  {
    ScopedStopWatch stop_watch(timer->insert_timer);
    tuples.reserve(num_rows);

    for (int i = 0; i < num_rows; ++i) {
      sort_tuple_t* tuple = (sort_tuple_t*) data->source_data + i;
      tuples.push_back(tuple);
    }
  }

  {
    ScopedStopWatch stop_watch(timer->sort_timer);
    sort(tuples.begin(), tuples.end(), ptr_cmp<sort_tuple_t>);
  }

  {
    ScopedStopWatch stop_watch(timer->output_timer);
    vector<sort_tuple_t> output_tuples;
    output_tuples.reserve(num_rows);
    for (int i = 0; i < num_rows; ++i) {
      output_tuples.push_back(*tuples[i]);
    }
  }
}

// Puts the key and a pointer to the data into the actual vector.
template <typename sort_tuple_t>
void TestKeyOnlySTL(TestData* data, Timer* timer, uint64_t num_rows) {
  typedef SortTupleKeyPtr<sort_tuple_t> key_tuple_t;
  vector<key_tuple_t> tuples;

  {
    ScopedStopWatch stop_watch(timer->insert_timer);
    tuples.reserve(num_rows);

    for (int i = 0; i < num_rows; ++i) {
      key_tuple_t key_tuple;
      sort_tuple_t* source_tuple = (sort_tuple_t*) data->source_data + i;
      memcpy(&key_tuple, source_tuple, sizeof(source_tuple->key));
      key_tuple.sort_tuple = source_tuple;
      tuples.push_back(key_tuple);
    }
  }

  {
    ScopedStopWatch stop_watch(timer->sort_timer);
    sort(tuples.begin(), tuples.end(), cmp<key_tuple_t>);
  }

  {
    ScopedStopWatch stop_watch(timer->output_timer);
    vector<sort_tuple_t> output_tuples;
    output_tuples.reserve(num_rows);
    for (int i = 0; i < num_rows; ++i) {
      output_tuples.push_back(*tuples[i].sort_tuple);
    }
  }
}

// Puts the key and a pointer to the data into the actual vector.
template <typename sort_tuple_t>
void TestKeyOnlyImpala(TestData* data, Timer* timer, uint64_t num_rows) {
  BufferPool buffer_pool(512, 1024*1024*8);
  ObjectPool obj_pool;
  BlockMemPool block_mem_pool(&obj_pool, &buffer_pool);
  typedef SortTupleKeyPtr<sort_tuple_t> key_tuple_t;
  BlockedVector<key_tuple_t> tuples(&block_mem_pool, sizeof(key_tuple_t));

  {
    ScopedStopWatch stop_watch(timer->insert_timer);

    for (int i = 0; i < num_rows; ++i) {
      key_tuple_t* key_tuple = tuples.AllocateElement();
      sort_tuple_t* source_tuple = (sort_tuple_t*) data->source_data + i;
      memcpy(key_tuple, source_tuple, sizeof(source_tuple->key));
      key_tuple->sort_tuple = source_tuple;
    }
  }

  {
    ScopedStopWatch stop_watch(timer->sort_timer);
    SortUtil<key_tuple_t>::SortNormalized(tuples.Begin(), tuples.End(),
        sizeof(key_tuple_t), 0, MEMBER_SIZE(key_tuple_t, key));
  }

  {
    ScopedStopWatch stop_watch(timer->output_timer);
    vector<sort_tuple_t> output_tuples;
    output_tuples.reserve(num_rows);
    for (int i = 0; i < num_rows; ++i) {
      output_tuples.push_back(*tuples[i]->sort_tuple);
    }
  }

  // Perform verification that the sort worked...
  for (int i = 1; i < num_rows; ++i) {
    key_tuple_t* x = tuples[i-1];
    key_tuple_t* y = tuples[i];
    DCHECK_LE(memcmp(x->key, y->key, sizeof(x->key)), 0);
  }
}

static uint64_t SORT_MEM_LIMIT = 0; // set in constructor

struct SortedRunGenerator : public RowBatchSupplier {
  int64_t num_;
  int64_t num_rows_;
  boost::mt19937 gen_;
  int num_null_bytes_;
  int num_value_bytes_;

  SortedRunGenerator(TupleDescriptor* tuple_desc, int64_t num_rows)
      : num_(0), num_rows_(num_rows), num_null_bytes_(tuple_desc->num_null_bytes()),
        num_value_bytes_(tuple_desc->byte_size() - tuple_desc->num_null_bytes()) {
    DCHECK_EQ(num_value_bytes_ % 8, 0);
  }

  Status GetNext(RowBatch* batch, bool* eos) {
    uint8_t* tuples = batch->tuple_data_pool()->Allocate(
        batch->capacity() * (num_null_bytes_ + num_value_bytes_));

    batch->AddRows(batch->capacity());
    for (int i = 0; i < batch->capacity() && num_rows_ > 0; ++i, --num_rows_) {
      TupleRow* row = batch->GetRow(i);
      Tuple* tuple = reinterpret_cast<Tuple*>(
          tuples + i * (num_null_bytes_ + num_value_bytes_));
      num_ += gen_();
      memset(tuple, 0, num_null_bytes_);
      for (int i = 0; i < num_value_bytes_ ; i += 8) {
        int64_t* slot = reinterpret_cast<int64_t*>(
            tuple->GetSlot(num_null_bytes_ + i));
        *slot = num_;
      }
      row->SetTuple(0, tuple);
    }
    batch->CommitRows(batch->capacity());
    *eos = (num_rows_ == 0);
    return Status::OK;
  }
};

template <typename sort_tuple_t, size_t num_strings, size_t num_keys>
void TestMergeBandwidth(TestData* data, Timer* timer, uint64_t num_rows,
    DescriptorTbl* desc_tbl, int (&str_lens)[num_strings], int (&key_slots)[num_keys]) {
  TupleDescriptor* tuple_desc = desc_tbl->GetTupleDescriptor(0);
  RowDescriptor* row_desc = CreateRowDescriptor(desc_tbl);
  vector<SlotDescriptor*> slots = tuple_desc->slots();

  // Initialize the Sorter. First have to construct a bunch of state and arguments.
  vector<Expr*> output_slot_exprs;
  for (int i = 0; i < slots.size(); ++i) {
    output_slot_exprs.push_back(new OffsetSlotRef(tuple_desc, i));
  }

  vector<Expr*> sort_exprs_lhs;
  vector<Expr*> sort_exprs_rhs;
  vector<bool> sort_ascending;
  for (int i = 0; i < num_keys; ++i) {
    int key_slot_id = key_slots[i];
    sort_exprs_lhs.push_back(new OffsetSlotRef(tuple_desc, key_slot_id));
    sort_exprs_rhs.push_back(new OffsetSlotRef(tuple_desc, key_slot_id));
    sort_ascending.push_back(true);
  }

  bool nulls_first = true;
  bool remove_dups = false;
  scoped_ptr<SortedMerger> merger(new SortedMerger(*row_desc,
                                       sort_exprs_lhs, sort_exprs_rhs,
                                       sort_ascending, nulls_first, remove_dups,
                                       SORT_MEM_LIMIT));


  int num_runs = 10;
  for (int i = 0; i < num_runs; ++i) {
    SortedRunGenerator* gen = new SortedRunGenerator(tuple_desc, num_rows / num_runs);
    merger->AddRun(gen);
  }

  int batch_size = 10000;
  RowBatch* output_batch = CreateEmptyOutputBatch(row_desc, tuple_desc, batch_size);
  bool eos = false;
  long num_batches = 0;
  MonotonicStopWatch stop_watch;
  uint64_t num_merged_since = 0;
  while (!eos) {
    output_batch->Reset();
    stop_watch.Start();
    merger->GetNext(output_batch, &eos);
    stop_watch.Stop();
    ++num_batches;
    num_merged_since += batch_size;

    if (num_batches % 1000 == 0) {
      double seconds = (double) stop_watch.ElapsedTime() / 1000 / 1000 / 1000;
      double tuples_per_sec = ((double) num_merged_since) / seconds;
      double bytes_per_sec = tuples_per_sec * tuple_desc->byte_size();
      stop_watch = MonotonicStopWatch();
      printf("Merged [%5.1f%%] ->%4lu mil rows: <%5.1fk tup/s, %5.1f MB/s>\n",
             ((double) num_batches / num_rows * batch_size * 100),
             batch_size * num_batches / 1000 / 1000,
             tuples_per_sec / 1000,
             bytes_per_sec / 1000 /10000);
      num_merged_since = 0;
    }
  }
}

// Initializes and executes an Impala sort.
template <typename sort_tuple_t, size_t num_strings, size_t num_keys>
void TestImpala(TestData* data, Timer* timer, uint64_t num_rows, DescriptorTbl* desc_tbl,
    int (&str_lens)[num_strings], int (&key_slots)[num_keys]) {

  TupleDescriptor* tuple_desc = desc_tbl->GetTupleDescriptor(0);
  RowDescriptor* row_desc = CreateRowDescriptor(desc_tbl);
  vector<SlotDescriptor*> slots = tuple_desc->slots();

  // Initialize the Sorter. First have to construct a bunch of state and arguments.
  vector<Expr*> output_slot_exprs;
  for (int i = 0; i < slots.size(); ++i) {
    output_slot_exprs.push_back(new OffsetSlotRef(tuple_desc, i));
  }

  vector<Expr*> sort_exprs_lhs;
  vector<Expr*> sort_exprs_rhs;
  vector<bool> sort_ascending;
  int ideal_sort_key_size = num_keys /* NULL bytes */;
  int string_id = 0;
  for (int i = 0; i < num_keys; ++i) {
    int key_slot_id = key_slots[i];
    sort_exprs_lhs.push_back(new OffsetSlotRef(tuple_desc, key_slot_id));
    sort_exprs_rhs.push_back(new OffsetSlotRef(tuple_desc, key_slot_id));
    sort_ascending.push_back(true);

    SlotDescriptor* slot = tuple_desc->slots()[key_slot_id];
    if (slot->type() == TYPE_STRING) {
      ideal_sort_key_size += str_lens[string_id++] + 1 /* string terminator */;
    } else {
      ideal_sort_key_size += GetByteSize(slot->type());
    }
  }

  // Align to 4 byte boundary, upwards.
  ideal_sort_key_size += (4 - (ideal_sort_key_size % 4)) % 4;
  int sort_key_size =
      (data->norm_key_len == -1 ? ideal_sort_key_size : data->norm_key_len);

  DiskIoMgr io_mgr;
  DiskIoMgr::ReaderContext* reader;
  DiskWriter writer;
  writer.Init();
  Status status = io_mgr.Init();
  DCHECK(status.ok());
  status = io_mgr.RegisterReader(NULL, &reader);
  DCHECK(status.ok());

  ThreadResourceMgr resource_mgr;
  ThreadResourceMgr::ResourcePool* resource_pool = resource_mgr.RegisterPool();

  bool nulls_first = true;
  bool remove_dups = false;
  scoped_ptr<Sorter> sorter(new Sorter(&writer, &io_mgr, reader, resource_pool,*tuple_desc,
                                       output_slot_exprs, sort_exprs_lhs, sort_exprs_rhs,
                                       sort_ascending, nulls_first, remove_dups,
                                       sort_key_size, SORT_MEM_LIMIT, 8 * 1024 * 1024, EXTRACT_KEYS));

  // Prepare a RowBatch to insert into the Sorter.
  uint8_t* source_data = (uint8_t*) data->source_data;
  RowBatch* batch = CreateRowBatch(row_desc, num_rows);
  for (int i = 0; i < num_rows; ++i) {
    TupleRow* row = batch->GetRow(i);
    int string_id = 0;
    for (int slot_id = 0; slot_id < slots.size(); ++slot_id) {
      SlotDescriptor* slot = slots[slot_id];

      if (slot->type() == TYPE_STRING) {
        int str_len = str_lens[string_id++];
        char* str_mem = (char*) batch->tuple_data_pool()->Allocate(str_len);
        memcpy(str_mem, source_data, str_len);
        source_data += str_len;
        StringValue str(str_mem, str_len);
        *((StringValue*) row->GetTuple(0)->GetSlot(slot->tuple_offset())) = str;
      } else {
        int byte_size = GetByteSize(slot->type());
        memcpy(row->GetTuple(0)->GetSlot(slot->tuple_offset()), source_data, byte_size);
        source_data += byte_size;
      }
    }
  }

  {
    ScopedStopWatch stop_watch(timer->insert_timer);
    sorter->AddBatch(batch);
  }
  free(batch);

  // Get first row, to make it sort.
  RowBatch* output_batch = CreateEmptyOutputBatch(row_desc, tuple_desc, 1);
  bool eos = false;
  {
    ScopedStopWatch stop_watch(timer->sort_timer);
    sorter->GetNext(output_batch, &eos);
  }
  DCHECK(!eos);
  free(output_batch);

  // Ask for all remaining rows (instead of just 1).
  output_batch = CreateEmptyOutputBatch(row_desc, tuple_desc, 1024);
  eos = false;
  {
    ScopedStopWatch stop_watch(timer->output_timer);
    while (!eos) {
      output_batch->Reset();
      sorter->GetNext(output_batch, &eos);
    }
  }
  DCHECK(eos);
  free(output_batch);

  writer.Cancel();
  io_mgr.UnregisterReader(reader);

  sorter->profile()->PrettyPrint(&cout);

  for (int i = 0; i < output_slot_exprs.size(); ++i) delete output_slot_exprs[i];
  for (int i = 0; i < sort_exprs_lhs.size(); ++i) delete sort_exprs_lhs[i];
  for (int i = 0; i < sort_exprs_rhs.size(); ++i) delete sort_exprs_rhs[i];
}

// Runs and prints results from all tests.
template <typename sort_tuple_t, size_t num_strings, size_t num_keys>
void RunTests(string test_name, uint64_t num_rows, DescriptorTbl* desc_tbl,
    int (&str_lens)[num_strings], int (&key_slots)[num_keys]) {
  TestData data;
  cout << endl << "===== " << test_name << " =====" << endl;
  cout << "Key size: " << MEMBER_SIZE(sort_tuple_t, key) << " bytes" << endl;
  cout << "Payload size: " << MEMBER_SIZE(sort_tuple_t, payload) << " bytes" << endl;
  cout << "Num rows: " << num_rows/1000 << "k" << endl;
  cout << "Essential data size: " << (num_rows*sizeof(sort_tuple_t)/1024/1024)
       << " MB" << endl;

  vector<pair<string, Timer> > benchmarks;
  InitTestData<sort_tuple_t>(&data, num_rows);

  Timer indirect_timer;
  TestIndirectSTL<sort_tuple_t>(&data, &indirect_timer, num_rows);
  benchmarks.push_back(pair<string,Timer>("Indirect STL", indirect_timer));

  Timer flat_timer;
  TestFlatSTL<sort_tuple_t>(&data, &flat_timer, num_rows);
  benchmarks.push_back(pair<string,Timer>("Flat STL", flat_timer));

  Timer flat_impala_timer;
  TestFlatImpala<sort_tuple_t>(&data, &flat_impala_timer, num_rows);
  benchmarks.push_back(pair<string,Timer>("Flat Impala", flat_impala_timer));

  Timer key_only_timer;
  TestKeyOnlySTL<sort_tuple_t>(&data, &key_only_timer, num_rows);
  benchmarks.push_back(pair<string,Timer>("Key-Only STL", key_only_timer));

  Timer key_only_impala_timer;
  TestKeyOnlyImpala<sort_tuple_t>(&data, &key_only_impala_timer, num_rows);
  benchmarks.push_back(pair<string,Timer>("Key-Only Impala", key_only_impala_timer));

  Timer impala_timer;
  data.norm_key_len = -1;
  TestImpala<sort_tuple_t>(&data, &impala_timer, num_rows, desc_tbl, str_lens, key_slots);
  benchmarks.push_back(pair<string,Timer>("Full key", impala_timer));

  Timer impala_timer2;
  data.norm_key_len = 0;
  TestImpala<sort_tuple_t>(&data, &impala_timer2, num_rows, desc_tbl, str_lens, key_slots);
  benchmarks.push_back(pair<string,Timer>("No key", impala_timer2));

  //TestMergeBandwidth<sort_tuple_t>(&data, &impala_timer, num_rows, desc_tbl,
  //                                 str_lens, key_slots);

  stringstream ss;

  int function_width = 15;
  int insert_width = 12;
  int sort_width = 12;
  int output_width = 12;
  int total_width = 12;
  int comparison_width = 12;
  int full_width = function_width + insert_width + sort_width + output_width + total_width
      + comparison_width;

  ss << setw(function_width) << "Function"
     << setw(insert_width) << "Insert (s)"
     << setw(sort_width) << "Sort (s)"
     << setw(output_width) << "Output (s)"
     << setw(total_width) << "Total (s)"
     << setw(comparison_width) << "Comparison" << endl;
  for (int i = 0; i < full_width; ++i) {
    ss << '-';
  }
  ss << endl;

  double comparison_time = 0;
  for (int i = 0; i < benchmarks.size(); ++i) {
    string name = benchmarks[i].first;
    Timer timer = benchmarks[i].second;

    double insert_time = timer.insert_timer.ElapsedTime() / 1000000000.0;
    double sort_time   = timer.sort_timer.ElapsedTime()   / 1000000000.0;
    double output_time = timer.output_timer.ElapsedTime() / 1000000000.0;
    double total_time  = insert_time + sort_time + output_time;
    if (comparison_time == 0) comparison_time = total_time;

    ss << setw(function_width) << name
       << setw(insert_width) << setprecision(4) << insert_time
       << setw(sort_width)   << setprecision(4) << sort_time
       << setw(output_width) << setprecision(4) << output_time
       << setw(total_width) << setprecision(4) << total_time
       << setw(comparison_width - 1) << setprecision(4)
       << (total_time / comparison_time) << "X" << endl;
  }

  cout << ss.str();
}

double TimerValue(MonotonicStopWatch timer) {
  return timer.ElapsedTime() / 1000.0 / 1000;
}

// TPCH-1: Small key, large payload
void TestTpch1(uint64_t num_rows) {
  const int KEY_SIZE = 2;
  const int PAYLOAD_SIZE = 60;
  typedef SortTuple<KEY_SIZE, PAYLOAD_SIZE> sort_tuple_t;
  DescriptorTblBuilder builder(&obj_pool_);
  builder.DeclareTuple() << TYPE_STRING << TYPE_STRING << TYPE_BIGINT << TYPE_BIGINT
                         << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT
                         << TYPE_INT;
  DescriptorTbl* desc_tbl = builder.Build();
  int str_lens[] = {1, 1};
  int key_slots[] = {0, 1};
  RunTests<sort_tuple_t>("TPCH-1", num_rows, desc_tbl, str_lens, key_slots);
}

// TPCH-3: Medium key, medium payload
void TestTpch3(uint64_t num_rows) {
  const int KEY_SIZE = 24;
  const int PAYLOAD_SIZE = 12;
  typedef SortTuple<KEY_SIZE, PAYLOAD_SIZE> sort_tuple_t;
  DescriptorTblBuilder builder(&obj_pool_);
  builder.DeclareTuple() << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT
                         << TYPE_INT;
  DescriptorTbl* desc_tbl = builder.Build();
  int str_lens[] = {-1}; // avoid irritating C problem with 0-length arrays
  int key_slots[] = {1, 2, 3};
  RunTests<sort_tuple_t>("TPCH-3", num_rows, desc_tbl, str_lens, key_slots);
}

// TPCH-16: Large key, no payload
void TestTpch16(uint64_t num_rows) {
  const int KEY_SIZE = 47;
  const int PAYLOAD_SIZE = 0;
  typedef SortTuple<KEY_SIZE, PAYLOAD_SIZE> sort_tuple_t;
  DescriptorTblBuilder builder(&obj_pool_);
  builder.DeclareTuple() << TYPE_STRING << TYPE_STRING << TYPE_BIGINT << TYPE_INT;
  DescriptorTbl* desc_tbl = builder.Build();
  int str_lens[] = {10, 25};
  int key_slots[] = {3, 0, 1, 2};
  RunTests<sort_tuple_t>("TPCH-16", num_rows, desc_tbl, str_lens, key_slots);
}

// KEY-10%: Key is 10% of the total payload.
void TestKey10Percent(uint64_t num_rows) {
  const int KEY_SIZE = 4;
  const int PAYLOAD_SIZE = 36;
  typedef SortTuple<KEY_SIZE, PAYLOAD_SIZE> sort_tuple_t;
  DescriptorTblBuilder builder(&obj_pool_);
  builder.DeclareTuple() << TYPE_INT << TYPE_BIGINT << TYPE_BIGINT
                         << TYPE_DOUBLE << TYPE_DOUBLE;
  DescriptorTbl* desc_tbl = builder.Build();
  int str_lens[] = {-1};
  int key_slots[] = {0};
  RunTests<sort_tuple_t>("KEY-10%", num_rows, desc_tbl, str_lens, key_slots);
}

// Testing ground for benchmarking external
void TestExternal(uint64_t num_rows) {
  const int KEY_SIZE = 24;
  const int PAYLOAD_SIZE = 24;
  typedef SortTuple<KEY_SIZE, PAYLOAD_SIZE> sort_tuple_t;
  DescriptorTblBuilder builder(&obj_pool_);
  builder.DeclareTuple() << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT
                         << TYPE_DOUBLE << TYPE_DOUBLE << TYPE_DOUBLE;
  DescriptorTbl* desc_tbl = builder.Build();
  int str_lens[] = {-1};
  int key_slots[] = {0, 1, 2};
  RunTests<sort_tuple_t>("EXT", num_rows, desc_tbl, str_lens, key_slots);
}

void TestStringy(uint64_t num_rows) {
  const int KEY_SIZE = 16;
  const int PAYLOAD_SIZE = 32;
  typedef SortTuple<KEY_SIZE, PAYLOAD_SIZE> sort_tuple_t;
  DescriptorTblBuilder builder(&obj_pool_);
  builder.DeclareTuple() << TYPE_STRING << TYPE_BIGINT << TYPE_BIGINT
      << TYPE_BIGINT << TYPE_BIGINT;
  DescriptorTbl* desc_tbl = builder.Build();
  int str_lens[] = {16};
  int key_slots[] = {0};
  RunTests<sort_tuple_t>("STR", num_rows, desc_tbl, str_lens, key_slots);
}

void TestBandwidth(uint64_t num_rows) {
  const int KEY_SIZE = 24;
  const int PAYLOAD_SIZE = 16 + 8*3*3;
  typedef SortTuple<KEY_SIZE, PAYLOAD_SIZE> sort_tuple_t;
  DescriptorTblBuilder builder(&obj_pool_);
  builder.DeclareTuple() << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT
      << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT
      << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT
      << TYPE_BIGINT << TYPE_BIGINT << TYPE_BIGINT
      << TYPE_BIGINT << TYPE_BIGINT;

  DescriptorTbl* desc_tbl = builder.Build();
  int str_lens[] = {-1}; // avoid irritating C problem with 0-length arrays
  int key_slots[] = {1, 2, 3};
  RunTests<sort_tuple_t>("BAND", num_rows, desc_tbl, str_lens, key_slots);
}

// args:
// 1 -> test-name
// 2 -> mem-limit (MB)
// 3 -> extract_keys
int main(int argc, char **argv) {
  CpuInfo::Init();
  DiskInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  if (argc != 4) {
    LOG(ERROR) << "Arg count == " << argc << ", aborting";
    return 0;
  }

  const char* test_name = argv[1];
  long mem_limit = atol(argv[2]) * 1024 * 1024;
  bool extract_keys = (atoi(argv[3]) == 1);

  printf("Mem limit: %lu\n", mem_limit / 1024 / 1024);
  printf("Extract keys: %d\n", extract_keys);

  SORT_MEM_LIMIT = mem_limit;
  EXTRACT_KEYS = extract_keys;
  uint64_t num_rows = 20000000;

  if (strcmp(test_name, "tpch1") == 0) {
    TestTpch1(num_rows);
  } else if (strcmp(test_name, "tpch3") == 0) {
    TestTpch3(num_rows);
  } else if (strcmp(test_name, "tpch16") == 0) {
    TestTpch16(num_rows);
  } else if (strcmp(test_name, "key10%") == 0) {
    TestKey10Percent(num_rows);
  } else if (strcmp(test_name, "equal") == 0) {
    TestExternal(num_rows);
  } else if (strcmp(test_name, "stringy") == 0) {
    TestStringy(num_rows);
  } else if (strcmp(test_name, "bandwidth") == 0) {
    TestBandwidth(num_rows);
  }

  return 0;
}
