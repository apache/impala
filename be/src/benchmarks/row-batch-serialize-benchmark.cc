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

#include <iostream>
#include <sstream>
#include <boost/scoped_ptr.hpp>

#include "common/init.h"
#include "runtime/mem-tracker.h"
#include "runtime/outbound-row-batch.h"
#include "runtime/raw-value.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "service/fe-support.h"
#include "service/frontend.h"
#include "testutil/desc-tbl-builder.h"
#include "util/benchmark.h"
#include "util/compress.h"
#include "util/cpu-info.h"
#include "util/decompress.h"
#include "util/scope-exit-trigger.h"

#include "common/names.h"

// Benchmark to measure how quickly we can serialize and deserialize row batches. More
// specifically, this benchmark was developed to measure the overhead of deduplication.
// The benchmarks are divided into serialization and deserialization benchmarks.
// The serialization benchmarks test different serialization methods (the new default of
// adjacent deduplication vs. the baseline of no deduplication) on row batches with
// different patterns of duplication: no_dups and adjacent_dups.
// For all benchmarks we use (int, string) tuples to exercise both variable-length and
// fixed-length slot handling. The small tuples with few slots emphasizes per-tuple
// dedup performance rather than per-slot serialization/deserialization performance.
//
// Machine Info: Intel(R) Xeon(R) Platinum 8375C CPU @ 2.90GHz
// serialize:            10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                (relative) (relative) (relative)
// -------------------------------------------------------------------------------
//    ser_no_dups_base     18.6     18.8     18.9         1X         1X         1X
//         ser_no_dups     18.5     18.5     18.8     0.998X     0.988X     0.991X
//    ser_no_dups_full     14.7     14.8     14.8     0.793X      0.79X     0.783X
//
//   ser_adj_dups_base     28.2     28.4     28.8         1X         1X         1X
//        ser_adj_dups     68.9     69.1     69.8      2.44X      2.43X      2.43X
//   ser_adj_dups_full     56.2     56.7     57.1      1.99X         2X      1.99X
//
//       ser_dups_base     20.7     20.9     20.9         1X         1X         1X
//            ser_dups     20.6     20.8     20.9     0.994X     0.995X         1X
//       ser_dups_full     39.8       40     40.5      1.93X      1.92X      1.94X
//
// deserialize:          10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                (relative) (relative) (relative)
// -------------------------------------------------------------------------------
//  deser_no_dups_base     75.9     76.6       77         1X         1X         1X
//       deser_no_dups     74.9     75.6       76     0.987X     0.987X     0.987X
//
// deser_adj_dups_base      127      128      129         1X         1X         1X
//      deser_adj_dups      179      193      195      1.41X      1.51X      1.51X
//
//     deser_dups_base      128      128      129         1X         1X         1X
//          deser_dups      165      190      193      1.29X      1.48X      1.49X

using namespace impala;

const int NUM_ROWS = 1024;
const int MAX_STRING_LEN = 10;

namespace impala {

// For computing tuple mem layouts.
static scoped_ptr<Frontend> fe;
static std::shared_ptr<CharMemTrackerAllocator> char_mem_tracker_allocator;

// Friend class with access to RowBatch internals
class RowBatchSerializeBaseline {
 public:
  // Copy of baseline version without dedup logic
  static int Serialize(RowBatch* batch, OutboundRowBatch* output_batch,
      TrackedString* compression_scratch) {
    RowBatchHeaderPB* header = &output_batch->header_;
    output_batch->tuple_offsets_.clear();
    header->set_num_rows(batch->num_rows_);
    header->set_compression_type(CompressionTypePB::NONE);
    output_batch->tuple_offsets_.reserve(batch->num_rows_ * batch->num_tuples_per_row_);

    int64_t size = TotalByteSize(batch);

    header->set_uncompressed_size(size);

    output_batch->tuple_data_.resize(size);

    SerializeInternal(batch, size, output_batch);

    if (size > 0) {
      // Try compressing tuple_data to compression_scratch, swap if compressed data is
      // smaller
      Lz4Compressor compressor(nullptr, false);
      Status status = compressor.Init();
      DCHECK(status.ok()) << status.GetDetail();
      auto compressor_cleanup =
          MakeScopeExitTrigger([&compressor]() { compressor.Close(); });

      int64_t compressed_size = compressor.MaxOutputLen(size);
      DCHECK_GT(compressed_size, 0);
      if (compression_scratch->size() < compressed_size) {
        compression_scratch->resize(compressed_size);
      }

      uint8_t* input = const_cast<uint8_t*>(
          reinterpret_cast<const uint8_t*>(output_batch->tuple_data_.data()));
      uint8_t* compressed_output = const_cast<uint8_t*>(
          reinterpret_cast<const uint8_t*>(compression_scratch->data()));
      status = compressor.ProcessBlock(
          true, size, input, &compressed_size, &compressed_output);
      DCHECK(status.ok()) << status.GetDetail();
      if (LIKELY(compressed_size < size)) {
        compression_scratch->resize(compressed_size);
        output_batch->tuple_data_.swap(*compression_scratch);
      }
      VLOG_ROW << "uncompressed size: " << size
               << ", compressed size: " << compressed_size;
    }

    return RowBatch::GetDeserializedSize(*output_batch);
  }

  // Copy of baseline version without dedup logic
  static void SerializeInternal(
      RowBatch* batch, int64_t size, OutboundRowBatch* output_batch) {
    DCHECK_LE(size, output_batch->tuple_data_.max_size());
    output_batch->tuple_data_.resize(size);

    // Copy tuple data of unique tuples, including strings, into output_batch (converting
    // string pointers into offsets in the process).
    int offset = 0; // current offset into output_batch->tuple_data
    char* tuple_data = const_cast<char*>(output_batch->tuple_data_.data());

    for (int i = 0; i < batch->num_rows_; ++i) {
      vector<TupleDescriptor*>::const_iterator desc =
          batch->row_desc_->tuple_descriptors().begin();
      for (int j = 0; desc != batch->row_desc_->tuple_descriptors().end(); ++desc, ++j) {
        Tuple* tuple = batch->GetRow(i)->GetTuple(j);
        if (tuple == NULL) {
          // NULLs are encoded as -1
          output_batch->tuple_offsets_.push_back(-1);
          continue;
        }
        // Record offset before creating copy (which increments offset and tuple_data)
        output_batch->tuple_offsets_.push_back(offset);
        tuple->DeepCopy(**desc, &tuple_data, &offset, /* convert_ptrs */ true);
        DCHECK_LE(offset, size);
      }
    }
    DCHECK_EQ(offset, size);
  }

  // Copy of baseline version without dedup logic
  static int64_t TotalByteSize(RowBatch* batch) {
    int64_t result = 0;
    for (int i = 0; i < batch->num_rows_; ++i) {
      for (int j = 0; j < batch->num_tuples_per_row_; ++j) {
        Tuple* tuple = batch->GetRow(i)->GetTuple(j);
        if (tuple == NULL) continue;
        result += tuple->TotalByteSize(*batch->row_desc_->tuple_descriptors()[j]);
      }
    }
    return result;
  }

  // Copy of baseline version without dedup logic
  static void Deserialize(RowBatch* batch, const OutboundRowBatch& input_batch) {
    batch->num_rows_ = input_batch.header()->num_rows();
    batch->capacity_ = batch->num_rows_;
    uint8_t* tuple_data;
    if (input_batch.header()->compression_type() != CompressionTypePB::NONE) {
      // Decompress tuple data into data pool
      uint8_t* compressed_data = const_cast<uint8_t*>(
          reinterpret_cast<const uint8_t*>(input_batch.tuple_data_.data()));
      size_t compressed_size = input_batch.tuple_data_.size();

      Lz4Decompressor decompressor(nullptr, false);
      Status status = decompressor.Init();
      DCHECK(status.ok()) << status.GetDetail();
      auto compressor_cleanup =
          MakeScopeExitTrigger([&decompressor]() { decompressor.Close(); });

      int64_t uncompressed_size = input_batch.header()->uncompressed_size();
      DCHECK_NE(uncompressed_size, -1) << "RowBatch decompression failed";
      tuple_data = batch->tuple_data_pool()->Allocate(uncompressed_size);
      status = decompressor.ProcessBlock(
          true, compressed_size, compressed_data, &uncompressed_size, &tuple_data);
      DCHECK(status.ok()) << "RowBatch decompression failed.";
    } else {
      // Tuple data uncompressed, copy directly into data pool
      tuple_data = batch->tuple_data_pool()->Allocate(input_batch.tuple_data_.size());
      memcpy(tuple_data, input_batch.tuple_data_.data(), input_batch.tuple_data_.size());
    }

    // Convert input_batch.tuple_offsets into pointers
    int tuple_idx = 0;
    for (vector<int32_t>::const_iterator offset = input_batch.tuple_offsets_.begin();
         offset != input_batch.tuple_offsets_.end(); ++offset) { ///
      if (*offset == -1) {
        batch->tuple_ptrs_[tuple_idx++] = NULL;
      } else {
        batch->tuple_ptrs_[tuple_idx++] = reinterpret_cast<Tuple*>(tuple_data + *offset);
      }
    }

    // Check whether we have slots that require offset-to-pointer conversion.
    if (!batch->row_desc_->HasVarlenSlots()) return;

    for (int i = 0; i < batch->num_rows_; ++i) {
      for (int j = 0; j < batch->num_tuples_per_row_; ++j) {
        const TupleDescriptor* desc = batch->row_desc_->tuple_descriptors()[j];
        if (!desc->HasVarlenSlots()) continue;
        Tuple* tuple = batch->GetRow(i)->GetTuple(j);
        if (tuple == NULL) continue;
        tuple->ConvertOffsetsToPointers(*desc, tuple_data);
      }
    }
  }
};

class RowBatchSerializeBenchmark {
 public:
  // Fill batch with (int, string) tuples with random data.
  static void FillBatch(RowBatch* batch, int rand_seed, int repeats, int cycle) {
    srand(rand_seed);
    if (cycle <= 0) cycle = NUM_ROWS; // Negative means no repeats in cycle.
    MemPool* mem_pool = batch->tuple_data_pool();
    const TupleDescriptor* tuple_desc = batch->row_desc()->tuple_descriptors()[0];
    int unique_tuples = (NUM_ROWS - 1) / repeats + 1;
    uint8_t* tuple_mem = mem_pool->Allocate(tuple_desc->byte_size() * unique_tuples);
    for (int i = 0; i < NUM_ROWS; ++i) {
      int row_idx = batch->AddRow();
      TupleRow* row = batch->GetRow(row_idx);
      Tuple* tuple;
      if (i >= cycle) {
        // Duplicate of tuple from previous cycle.
        tuple = batch->GetRow(i - cycle)->GetTuple(0);
      } else if (i % repeats == 0) {
        // Generate new unique tuple.
        tuple = reinterpret_cast<Tuple*>(tuple_mem);
        tuple->Init(tuple_desc->byte_size());
        tuple_mem += tuple_desc->byte_size();
        int int_val = rand();
        RawValue::Write(&int_val, tuple, tuple_desc->slots()[0], mem_pool);
        char string_buf[MAX_STRING_LEN + 1];
        int string_len = rand() % MAX_STRING_LEN;
        for (int j = 0; j < string_len; ++j) {
          string_buf[j] = (char)rand() % 256;
        }
        StringValue string_val(string_buf, string_len);
        RawValue::Write(&string_val, tuple, tuple_desc->slots()[1], mem_pool);
      } else {
        // Duplicate of previous.
        tuple = batch->GetRow(i - 1)->GetTuple(0);
      }
      row->SetTuple(0, tuple);
      batch->CommitLastRow();
    }
  }

  struct SerializeArgs {
    RowBatch* batch;
    bool full_dedup;
  };

  static void TestSerialize(int batch_size, void* data) {
    SerializeArgs* args = reinterpret_cast<SerializeArgs*>(data);
    for (int iter = 0; iter < batch_size; ++iter) {
      OutboundRowBatch row_batch(char_mem_tracker_allocator);
      TrackedString compression_scratch(*char_mem_tracker_allocator.get());
      ABORT_IF_ERROR(args->batch->Serialize(&row_batch, args->full_dedup,
          &compression_scratch));
    }
  }

  static void TestSerializeBaseline(int batch_size, void* data) {
    RowBatch* batch = reinterpret_cast<RowBatch*>(data);
    for (int iter = 0; iter < batch_size; ++iter) {
      OutboundRowBatch row_batch(char_mem_tracker_allocator);
      TrackedString compression_scratch(*char_mem_tracker_allocator.get());
      RowBatchSerializeBaseline::Serialize(batch, &row_batch, &compression_scratch);
    }
  }

  struct DeserializeArgs {
    OutboundRowBatch* row_batch;
    RowDescriptor* row_desc;
    MemTracker* tracker;
  };

  static void TestDeserialize(int batch_size, void* data) {
    struct DeserializeArgs* args = reinterpret_cast<struct DeserializeArgs*>(data);
    for (int iter = 0; iter < batch_size; ++iter) {
      RowBatch deserialized_batch(args->row_desc, *args->row_batch, args->tracker);
    }
  }

  static void TestDeserializeBaseline(int batch_size, void* data) {
    struct DeserializeArgs* args = reinterpret_cast<struct DeserializeArgs*>(data);
    for (int iter = 0; iter < batch_size; ++iter) {
      RowBatch deserialized_batch(
          args->row_desc, args->row_batch->header()->num_rows(), args->tracker);
      RowBatchSerializeBaseline::Deserialize(&deserialized_batch, *args->row_batch);
    }
  }

  static void Run() {
    cout << Benchmark::GetMachineInfo() << endl;

    char_mem_tracker_allocator.reset(
        new CharMemTrackerAllocator(std::make_shared<MemTracker>()));
    shared_ptr<MemTracker> tracker = char_mem_tracker_allocator->mem_tracker();
    MemPool mem_pool(tracker.get());
    ObjectPool obj_pool;
    DescriptorTblBuilder builder(fe.get(), &obj_pool);
    builder.DeclareTuple() << TYPE_INT << TYPE_STRING;
    DescriptorTbl* desc_tbl = builder.Build();

    vector<bool> nullable_tuples(1, false);
    vector<TTupleId> tuple_id(1, (TTupleId)0);
    RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);

    RowBatch* no_dup_batch =
        obj_pool.Add(new RowBatch(&row_desc, NUM_ROWS, tracker.get()));
    FillBatch(no_dup_batch, 12345, 1, -1);
    TrackedString compression_scratch(*char_mem_tracker_allocator.get());

    OutboundRowBatch no_dup_row_batch(char_mem_tracker_allocator);
    ABORT_IF_ERROR(no_dup_batch->Serialize(&no_dup_row_batch, &compression_scratch));

    RowBatch* adjacent_dup_batch =
        obj_pool.Add(new RowBatch(&row_desc, NUM_ROWS, tracker.get()));
    FillBatch(adjacent_dup_batch, 12345, 5, -1);
    OutboundRowBatch adjacent_dup_row_batch(char_mem_tracker_allocator);
    ABORT_IF_ERROR(adjacent_dup_batch->Serialize(&adjacent_dup_row_batch,
        false, &compression_scratch));

    RowBatch* dup_batch = obj_pool.Add(new RowBatch(&row_desc, NUM_ROWS, tracker.get()));
    // Non-adjacent duplicates.
    FillBatch(dup_batch, 12345, 1, NUM_ROWS / 5);
    OutboundRowBatch dup_row_batch(char_mem_tracker_allocator);
    ABORT_IF_ERROR(dup_batch->Serialize(&dup_row_batch, true, &compression_scratch));

    int baseline;
    Benchmark ser_suite("serialize");
    baseline = ser_suite.AddBenchmark(
        "ser_no_dups_base", TestSerializeBaseline, no_dup_batch, -1);
    struct SerializeArgs no_dup_ser_args = {no_dup_batch, false};
    struct SerializeArgs no_dup_ser_full_args = {no_dup_batch, true};
    ser_suite.AddBenchmark("ser_no_dups", TestSerialize, &no_dup_ser_args, baseline);
    ser_suite.AddBenchmark(
        "ser_no_dups_full", TestSerialize, &no_dup_ser_full_args, baseline);

    baseline = ser_suite.AddBenchmark(
        "ser_adj_dups_base", TestSerializeBaseline, adjacent_dup_batch, -1);
    struct SerializeArgs adjacent_dup_ser_args = {adjacent_dup_batch, false};
    struct SerializeArgs adjacent_dup_ser_full_args = {adjacent_dup_batch, true};
    ser_suite.AddBenchmark(
        "ser_adj_dups", TestSerialize, &adjacent_dup_ser_args, baseline);
    ser_suite.AddBenchmark(
        "ser_adj_dups_full", TestSerialize, &adjacent_dup_ser_full_args, baseline);

    baseline =
        ser_suite.AddBenchmark("ser_dups_base", TestSerializeBaseline, dup_batch, -1);
    struct SerializeArgs dup_ser_args = {dup_batch, false};
    struct SerializeArgs dup_ser_full_args = {dup_batch, true};
    ser_suite.AddBenchmark("ser_dups", TestSerialize, &dup_ser_args, baseline);
    ser_suite.AddBenchmark("ser_dups_full", TestSerialize, &dup_ser_full_args, baseline);

    cout << ser_suite.Measure() << endl;

    Benchmark deser_suite("deserialize");
    struct DeserializeArgs no_dup_deser_args = {
        &no_dup_row_batch, &row_desc, tracker.get()};
    baseline = deser_suite.AddBenchmark(
        "deser_no_dups_base", TestDeserializeBaseline, &no_dup_deser_args, -1);
    deser_suite.AddBenchmark(
        "deser_no_dups", TestDeserialize, &no_dup_deser_args, baseline);

    struct DeserializeArgs adjacent_dup_deser_args = {
        &adjacent_dup_row_batch, &row_desc, tracker.get()};
    baseline = deser_suite.AddBenchmark(
        "deser_adj_dups_base", TestDeserializeBaseline, &adjacent_dup_deser_args, -1);
    deser_suite.AddBenchmark(
        "deser_adj_dups", TestDeserialize, &adjacent_dup_deser_args, baseline);

    struct DeserializeArgs dup_deser_args = {&dup_row_batch, &row_desc, tracker.get()};
    baseline = deser_suite.AddBenchmark(
        "deser_dups_base", TestDeserializeBaseline, &dup_deser_args, -1);
    deser_suite.AddBenchmark("deser_dups", TestDeserialize, &dup_deser_args, baseline);

    cout << deser_suite.Measure() << endl;
    mem_pool.FreeAll();
  }
};

} // namespace impala

int main(int argc, char** argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  InitFeSupport();
  fe.reset(new Frontend());
  RowBatchSerializeBenchmark::Run();
  return 0;
}
