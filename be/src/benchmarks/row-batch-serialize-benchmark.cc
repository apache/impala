// Copyright 2015 Cloudera Inc.
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

#include <iostream>
#include <sstream>

#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "testutil/desc-tbl-builder.h"
#include "util/benchmark.h"
#include "util/compress.h"
#include "util/cpu-info.h"
#include "util/decompress.h"

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
// serialize:            Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//          ser_no_dups_baseline               17.43                  1X
//                   ser_no_dups               17.33             0.9944X
//              ser_no_dups_full                14.1             0.8092X
//
//    ser_adjacent_dups_baseline               26.65                  1X
//             ser_adjacent_dups               63.98                2.4X
//        ser_adjacent_dups_full               55.88              2.096X
//
//             ser_dups_baseline               19.26                  1X
//                      ser_dups               19.55              1.015X
//                 ser_dups_full                32.4              1.682X
//
// deserialize:          Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//        deser_no_dups_baseline               64.94                  1X
//                 deser_no_dups               69.24              1.066X
//
//  deser_adjacent_dups_baseline                 112                  1X
//           deser_adjacent_dups               207.4              1.852X
//
//           deser_dups_baseline               114.8                  1X
//                    deser_dups               208.5              1.817X
//
// Earlier results with LossyHashTable
// serialize:            Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//             ser_no_dups_lossy               15.93             0.9139X
//       ser_adjacent_dups_lossy               58.21              2.184X
//                ser_dups_lossy               50.46               2.62X
//
// Earlier results with boost::unordered_map
// serialize:            Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//              ser_no_dups_full                8.73             0.5582X
//
//        ser_adjacent_dups_full                38.7              1.634X
//
//                 ser_dups_full                27.5               1.54X
//

using namespace impala;

const int NUM_ROWS = 1024;
const int MAX_STRING_LEN = 10;

namespace impala {
// Friend class with access to RowBatch internals
class RowBatchSerializeBaseline {
 public:
   // Copy of baseline version without dedup logic
  static int Serialize(RowBatch* batch, TRowBatch* output_batch) {
    output_batch->row_tuples.clear();
    output_batch->tuple_offsets.clear();
    output_batch->compression_type = THdfsCompression::NONE;

    output_batch->num_rows = batch->num_rows_;
    batch->row_desc_.ToThrift(&output_batch->row_tuples);
    output_batch->tuple_offsets.reserve(batch->num_rows_ * batch->num_tuples_per_row_);

    int64_t size = TotalByteSize(batch);
    SerializeInternal(batch, size, output_batch);

    if (size > 0) {
      // Try compressing tuple_data to compression_scratch_, swap if compressed data is
      // smaller
      scoped_ptr<Codec> compressor;
      Status status = Codec::CreateCompressor(NULL, false, THdfsCompression::LZ4,
                                              &compressor);
      DCHECK(status.ok()) << status.GetDetail();

      int64_t compressed_size = compressor->MaxOutputLen(size);
      if (batch->compression_scratch_.size() < compressed_size) {
        batch->compression_scratch_.resize(compressed_size);
      }
      uint8_t* input = (uint8_t*)output_batch->tuple_data.c_str();
      uint8_t* compressed_output = (uint8_t*)batch->compression_scratch_.c_str();
      compressor->ProcessBlock(true, size, input, &compressed_size, &compressed_output);
      if (LIKELY(compressed_size < size)) {
        batch->compression_scratch_.resize(compressed_size);
        output_batch->tuple_data.swap(batch->compression_scratch_);
        output_batch->compression_type = THdfsCompression::LZ4;
      }
      VLOG_ROW << "uncompressed size: " << size << ", compressed size: " << compressed_size;
    }

    // The size output_batch would be if we didn't compress tuple_data (will be equal to
    // actual batch size if tuple_data isn't compressed)
    return batch->GetBatchSize(*output_batch) - output_batch->tuple_data.size() + size;
  }

  // Copy of baseline version without dedup logic
  static void SerializeInternal(RowBatch* batch, int64_t size, TRowBatch* output_batch) {
    DCHECK_LE(size, output_batch->tuple_data.max_size());
    output_batch->tuple_data.resize(size);
    output_batch->uncompressed_size = size;

    // Copy tuple data of unique tuples, including strings, into output_batch (converting
    // string pointers into offsets in the process).
    int offset = 0; // current offset into output_batch->tuple_data
    char* tuple_data = const_cast<char*>(output_batch->tuple_data.c_str());

    for (int i = 0; i < batch->num_rows_; ++i) {
       vector<TupleDescriptor*>::const_iterator desc =
         batch->row_desc_.tuple_descriptors().begin();
      for (int j = 0; desc != batch->row_desc_.tuple_descriptors().end(); ++desc, ++j) {
        Tuple* tuple = batch->GetRow(i)->GetTuple(j);
        if (tuple == NULL) {
          // NULLs are encoded as -1
          output_batch->tuple_offsets.push_back(-1);
          continue;
        }
        // Record offset before creating copy (which increments offset and tuple_data)
        output_batch->tuple_offsets.push_back(offset);
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
        result += tuple->TotalByteSize(*batch->row_desc_.tuple_descriptors()[j]);
      }
    }
    return result;
  }

  // Copy of baseline version without dedup logic
  static void Deserialize(RowBatch* batch, const TRowBatch& input_batch) {
     batch->num_rows_ = input_batch.num_rows;
     batch->capacity_ =  batch->num_rows_;
    uint8_t* tuple_data;
    if (input_batch.compression_type != THdfsCompression::NONE) {
      // Decompress tuple data into data pool
      uint8_t* compressed_data = (uint8_t*)input_batch.tuple_data.c_str();
      size_t compressed_size = input_batch.tuple_data.size();

      scoped_ptr<Codec> decompressor;
      Status status = Codec::CreateDecompressor(NULL, false, input_batch.compression_type,
          &decompressor);
      DCHECK(status.ok()) << status.GetDetail();

      int64_t uncompressed_size = input_batch.uncompressed_size;
      DCHECK_NE(uncompressed_size, -1) << "RowBatch decompression failed";
      tuple_data = batch->tuple_data_pool_->Allocate(uncompressed_size);
      status = decompressor->ProcessBlock(true, compressed_size, compressed_data,
          &uncompressed_size, &tuple_data);
      DCHECK(status.ok()) << "RowBatch decompression failed.";
      decompressor->Close();
    } else {
      // Tuple data uncompressed, copy directly into data pool
      tuple_data = batch->tuple_data_pool_->Allocate(input_batch.tuple_data.size());
      memcpy(tuple_data, input_batch.tuple_data.c_str(), input_batch.tuple_data.size());
    }

    // Convert input_batch.tuple_offsets into pointers
    int tuple_idx = 0;
    for (vector<int32_t>::const_iterator offset = input_batch.tuple_offsets.begin();
         offset != input_batch.tuple_offsets.end(); ++offset) {///
      if (*offset == -1) {
        batch->tuple_ptrs_[tuple_idx++] = NULL;
      } else {
        batch->tuple_ptrs_[tuple_idx++] = reinterpret_cast<Tuple*>(tuple_data + *offset);
      }
    }

    // Check whether we have slots that require offset-to-pointer conversion.
    if (!batch->row_desc_.HasVarlenSlots()) return;

    for (int i = 0; i < batch->num_rows_; ++i) {
      for (int j = 0; j < batch->num_tuples_per_row_; ++j) {
        const TupleDescriptor* desc = batch->row_desc_.tuple_descriptors()[j];
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
    const TupleDescriptor* tuple_desc = batch->row_desc().tuple_descriptors()[0];
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
      TRowBatch trow_batch;
      args->batch->Serialize(&trow_batch, args->full_dedup);
    }
  }

  static void TestSerializeBaseline(int batch_size, void* data) {
    RowBatch* batch = reinterpret_cast<RowBatch*>(data);
    for (int iter = 0; iter < batch_size; ++iter) {
      TRowBatch trow_batch;
      RowBatchSerializeBaseline::Serialize(batch, &trow_batch);
    }
  }

  struct DeserializeArgs {
    TRowBatch* trow_batch;
    RowDescriptor* row_desc;
    MemTracker* tracker;
  };

  static void TestDeserialize(int batch_size, void* data) {
    struct DeserializeArgs* args = reinterpret_cast<struct DeserializeArgs*>(data);
    for (int iter = 0; iter < batch_size; ++iter) {
      RowBatch deserialized_batch(*args->row_desc, *args->trow_batch, args->tracker);
    }
  }

  static void TestDeserializeBaseline(int batch_size, void* data) {
    struct DeserializeArgs* args = reinterpret_cast<struct DeserializeArgs*>(data);
    for (int iter = 0; iter < batch_size; ++iter) {
      RowBatch deserialized_batch(*args->row_desc, args->trow_batch->num_rows,
          args->tracker);
      RowBatchSerializeBaseline::Deserialize(&deserialized_batch, *args->trow_batch);
    }
  }

  static void Run() {
    CpuInfo::Init();

    MemTracker tracker;
    MemPool mem_pool(&tracker);
    ObjectPool obj_pool;
    DescriptorTblBuilder builder(&obj_pool);
    builder.DeclareTuple() << TYPE_INT << TYPE_STRING;
    DescriptorTbl* desc_tbl = builder.Build();

    vector<bool> nullable_tuples(1, false);
    vector<TTupleId> tuple_id(1, (TTupleId) 0);
    RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);

    RowBatch* no_dup_batch = obj_pool.Add(new RowBatch(row_desc, NUM_ROWS, &tracker));
    FillBatch(no_dup_batch, 12345, 1, -1);
    TRowBatch no_dup_tbatch;
    no_dup_batch->Serialize(&no_dup_tbatch);

    RowBatch* adjacent_dup_batch =
        obj_pool.Add(new RowBatch(row_desc, NUM_ROWS, &tracker));
    FillBatch(adjacent_dup_batch, 12345, 5, -1);
    TRowBatch adjacent_dup_tbatch;
    adjacent_dup_batch->Serialize(&adjacent_dup_tbatch, false);

    RowBatch* dup_batch =
        obj_pool.Add(new RowBatch(row_desc, NUM_ROWS, &tracker));
    // Non-adjacent duplicates.
    FillBatch(dup_batch, 12345, 1, NUM_ROWS / 5);
    TRowBatch dup_tbatch;
    dup_batch->Serialize(&dup_tbatch, true);

    int baseline;
    Benchmark ser_suite("serialize");
    baseline = ser_suite.AddBenchmark("ser_no_dups_baseline", TestSerializeBaseline,
        no_dup_batch, -1);
    struct SerializeArgs no_dup_ser_args = { no_dup_batch, false };
    struct SerializeArgs no_dup_ser_full_args = { no_dup_batch, true };
    ser_suite.AddBenchmark("ser_no_dups", TestSerialize, &no_dup_ser_args, baseline);
    ser_suite.AddBenchmark("ser_no_dups_full",
        TestSerialize, &no_dup_ser_full_args, baseline);

    baseline = ser_suite.AddBenchmark("ser_adjacent_dups_baseline",
        TestSerializeBaseline, adjacent_dup_batch, -1);
    struct SerializeArgs adjacent_dup_ser_args = { adjacent_dup_batch, false };
    struct SerializeArgs adjacent_dup_ser_full_args = { adjacent_dup_batch, true };
    ser_suite.AddBenchmark("ser_adjacent_dups",
        TestSerialize, &adjacent_dup_ser_args, baseline);
    ser_suite.AddBenchmark("ser_adjacent_dups_full",
        TestSerialize, &adjacent_dup_ser_full_args, baseline);

    baseline = ser_suite.AddBenchmark("ser_dups_baseline",
        TestSerializeBaseline, dup_batch, -1);
    struct SerializeArgs dup_ser_args = { dup_batch, false };
    struct SerializeArgs dup_ser_full_args = { dup_batch, true };
    ser_suite.AddBenchmark("ser_dups", TestSerialize, &dup_ser_args, baseline);
    ser_suite.AddBenchmark("ser_dups_full", TestSerialize, &dup_ser_full_args, baseline);

    cout << ser_suite.Measure() << endl;

    Benchmark deser_suite("deserialize");
    struct DeserializeArgs no_dup_deser_args = { &no_dup_tbatch, &row_desc, &tracker };
    baseline = deser_suite.AddBenchmark("deser_no_dups_baseline",
        TestDeserializeBaseline, &no_dup_deser_args, -1);
    deser_suite.AddBenchmark("deser_no_dups",
        TestDeserialize, &no_dup_deser_args, baseline);

    struct DeserializeArgs adjacent_dup_deser_args = { &adjacent_dup_tbatch, &row_desc,
        &tracker };
    baseline = deser_suite.AddBenchmark("deser_adjacent_dups_baseline",
        TestDeserializeBaseline, &adjacent_dup_deser_args, -1);
    deser_suite.AddBenchmark("deser_adjacent_dups",
        TestDeserialize, &adjacent_dup_deser_args, baseline);

    struct DeserializeArgs dup_deser_args = { &dup_tbatch, &row_desc, &tracker };
    baseline = deser_suite.AddBenchmark("deser_dups_baseline",
        TestDeserializeBaseline, &dup_deser_args, -1);
    deser_suite.AddBenchmark("deser_dups", TestDeserialize, &dup_deser_args, baseline);

    cout << deser_suite.Measure() << endl;
  }
};

}

int main(int argc, char** argv) {
  RowBatchSerializeBenchmark::Run();
  return 0;
}
