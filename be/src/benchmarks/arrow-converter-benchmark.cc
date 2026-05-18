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

#include <arrow/api.h>

#include "common/init.h"
#include "exec/arrow-converter.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "util/benchmark.h"

#include "common/names.h"

using namespace impala;

// Benchmark tests for converting arrow record batch into impala tuple batch.

// Machine Info: Intel(R) Core(TM) i9-14900K
// Arrow To Impala:
// Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                (relative) (relative) (relative)
// -------------------------------------------------------------------------------
//     Int8           1.14e+03 1.17e+03  1.2e+03         1X         1X         1X
//    Int16           1.08e+03  1.1e+03 1.14e+03     0.945X     0.947X     0.947X
//    Int32           1.07e+03 1.09e+03 1.11e+03     0.934X     0.939X     0.927X
//    Int64           1.07e+03 1.09e+03 1.11e+03     0.936X     0.935X     0.924X
//    Mixed                271      277      281     0.238X     0.237X     0.234X
//
// Arrow To Arrow:
// Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                (relative) (relative) (relative)
// -------------------------------------------------------------------------------
//     Int8           5.51e+03 5.56e+03 5.64e+03         1X         1X         1X
//    Int16            5.4e+03 5.45e+03 5.53e+03     0.979X     0.979X     0.981X
//    Int32           5.24e+03 5.31e+03 5.36e+03      0.95X     0.954X      0.95X
//    Int64           4.91e+03 4.95e+03 5.01e+03     0.891X     0.891X     0.889X
//    Mixed           1.44e+03 1.46e+03 1.48e+03     0.261X     0.262X     0.263X

constexpr int NUM_ROWS = 1024;

template<typename T, typename BuilderT>
static arrow::Status BuildArrowArray(std::shared_ptr<arrow::Array>& array) {
  BuilderT builder;
  for (int row_idx = 0; row_idx < NUM_ROWS; ++row_idx) {
    ARROW_RETURN_NOT_OK(builder.Append((T)row_idx));
  }
  ARROW_ASSIGN_OR_RAISE(array, builder.Finish());

  return arrow::Status::OK();
}

struct BenchmarkData {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ArrowToTupleConverter converter;
  uint8_t* tuple_mem;
  MemPool* mem_pool;

  BenchmarkData(const std::shared_ptr<arrow::RecordBatch>& record_batch,
      const TupleDescriptor* tuple_desc, uint8_t* tuple_mem, MemPool* mem_pool)
    : record_batch(record_batch), converter(tuple_desc), tuple_mem(tuple_mem),
      mem_pool(mem_pool) {
  }
};

void TestIntConversion(int batch_size, void* d) {
  BenchmarkData* data = static_cast<BenchmarkData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    Status status = data->converter.Convert(
        *data->record_batch, data->tuple_mem, data->mem_pool);

    if (UNLIKELY(!status.ok())) {
      cout << "Could not convert arrow record batch to tuple batch: "
          << status.GetDetail() << endl;
      return;
    }
  }
}

// Make a copy of an arrow record batch, to compare performance with conversion.
struct ArrowCopyBenchmarkData {
  std::shared_ptr<arrow::RecordBatch> record_batch;

  ArrowCopyBenchmarkData(const std::shared_ptr<arrow::RecordBatch>& record_batch)
      : record_batch(record_batch) {
  }
};

static arrow::Result<std::shared_ptr<arrow::RecordBatch>> CopyArrowRecordBatch(
    const std::shared_ptr<arrow::RecordBatch>& record_batch) {
  // Make a deep copy of all columns.
  std::vector<std::shared_ptr<arrow::Array>> new_columns;
  new_columns.reserve(record_batch->num_columns());
  for (int column_idx = 0; column_idx < record_batch->num_columns(); ++column_idx) {
    const std::shared_ptr<arrow::ArrayData>& array_data =
        record_batch->column(column_idx)->data();
    std::vector<std::shared_ptr<arrow::Buffer>> new_buffers;
    new_buffers.reserve(array_data->buffers.size());
    for (const std::shared_ptr<arrow::Buffer>& buffer : array_data->buffers) {
      if (buffer == nullptr) {
        new_buffers.push_back(nullptr);
        continue;
      }

      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Buffer> new_buffer,
          arrow::AllocateBuffer(buffer->size()));
      memcpy(new_buffer->mutable_data(), buffer->data(), buffer->size());
      new_buffers.push_back(std::move(new_buffer));
    }
    std::shared_ptr<arrow::ArrayData> new_array_data = arrow::ArrayData::Make(
        array_data->type, array_data->length, new_buffers, array_data->null_count,
        array_data->offset);
    new_columns.push_back(arrow::MakeArray(new_array_data));
  }
  return arrow::RecordBatch::Make(record_batch->schema(), record_batch->num_rows(),
      new_columns);
}

void TestArrowToArrowCopy(int batch_size, void* d) {
  ArrowCopyBenchmarkData* data = static_cast<ArrowCopyBenchmarkData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> result =
        CopyArrowRecordBatch(data->record_batch);
    if (UNLIKELY(!result.ok())) {
      cout << "Could not copy arrow record batch: " << result.status().ToString() << endl;
      return;
    }
  }
}

int main(int argc, char** argv) {
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  cout << Benchmark::GetMachineInfo() << endl;
  impala::InitFeSupport();

  Status status;
  RuntimeState* state;
  TestEnv test_env;
  status = test_env.Init();
  if (!status.ok()) {
    cout << "Could not init TestEnv";
    return -1;
  }
  status = test_env.CreateQueryState(0, nullptr, &state);
  if (!status.ok()) {
    cout << "Could not create RuntimeState";
    return -1;
  }
  QueryState* qs = state->query_state();

  MemTracker tracker;
  MemPool mem_pool(&tracker);

  arrow::Status arrow_status;
  vector<TTupleId> tuple_ids(1, static_cast<TTupleId>(0));
  vector<bool> nullable_tuples(1, false);

  std::shared_ptr<arrow::Array> int8_array;
  arrow_status = BuildArrowArray<int8_t, arrow::Int8Builder>(int8_array);
  if (!arrow_status.ok()) {
    cout << "Could not create arrow int8 array.";
    return -1;
  }
  std::shared_ptr<arrow::Field> int8_field = arrow::field("field_int8", arrow::int8());
  std::shared_ptr<arrow::Schema> int8_schema = arrow::schema({int8_field});
  std::shared_ptr<arrow::RecordBatch> int8_record_batch = arrow::RecordBatch::Make(
      int8_schema, NUM_ROWS, {int8_array});
  DescriptorTblBuilder int8_tbl_builder(test_env.exec_env()->frontend(), qs->obj_pool());
  int8_tbl_builder.DeclareTuple() << TYPE_TINYINT;
  RowDescriptor* int8_row_desc = qs->obj_pool()->Add(
      new RowDescriptor(*int8_tbl_builder.Build(), tuple_ids, nullable_tuples));
  TupleDescriptor* int8_tuple_desc = int8_row_desc->tuple_descriptors()[0];

  std::shared_ptr<arrow::Array> int16_array;
  arrow_status = BuildArrowArray<int16_t, arrow::Int16Builder>(int16_array);
  if (!arrow_status.ok()) {
    cout << "Could not create arrow int16 array.";
    return -1;
  }
  std::shared_ptr<arrow::Field> int16_field = arrow::field("field_int16", arrow::int16());
  std::shared_ptr<arrow::Schema> int16_schema = arrow::schema({int16_field});
  std::shared_ptr<arrow::RecordBatch> int16_record_batch = arrow::RecordBatch::Make(
      int16_schema, NUM_ROWS, {int16_array});
  DescriptorTblBuilder int16_tbl_builder(test_env.exec_env()->frontend(), qs->obj_pool());
  int16_tbl_builder.DeclareTuple() << TYPE_SMALLINT;
  RowDescriptor* int16_row_desc = qs->obj_pool()->Add(
      new RowDescriptor(*int16_tbl_builder.Build(), tuple_ids, nullable_tuples));
  TupleDescriptor* int16_tuple_desc = int16_row_desc->tuple_descriptors()[0];

  std::shared_ptr<arrow::Array> int32_array;
  arrow_status = BuildArrowArray<int32_t, arrow::Int32Builder>(int32_array);
  if (!arrow_status.ok()) {
    cout << "Could not create arrow int32 array.";
    return -1;
  }
  std::shared_ptr<arrow::Field> int32_field = arrow::field("field_int32", arrow::int32());
  std::shared_ptr<arrow::Schema> int32_schema = arrow::schema({int32_field});
  std::shared_ptr<arrow::RecordBatch> int32_record_batch = arrow::RecordBatch::Make(
      int32_schema, NUM_ROWS, {int32_array});
  DescriptorTblBuilder int32_tbl_builder(test_env.exec_env()->frontend(), qs->obj_pool());
  int32_tbl_builder.DeclareTuple() << TYPE_INT;
  RowDescriptor* int32_row_desc = qs->obj_pool()->Add(
      new RowDescriptor(*int32_tbl_builder.Build(), tuple_ids, nullable_tuples));
  TupleDescriptor* int32_tuple_desc = int32_row_desc->tuple_descriptors()[0];

  std::shared_ptr<arrow::Array> int64_array;
  arrow_status = BuildArrowArray<int64_t, arrow::Int64Builder>(int64_array);
  if (!arrow_status.ok()) {
    cout << "Could not create arrow int64 array.";
    return -1;
  }
  std::shared_ptr<arrow::Field> int64_field = arrow::field("field_int64", arrow::int64());
  std::shared_ptr<arrow::Schema> int64_schema = arrow::schema({int64_field});
  std::shared_ptr<arrow::RecordBatch> int64_record_batch = arrow::RecordBatch::Make(
      int64_schema, NUM_ROWS, {int64_array});
  DescriptorTblBuilder int64_tbl_builder(test_env.exec_env()->frontend(), qs->obj_pool());
  int64_tbl_builder.DeclareTuple() << TYPE_BIGINT;
  RowDescriptor* int64_row_desc = qs->obj_pool()->Add(
      new RowDescriptor(*int64_tbl_builder.Build(), tuple_ids, nullable_tuples));
  TupleDescriptor* int64_tuple_desc = int64_row_desc->tuple_descriptors()[0];

  std::shared_ptr<arrow::Schema> mixed_schema = arrow::schema({int8_field, int16_field,
      int32_field, int64_field});
  std::shared_ptr<arrow::RecordBatch> mixed_record_batch = arrow::RecordBatch::Make(
      mixed_schema, NUM_ROWS, {int8_array, int16_array, int32_array, int64_array});
  DescriptorTblBuilder mixed_tbl_builder(test_env.exec_env()->frontend(), qs->obj_pool());
  mixed_tbl_builder.DeclareTuple() << TYPE_TINYINT << TYPE_SMALLINT << TYPE_INT
      << TYPE_BIGINT;
  RowDescriptor* mixed_row_desc = qs->obj_pool()->Add(
      new RowDescriptor(*mixed_tbl_builder.Build(), tuple_ids, nullable_tuples));
  TupleDescriptor* mixed_tuple_desc = mixed_row_desc->tuple_descriptors()[0];

  // Allocate enough memory for tuples to fit in each case.
  uint8_t* tuple_mem = mem_pool.Allocate(NUM_ROWS * 4 * sizeof(int64_t));

  BenchmarkData int8_data(int8_record_batch, int8_tuple_desc, tuple_mem, &mem_pool);
  BenchmarkData int16_data(int16_record_batch, int16_tuple_desc, tuple_mem, &mem_pool);
  BenchmarkData int32_data(int32_record_batch, int32_tuple_desc, tuple_mem, &mem_pool);
  BenchmarkData int64_data(int64_record_batch, int64_tuple_desc, tuple_mem, &mem_pool);
  BenchmarkData mixed_data(mixed_record_batch, mixed_tuple_desc, tuple_mem, &mem_pool);
  for (BenchmarkData* data :
      {&int8_data, &int16_data, &int32_data, &int64_data, &mixed_data}) {
    status = data->converter.Init(state);
    if (!status.ok()) {
      cout << "Could not init converter: " << status.GetDetail() << endl;
      return -1;
    }
  }

  Benchmark arrow_to_impala_suite("Arrow To Impala");
  arrow_to_impala_suite.AddBenchmark("Int8", TestIntConversion, &int8_data);
  arrow_to_impala_suite.AddBenchmark("Int16", TestIntConversion, &int16_data);
  arrow_to_impala_suite.AddBenchmark("Int32", TestIntConversion, &int32_data);
  arrow_to_impala_suite.AddBenchmark("Int64", TestIntConversion, &int64_data);
  arrow_to_impala_suite.AddBenchmark("Mixed", TestIntConversion, &mixed_data);
  cout << arrow_to_impala_suite.Measure() << endl;

  ArrowCopyBenchmarkData arrow_int8_data(int8_record_batch);
  ArrowCopyBenchmarkData arrow_int16_data(int16_record_batch);
  ArrowCopyBenchmarkData arrow_int32_data(int32_record_batch);
  ArrowCopyBenchmarkData arrow_int64_data(int64_record_batch);
  ArrowCopyBenchmarkData arrow_mixed_data(mixed_record_batch);

  Benchmark arrow_to_arrow_suite("Arrow To Arrow");
  arrow_to_arrow_suite.AddBenchmark("Int8", TestArrowToArrowCopy, &arrow_int8_data);
  arrow_to_arrow_suite.AddBenchmark("Int16", TestArrowToArrowCopy, &arrow_int16_data);
  arrow_to_arrow_suite.AddBenchmark("Int32", TestArrowToArrowCopy, &arrow_int32_data);
  arrow_to_arrow_suite.AddBenchmark("Int64", TestArrowToArrowCopy, &arrow_int64_data);
  arrow_to_arrow_suite.AddBenchmark("Mixed", TestArrowToArrowCopy, &arrow_mixed_data);
  cout << arrow_to_arrow_suite.Measure() << endl;

  mem_pool.FreeAll();
  return 0;
}