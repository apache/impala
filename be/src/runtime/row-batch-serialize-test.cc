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

#include <gtest/gtest.h>

#include "runtime/array-value.h"
#include "runtime/array-value-builder.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "util/stopwatch.h"
#include "testutil/desc-tbl-builder.h"

#include "common/names.h"

namespace impala {

const int NUM_ROWS = 20;
const int MAX_STRING_LEN = 10;
const int MAX_ARRAY_LEN = 3;
const int NULL_VALUE_PERCENT = 10;

class RowBatchSerializeTest : public testing::Test {
 protected:
  ObjectPool pool_;
  scoped_ptr<MemTracker> tracker_;

  virtual void SetUp() {
    tracker_.reset(new MemTracker());
  }

  virtual void TearDown() {
    pool_.Clear();
    tracker_.reset();
  }

  // Serializes and deserializes 'batch', then checks that the deserialized batch is valid
  // and has the same contents as 'batch'.
  void TestRowBatch(const RowDescriptor& row_desc, RowBatch* batch, bool print_batches) {
    if (print_batches) cout << PrintBatch(batch) << endl;

    TRowBatch trow_batch;
    batch->Serialize(&trow_batch);

    RowBatch deserialized_batch(row_desc, trow_batch, tracker_.get());
    if (print_batches) cout << PrintBatch(&deserialized_batch) << endl;

    EXPECT_EQ(batch->num_rows(), deserialized_batch.num_rows());
    for (int row_idx = 0; row_idx < batch->num_rows(); ++row_idx) {
      TupleRow* row = batch->GetRow(row_idx);
      TupleRow* deserialized_row = deserialized_batch.GetRow(row_idx);

      for (int tuple_idx = 0; tuple_idx < row_desc.tuple_descriptors().size(); ++tuple_idx) {
        TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[tuple_idx];
        Tuple* tuple = row->GetTuple(tuple_idx);
        Tuple* deserialized_tuple = deserialized_row->GetTuple(tuple_idx);
        TestTuplesEqual(*tuple_desc, tuple, deserialized_tuple);
      }
    }
  }

  // Recursively checks that 'deserialized_tuple' is valid and has the same contents as
  // 'tuple'. 'deserialized_tuple' should be the result of serializing then deserializing
  // 'tuple'.
  void TestTuplesEqual(const TupleDescriptor& tuple_desc, Tuple* tuple,
      Tuple* deserialized_tuple) {
    if (tuple == NULL) {
      EXPECT_TRUE(deserialized_tuple == NULL);
      return;
    }
    EXPECT_FALSE(deserialized_tuple == NULL);

    for (int slot_idx = 0; slot_idx < tuple_desc.slots().size(); ++slot_idx) {
      SlotDescriptor* slot_desc = tuple_desc.slots()[slot_idx];
      if (!slot_desc->is_materialized()) continue;

      if (tuple->IsNull(slot_desc->null_indicator_offset())) {
        EXPECT_TRUE(deserialized_tuple->IsNull(slot_desc->null_indicator_offset()));
        continue;
      }
      EXPECT_FALSE(deserialized_tuple->IsNull(slot_desc->null_indicator_offset()));

      const ColumnType& type = slot_desc->type();
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      void* deserialized_slot = deserialized_tuple->GetSlot(slot_desc->tuple_offset());

      if (!type.IsCollectionType()) {
        EXPECT_TRUE(RawValue::Eq(slot, deserialized_slot, type));
      }

      if (type.IsStringType()) {
        // Check that serialized and deserialized string values have different pointers.
        StringValue* string_value = reinterpret_cast<StringValue*>(slot);
        StringValue* deserialized_string_value =
            reinterpret_cast<StringValue*>(deserialized_slot);
        EXPECT_NE(string_value->ptr, deserialized_string_value->ptr);
      }

      if (type.IsCollectionType()) {
        const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
        ArrayValue* array_value = reinterpret_cast<ArrayValue*>(slot);
        ArrayValue* deserialized_array_value =
            reinterpret_cast<ArrayValue*>(deserialized_slot);
        EXPECT_EQ(array_value->num_tuples, deserialized_array_value->num_tuples);

        uint8_t* array_data = array_value->ptr;
        uint8_t* deserialized_array_data = deserialized_array_value->ptr;
        for (int i = 0; i < array_value->num_tuples; ++i) {
          TestTuplesEqual(item_desc, reinterpret_cast<Tuple*>(array_data),
              reinterpret_cast<Tuple*>(deserialized_array_data));
          array_data += item_desc.byte_size();
          deserialized_array_data += item_desc.byte_size();
        }

        // Check that array values have different pointers
        EXPECT_NE(array_value->ptr, deserialized_array_value->ptr);
      }
    }
  }

  // Writes a randomized value in 'tuple'.
  void WriteValue(Tuple* tuple, const SlotDescriptor& slot_desc, MemPool* pool) {
    switch (slot_desc.type().type) {
      case TYPE_INT: {
        int val = rand();
        RawValue::Write(&val, tuple, &slot_desc, pool);
        break;
      }
      case TYPE_STRING: {
        // Via http://stackoverflow.com/questions/440133/
        //     how-do-i-create-a-random-alpha-numeric-string-in-c
        static const char chars[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        int len = rand() % (MAX_STRING_LEN + 1);
        char buf[len];
        for (int i = 0; i < len; ++i) {
          buf[i] = chars[rand() % (sizeof(chars) - 1)];
        }

        StringValue sv(&buf[0], len);
        RawValue::Write(&sv, tuple, &slot_desc, pool);
        break;
      }
      case TYPE_ARRAY: {
        const TupleDescriptor* item_desc = slot_desc.collection_item_descriptor();
        int array_len = rand() % (MAX_ARRAY_LEN + 1);
        ArrayValue av;
        ArrayValueBuilder builder(&av, *item_desc, pool, array_len);
        Tuple* tuple_mem;
        int n = builder.GetFreeMemory(&tuple_mem);
        DCHECK_GE(n, array_len);
        for (int i = 0; i < array_len; ++i) {
          for (int slot_idx = 0; slot_idx < item_desc->slots().size(); ++slot_idx) {
            SlotDescriptor* item_slot_desc = item_desc->slots()[slot_idx];
            if (!item_slot_desc->is_materialized()) continue;
            WriteValue(tuple_mem, *item_slot_desc, pool);
          }
          tuple_mem += item_desc->byte_size();
        }
        builder.CommitTuples(array_len);
        // Array data already lives in 'pool'
        RawValue::Write(&av, tuple, &slot_desc, NULL);
        break;
      }
      default:
        DCHECK(false) << "NYI: " << slot_desc.type().DebugString();
    }
  }

  // Creates a row batch with randomized values.
  RowBatch* CreateRowBatch(const RowDescriptor& row_desc) {
    RowBatch* batch = pool_.Add(new RowBatch(row_desc, NUM_ROWS, tracker_.get()));
    int len = row_desc.GetRowSize() * NUM_ROWS;
    uint8_t* tuple_mem = batch->tuple_data_pool()->Allocate(len);
    memset(tuple_mem, 0, len);

    for (int i = 0; i < NUM_ROWS; ++i) {
      int idx = batch->AddRow();
      DCHECK(idx != RowBatch::INVALID_ROW_INDEX);
      TupleRow* row = batch->GetRow(idx);

      for (int tuple_idx = 0; tuple_idx < row_desc.tuple_descriptors().size(); ++tuple_idx) {
        TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[tuple_idx];
        Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);

        for (int slot_idx = 0; slot_idx < tuple_desc->slots().size(); ++slot_idx) {
          SlotDescriptor* slot_desc = tuple_desc->slots()[slot_idx];
          if (!slot_desc->is_materialized()) continue;

          if (rand() % 100 < NULL_VALUE_PERCENT) {
            tuple->SetNull(slot_desc->null_indicator_offset());
          } else {
            WriteValue(tuple, *slot_desc, batch->tuple_data_pool());
          }
        }

        row->SetTuple(tuple_idx, tuple);
        tuple_mem += tuple_desc->byte_size();
      }
      batch->CommitLastRow();
    }
    return batch;
  }

  // Generate num_tuples distinct tuples with randomized values.
  void CreateTuples(const TupleDescriptor& tuple_desc, MemPool* pool,
      int num_tuples, int null_tuple_percent, int null_value_percent,
      vector<Tuple*>* result) {
    uint8_t* tuple_mem = pool->Allocate(tuple_desc.byte_size() * num_tuples);
    for (int i = 0; i < num_tuples; ++i) {
      if (null_tuple_percent > 0 && rand() % 100 < null_tuple_percent) {
        result->push_back(NULL);
        continue;
      }
      Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);
      for (int slot_idx = 0; slot_idx < tuple_desc.slots().size(); ++slot_idx) {
        SlotDescriptor* slot_desc = tuple_desc.slots()[slot_idx];
        if (!slot_desc->is_materialized()) continue;
        if (null_value_percent > 0 && rand() % 100 < null_value_percent) {
          tuple->SetNull(slot_desc->null_indicator_offset());
        } else {
          WriteValue(tuple, *slot_desc, pool);
        }
      }
      result->push_back(tuple);
      tuple_mem += tuple_desc.byte_size();
    }
  }

  // Create a row batch from preconstructed tuples. Each tuple instance for tuple i
  // is consecutively repeated repeats[i] times. The tuple instances are used in the
  // order provided, starting at the beginning once all are used.
  void AddTuplesToRowBatch(int num_rows, const vector<vector<Tuple*> >& tuples,
      const vector<int>& repeats, RowBatch* batch) {
    int tuples_per_row = batch->row_desc().tuple_descriptors().size();
    DCHECK_EQ(tuples_per_row, tuples.size());
    DCHECK_EQ(tuples_per_row, repeats.size());
    vector<int> next_tuple(tuples_per_row, 0);
    for (int i = 0; i < num_rows; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);
      for (int tuple_idx = 0; tuple_idx < tuples_per_row; ++tuple_idx) {
        int curr_tuple = next_tuple[tuple_idx];
        DCHECK(tuples[tuple_idx].size() > 0);
        row->SetTuple(tuple_idx, tuples[tuple_idx][curr_tuple]);
        if ((i + 1) % repeats[tuple_idx] == 0) {
          next_tuple[tuple_idx] = (curr_tuple + 1) % tuples[tuple_idx].size();
        }
      }
      batch->CommitLastRow();
    }
  }

  // Helper to build a row batch with only one tuple per row.
  void AddTuplesToRowBatch(int num_rows, const vector<Tuple*>& tuples, int repeats,
      RowBatch* batch) {
    vector<vector<Tuple*> > tmp_tuples(1, tuples);
    vector<int> tmp_repeats(1, repeats);
    AddTuplesToRowBatch(num_rows, tmp_tuples, tmp_repeats, batch);
  }
};

TEST_F(RowBatchSerializeTest, Basic) {
  // tuple: (int)
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  DCHECK_EQ(row_desc.tuple_descriptors().size(), 1);

  RowBatch* batch = CreateRowBatch(row_desc);
  TestRowBatch(row_desc, batch, true);
}

TEST_F(RowBatchSerializeTest, String) {
  // tuple: (int, string)
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT << TYPE_STRING;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  DCHECK_EQ(row_desc.tuple_descriptors().size(), 1);

  RowBatch* batch = CreateRowBatch(row_desc);
  TestRowBatch(row_desc, batch, true);
}

TEST_F(RowBatchSerializeTest, BasicArray) {
  // tuple: (int, string, array<int>)
  ColumnType array_type;
  array_type.type = TYPE_ARRAY;
  array_type.children.push_back(TYPE_INT);

  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT << TYPE_STRING << array_type;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  DCHECK_EQ(row_desc.tuple_descriptors().size(), 1);

  RowBatch* batch = CreateRowBatch(row_desc);
  TestRowBatch(row_desc, batch, true);
}

TEST_F(RowBatchSerializeTest, StringArray) {
  // tuple: (int, string, array<struct<int, string, string>>)
  ColumnType struct_type;
  struct_type.type = TYPE_STRUCT;
  struct_type.children.push_back(TYPE_INT);
  struct_type.field_names.push_back("int1");
  struct_type.children.push_back(TYPE_STRING);
  struct_type.field_names.push_back("string1");
  struct_type.children.push_back(TYPE_STRING);
  struct_type.field_names.push_back("string2");

  ColumnType array_type;
  array_type.type = TYPE_ARRAY;
  array_type.children.push_back(struct_type);

  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT << TYPE_STRING << array_type;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  DCHECK_EQ(row_desc.tuple_descriptors().size(), 1);

  RowBatch* batch = CreateRowBatch(row_desc);
  TestRowBatch(row_desc, batch, true);
}

TEST_F(RowBatchSerializeTest, NestedArrays) {
  // tuple: (array<struct<array<string>, array<string, string>>>)
  ColumnType inner_array_type1;
  inner_array_type1.type = TYPE_ARRAY;
  inner_array_type1.children.push_back(TYPE_STRING);

  ColumnType inner_struct_type;
  inner_struct_type.type = TYPE_STRUCT;
  inner_struct_type.children.push_back(TYPE_STRING);
  inner_struct_type.field_names.push_back("string1");
  inner_struct_type.children.push_back(TYPE_STRING);
  inner_struct_type.field_names.push_back("string2");

  ColumnType inner_array_type2;
  inner_array_type2.type = TYPE_ARRAY;
  inner_array_type2.children.push_back(inner_struct_type);

  ColumnType struct_type;
  struct_type.type = TYPE_STRUCT;
  struct_type.children.push_back(inner_array_type1);
  struct_type.field_names.push_back("array1");
  struct_type.children.push_back(inner_array_type2);
  struct_type.field_names.push_back("array2");

  ColumnType array_type;
  array_type.type = TYPE_ARRAY;
  array_type.children.push_back(struct_type);

  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << array_type;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  DCHECK_EQ(row_desc.tuple_descriptors().size(), 1);

  RowBatch* batch = CreateRowBatch(row_desc);
  TestRowBatch(row_desc, batch, true);
}

// Test that we get correct results when serializing/deserializing with duplicates.
TEST_F(RowBatchSerializeTest, DupCorrectness) {
  // tuples: (int), (string)
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT;
  builder.DeclareTuple() << TYPE_STRING;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(2, false);
  vector<TTupleId> tuple_id;
  tuple_id.push_back((TTupleId) 0);
  tuple_id.push_back((TTupleId) 1);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  DCHECK_EQ(row_desc.tuple_descriptors().size(), 2);

  int num_rows = 1000;
  int distinct_int_tuples = 100;
  int distinct_string_tuples = 100;
  vector<int> repeats;
  // All int dups are non-consecutive
  repeats.push_back(1);
  // All string dups are consecutive
  repeats.push_back(num_rows / distinct_string_tuples + 1);
  RowBatch* batch = pool_.Add(new RowBatch(row_desc, num_rows, tracker_.get()));
  vector<vector<Tuple*> > distinct_tuples(2);
  CreateTuples(*row_desc.tuple_descriptors()[0], batch->tuple_data_pool(),
      distinct_int_tuples, 0, 10, &distinct_tuples[0]);
  CreateTuples(*row_desc.tuple_descriptors()[1], batch->tuple_data_pool(),
      distinct_string_tuples, 0, 10, &distinct_tuples[1]);
  AddTuplesToRowBatch(num_rows, distinct_tuples, repeats, batch);
  TestRowBatch(row_desc, batch, false);
}

// Test that tuple deduplication results in the expected reduction in serialized size.
TEST_F(RowBatchSerializeTest, DupRemoval) {
  // tuples: (int, string)
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT << TYPE_STRING;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  TupleDescriptor& tuple_desc = *row_desc.tuple_descriptors()[0];

  int num_rows = 1000;
  int num_distinct_tuples = 100;
  // All dups are consecutive
  int repeats = num_rows / num_distinct_tuples;
  RowBatch* batch = pool_.Add(new RowBatch(row_desc, num_rows, tracker_.get()));
  vector<Tuple*> tuples;
  CreateTuples(tuple_desc, batch->tuple_data_pool(), num_distinct_tuples, 0, 10, &tuples);
  AddTuplesToRowBatch(num_rows, tuples, repeats, batch);
  TRowBatch trow_batch;
  batch->Serialize(&trow_batch);
  // Serialized data should only have one copy of each tuple.
  int64_t total_byte_size = 0; // Total size without duplication
  for (int i = 0; i < tuples.size(); ++i) {
    total_byte_size += tuples[i]->TotalByteSize(tuple_desc);
  }
  EXPECT_EQ(total_byte_size, trow_batch.uncompressed_size);
  TestRowBatch(row_desc, batch, false);
}

// Test that deduplication handles NULL tuples correctly.
TEST_F(RowBatchSerializeTest, ConsecutiveNullTuples) {
  // tuples: (int)
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT;
  DescriptorTbl* desc_tbl = builder.Build();
  vector<bool> nullable_tuples(1, true);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);

  int num_rows = 100;
  int num_distinct_tuples = 20;
  int repeats = 5;
  RowBatch* batch = pool_.Add(new RowBatch(row_desc, num_rows, tracker_.get()));
  vector<Tuple*> tuples;
  CreateTuples(*row_desc.tuple_descriptors()[0], batch->tuple_data_pool(),
      num_distinct_tuples, 50, 10, &tuples);
  AddTuplesToRowBatch(num_rows, tuples, repeats, batch);
  TestRowBatch(row_desc, batch, false);
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  uint32_t seed = time(NULL);
  cout << "seed = " << seed << endl;
  srand(seed);
  return RUN_ALL_TESTS();
}
