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

#include "runtime/collection-value.h"
#include "runtime/collection-value-builder.h"
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
  void TestRowBatch(const RowDescriptor& row_desc, RowBatch* batch, bool print_batches,
      bool full_dedup = false) {
    if (print_batches) cout << PrintBatch(batch) << endl;

    TRowBatch trow_batch;
    EXPECT_TRUE(batch->Serialize(&trow_batch, full_dedup).ok());

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
    if (tuple_desc.byte_size() == 0) return;
    if (tuple == NULL) {
      EXPECT_TRUE(deserialized_tuple == NULL);
      return;
    }
    EXPECT_FALSE(deserialized_tuple == NULL);

    for (int slot_idx = 0; slot_idx < tuple_desc.slots().size(); ++slot_idx) {
      SlotDescriptor* slot_desc = tuple_desc.slots()[slot_idx];

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
        CollectionValue* coll_value = reinterpret_cast<CollectionValue*>(slot);
        CollectionValue* deserialized_coll_value =
            reinterpret_cast<CollectionValue*>(deserialized_slot);
        EXPECT_EQ(coll_value->num_tuples, deserialized_coll_value->num_tuples);

        uint8_t* coll_data = coll_value->ptr;
        uint8_t* deserialized_coll_data = deserialized_coll_value->ptr;
        for (int i = 0; i < coll_value->num_tuples; ++i) {
          TestTuplesEqual(item_desc, reinterpret_cast<Tuple*>(coll_data),
              reinterpret_cast<Tuple*>(deserialized_coll_data));
          coll_data += item_desc.byte_size();
          deserialized_coll_data += item_desc.byte_size();
        }

        // Check that collection values have different pointers
        EXPECT_NE(coll_value->ptr, deserialized_coll_value->ptr);
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
        CollectionValue cv;
        CollectionValueBuilder builder(&cv, *item_desc, pool, array_len);
        Tuple* tuple_mem;
        int n = builder.GetFreeMemory(&tuple_mem);
        ASSERT_GE(n, array_len);
        memset(tuple_mem, 0, item_desc->byte_size() * array_len);
        for (int i = 0; i < array_len; ++i) {
          for (int slot_idx = 0; slot_idx < item_desc->slots().size(); ++slot_idx) {
            SlotDescriptor* item_slot_desc = item_desc->slots()[slot_idx];
            WriteValue(tuple_mem, *item_slot_desc, pool);
          }
          tuple_mem += item_desc->byte_size();
        }
        builder.CommitTuples(array_len);
        // Array data already lives in 'pool'
        RawValue::Write(&cv, tuple, &slot_desc, NULL);
        break;
      }
      default:
        ASSERT_TRUE(false) << "NYI: " << slot_desc.type().DebugString();
    }
  }

  // Creates a row batch with randomized values.
  RowBatch* CreateRowBatch(const RowDescriptor& row_desc) {
    RowBatch* batch = pool_.Add(new RowBatch(row_desc, NUM_ROWS, tracker_.get()));
    int len = row_desc.GetRowSize() * NUM_ROWS;
    uint8_t* tuple_mem = batch->tuple_data_pool()->Allocate(len);
    memset(tuple_mem, 0, len);

    for (int i = 0; i < NUM_ROWS; ++i) {
      TupleRow* row = batch->GetRow(batch->AddRow());

      for (int tuple_idx = 0; tuple_idx < row_desc.tuple_descriptors().size(); ++tuple_idx) {
        TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[tuple_idx];
        // Allocating zero-length tuples in this way means that multiple tuples can have
        // same address: an important corner case to test.
        Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);

        for (int slot_idx = 0; slot_idx < tuple_desc->slots().size(); ++slot_idx) {
          SlotDescriptor* slot_desc = tuple_desc->slots()[slot_idx];
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
    memset(tuple_mem, 0, tuple_desc.byte_size() * num_tuples);
    for (int i = 0; i < num_tuples; ++i) {
      if (null_tuple_percent > 0 && rand() % 100 < null_tuple_percent) {
        result->push_back(NULL);
        continue;
      }
      Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);
      for (int slot_idx = 0; slot_idx < tuple_desc.slots().size(); ++slot_idx) {
        SlotDescriptor* slot_desc = tuple_desc.slots()[slot_idx];
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
    ASSERT_EQ(tuples_per_row, tuples.size());
    ASSERT_EQ(tuples_per_row, repeats.size());
    vector<int> next_tuple(tuples_per_row, 0);
    for (int i = 0; i < num_rows; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);
      for (int tuple_idx = 0; tuple_idx < tuples_per_row; ++tuple_idx) {
        int curr_tuple = next_tuple[tuple_idx];
        ASSERT_GT(tuples[tuple_idx].size(), 0);
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

  // Helper to access internal state of batch (this class is friend of RowBatch).
  bool UseFullDedup(RowBatch* batch) { return batch->UseFullDedup(); }

  void TestDupCorrectness(bool full_dedup);

  void TestDupRemoval(bool full_dedup);

  void TestConsecutiveNulls(bool full_dedup);

  void TestZeroLengthTuple(bool full_dedup);
};

TEST_F(RowBatchSerializeTest, Basic) {
  // tuple: (int)
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT;
  DescriptorTbl* desc_tbl = builder.Build();

  vector<bool> nullable_tuples(1, false);
  vector<TTupleId> tuple_id(1, (TTupleId) 0);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  ASSERT_EQ(row_desc.tuple_descriptors().size(), 1);

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
  ASSERT_EQ(row_desc.tuple_descriptors().size(), 1);

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
  ASSERT_EQ(row_desc.tuple_descriptors().size(), 1);

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
  ASSERT_EQ(row_desc.tuple_descriptors().size(), 1);

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
  ASSERT_EQ(row_desc.tuple_descriptors().size(), 1);

  RowBatch* batch = CreateRowBatch(row_desc);
  TestRowBatch(row_desc, batch, true);
}

// Test that we get correct result when serializing/deserializing with duplicates
TEST_F(RowBatchSerializeTest, DupCorrectnessAdjacent) {
  TestDupCorrectness(false);
}

TEST_F(RowBatchSerializeTest, DupCorrectnessFull) {
  TestDupCorrectness(true);
}

void RowBatchSerializeTest::TestDupCorrectness(bool full_dedup) {
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
  ASSERT_EQ(row_desc.tuple_descriptors().size(), 2);

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
  TestRowBatch(row_desc, batch, false, full_dedup);
}

TEST_F(RowBatchSerializeTest, DupRemovalAdjacent) {
  TestDupRemoval(false);
}

TEST_F(RowBatchSerializeTest, DupRemovalFull) {
  TestDupRemoval(true);
}

// Test that tuple deduplication results in the expected reduction in serialized size.
void RowBatchSerializeTest::TestDupRemoval(bool full_dedup) {
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
  EXPECT_TRUE(batch->Serialize(&trow_batch, full_dedup).ok());
  // Serialized data should only have one copy of each tuple.
  int64_t total_byte_size = 0; // Total size without duplication
  for (int i = 0; i < tuples.size(); ++i) {
    total_byte_size += tuples[i]->TotalByteSize(tuple_desc);
  }
  EXPECT_EQ(total_byte_size, trow_batch.uncompressed_size);
  TestRowBatch(row_desc, batch, false, full_dedup);
}

TEST_F(RowBatchSerializeTest, ConsecutiveNullsAdjacent) {
  TestConsecutiveNulls(false);
}

TEST_F(RowBatchSerializeTest, ConsecutiveNullsFull) {
  TestConsecutiveNulls(true);
}

// Test that deduplication handles NULL tuples correctly.
void RowBatchSerializeTest::TestConsecutiveNulls(bool full_dedup) {
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
  TestRowBatch(row_desc, batch, false, full_dedup);
}

TEST_F(RowBatchSerializeTest, ZeroLengthTuples) {
  TestZeroLengthTuple(false);
}

TEST_F(RowBatchSerializeTest, ZeroLengthTuplesDedup) {
  TestZeroLengthTuple(true);
}

void RowBatchSerializeTest::TestZeroLengthTuple(bool full_dedup) {
  // tuples: (int), (string), ()
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT;
  builder.DeclareTuple() << TYPE_STRING;
  builder.DeclareTuple();
  DescriptorTbl* desc_tbl = builder.Build();
  vector<bool> nullable_tuples(3, false);
  vector<TTupleId> tuple_ids;
  tuple_ids.push_back((TTupleId) 0);
  tuple_ids.push_back((TTupleId) 1);
  tuple_ids.push_back((TTupleId) 2);
  RowDescriptor row_desc(*desc_tbl, tuple_ids, nullable_tuples);

  RowBatch* batch = CreateRowBatch(row_desc);
  TestRowBatch(row_desc, batch, false, full_dedup);
}

// Test a pathological case for consecutive deduplication - two large alternating tuples.
// This tests that we are capable of duplicating non-adjacent tuples to produce a compact
// serialized batch with no duplication. It also stresses the serialization logic to
// ensure that it can handle batches with large degrees of duplication without hitting
// any runtime errors.
TEST_F(RowBatchSerializeTest, DedupPathologicalFull) {
  // tuples: (int, int array<(string)>)
  // Need 3 tuples + array to enable non-adjacent dedup automatically
  ColumnType array_type;
  array_type.type = TYPE_ARRAY;
  array_type.children.push_back(TYPE_STRING);
  DescriptorTblBuilder builder(&pool_);
  builder.DeclareTuple() << TYPE_INT;
  builder.DeclareTuple() << TYPE_INT;
  builder.DeclareTuple() << array_type;
  DescriptorTbl* desc_tbl = builder.Build();
  vector<bool> nullable_tuples(3, true);
  vector<TTupleId> tuple_id;
  tuple_id.push_back((TTupleId) 0);
  tuple_id.push_back((TTupleId) 1);
  tuple_id.push_back((TTupleId) 2);
  RowDescriptor row_desc(*desc_tbl, tuple_id, nullable_tuples);
  const int num_tuples = 3;
  const int num_int_tuples = 2;
  const int array_tuple_idx = 2;
  int huge_string_size = INT_MAX / 256;
  int num_rows = 100000; // Without deduplication this will be gigantic
  int num_distinct_array_tuples = 2;

  // Build two tuples with huge strings in them
  string huge_string;
  LOG(INFO) << "Try to resize to " << huge_string_size;
  huge_string.resize(huge_string_size, 'z');
  vector<vector<Tuple*> > tuples(num_tuples);
  vector<int> repeats(num_tuples, 1); // Don't repeat tuples adjacently
  int64_t total_byte_size = 0;
  RowBatch* batch = pool_.Add(new RowBatch(row_desc, num_rows, tracker_.get()));
  // First two tuples are integers because it doesn't matter for this test.
  for (int tuple_idx = 0; tuple_idx < num_int_tuples; ++tuple_idx) {
    TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[tuple_idx];
    CreateTuples(*tuple_desc, batch->tuple_data_pool(), num_rows, 0, false,
        &tuples[tuple_idx]);
    total_byte_size += tuple_desc->byte_size() * num_rows;
  }
  // The last tuple is a duplicated array with a large string inside.
  const TupleDescriptor* array_tuple_desc = row_desc.tuple_descriptors()[array_tuple_idx];
  const SlotDescriptor* array_slot_desc = array_tuple_desc->slots()[0];
  const TupleDescriptor* array_item_desc = array_slot_desc->collection_item_descriptor();
  const SlotDescriptor* string_slot_desc = array_item_desc->slots()[0];
  MemPool* pool = batch->tuple_data_pool();
  for (int i = 0; i < num_distinct_array_tuples; ++i) {
    uint8_t* tuple_mem = pool->Allocate(array_tuple_desc->byte_size());
    memset(tuple_mem, 0, array_tuple_desc->byte_size());
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);
    CollectionValue cv;
    cv.ptr = pool->Allocate(array_item_desc->byte_size());
    memset(cv.ptr, 0, array_item_desc->byte_size());
    cv.num_tuples = 1;
    StringValue huge_string_value((char*)huge_string.data(), huge_string_size);
    RawValue::Write(&huge_string_value, reinterpret_cast<Tuple*>(cv.ptr),
        string_slot_desc, NULL);
    RawValue::Write(&cv, tuple, array_slot_desc, NULL);
    tuples[array_tuple_idx].push_back(tuple);
    total_byte_size += array_tuple_desc->byte_size() + cv.ByteSize(*array_item_desc) +
        huge_string_size;
  }
  LOG(INFO) << "Building row batch";
  AddTuplesToRowBatch(num_rows, tuples, repeats, batch);
  // Full dedup should be automatically enabled because of row batch structure.
  EXPECT_TRUE(UseFullDedup(batch));
  LOG(INFO) << "Serializing row batch";
  TRowBatch trow_batch;
  EXPECT_TRUE(batch->Serialize(&trow_batch).ok());
  LOG(INFO) << "Serialized batch size: " << trow_batch.tuple_data.size();
  LOG(INFO) << "Serialized batch uncompressed size: " << trow_batch.uncompressed_size;
  LOG(INFO) << "Serialized batch expected size: " << total_byte_size;
  // Serialized data should only have one copy of each tuple.
  EXPECT_EQ(total_byte_size, trow_batch.uncompressed_size);
  LOG(INFO) << "Deserializing row batch";
  RowBatch deserialized_batch(row_desc, trow_batch, tracker_.get());
  LOG(INFO) << "Verifying row batch";
  // Need to do special verification: comparing all duplicate strings is too slow.
  EXPECT_EQ(batch->num_rows(), deserialized_batch.num_rows());
  for (int row_idx = 0; row_idx < batch->num_rows(); ++row_idx) {
    for (int tuple_idx = 0; tuple_idx < num_tuples; ++tuple_idx) {
      TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[tuple_idx];
      Tuple* tuple = batch->GetRow(row_idx)->GetTuple(tuple_idx);
      Tuple* deserialized_tuple = deserialized_batch.GetRow(row_idx)->GetTuple(tuple_idx);
      if (tuple_idx != array_tuple_idx || row_idx < tuples.size()) {
        // Compare tuples directly
        TestTuplesEqual(*tuple_desc, tuple, deserialized_tuple);
      } else {
        // This should just be a repeat of a previous tuple - avoid comparison.
        int prev_dup_idx = row_idx - num_distinct_array_tuples;
        EXPECT_EQ(deserialized_batch.GetRow(prev_dup_idx)->GetTuple(tuple_idx),
            deserialized_tuple);
      }
    }
  }
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
