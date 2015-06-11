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

#include "exec/kudu-testutil.h"

#include "common/init.h"
#include "common/object-pool.h"
#include "codegen/llvm-codegen.h"
#include "exec/kudu-table-sink.h"
#include "exec/kudu-util.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"
#include "gutil/strings/split.h"
#include "kudu/client/row_result.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/test-macros.h"
#include "util/cpu-info.h"
#include "util/test-info.h"

using apache::thrift::ThriftDebugString;

namespace impala {

static const char* BASE_TABLE_NAME = "TestInsertNodeTable";
static const int FIRST_SLOT_ID = 2;
static const int SECOND_SLOT_ID = 3;
static const int THIRD_SLOT_ID = 4;

class KuduTableSinkTest : public testing::Test {
 public:
  KuduTableSinkTest()
      : runtime_state_(TPlanFragmentInstanceCtx(), "", &exec_env_) {}

  virtual void SetUp() {
    // Create a Kudu client and the table (this will abort the test here
    // if a Kudu cluster is not available).
    kudu_test_helper_.CreateClient();
    kudu_test_helper_.CreateTable(BASE_TABLE_NAME);

    // Initialize the environment/runtime so that we can use a scan node in
    // isolation.
    DCHECK_OK(exec_env_.InitForFeTests());
    runtime_state_.InitMemTrackers(TUniqueId(), NULL, -1);
    exec_env_.disk_io_mgr()->Init(&mem_tracker_);
  }

  void BuildRuntimeStateForInsert(int num_cols_to_insert) {
    TTableSink table_sink;
    table_sink.__set_target_table_id(0);
    table_sink.__set_type(TTableSinkType::KUDU_INSERT);

    data_sink_.__set_type(TDataSinkType::TABLE_SINK);
    data_sink_.__set_table_sink(table_sink);

    kudu_test_helper_.CreateTableDescriptor(num_cols_to_insert, &desc_tbl_);

    row_desc_ = obj_pool_.Add(
        new RowDescriptor(*desc_tbl_,
                          boost::assign::list_of(0),
                          boost::assign::list_of(false)));

    runtime_state_.set_desc_tbl(desc_tbl_);
  }

  void CreateTExprNode(int slot_id, TPrimitiveType::type type, TExpr* expr) {
    TExprNode expr_node;
    expr_node.node_type = TExprNodeType::SLOT_REF;
    expr_node.type.types.push_back(TTypeNode());
    expr_node.type.types.back().__isset.scalar_type = true;
    expr_node.type.types.back().scalar_type.type = type;
    expr_node.num_children = 0;
    TSlotRef slot_ref;
    slot_ref.slot_id = slot_id;
    expr_node.__set_slot_ref(slot_ref);
    expr->nodes.push_back(expr_node);
  }

  void CreateTExpr(int num_cols_to_insert, vector<TExpr>* exprs) {
    DCHECK(num_cols_to_insert > 0 && num_cols_to_insert <= 3);
    TExpr expr_1;
    CreateTExprNode(FIRST_SLOT_ID, TPrimitiveType::INT, &expr_1);
    exprs->push_back(expr_1);
    if (num_cols_to_insert == 1) return;
    TExpr expr_2;
    CreateTExprNode(SECOND_SLOT_ID, TPrimitiveType::INT, &expr_2);
    exprs->push_back(expr_2);
    if (num_cols_to_insert == 2) return;
    TExpr expr_3;
    CreateTExprNode(THIRD_SLOT_ID, TPrimitiveType::STRING, &expr_3);
    exprs->push_back(expr_3);
  }

  // Create a batch and fill it with 'num_cols_to_insert' columns.
  RowBatch* CreateRowBatch(int first_row, int batch_size, int num_cols_to_insert) {
    DCHECK(desc_tbl_->GetTupleDescriptor(0) != NULL);
    TupleDescriptor* tuple_desc = desc_tbl_->GetTupleDescriptor(0);
    RowBatch* batch = new RowBatch(*row_desc_, batch_size, &mem_tracker_);
    int tuple_buffer_size = batch->capacity() * tuple_desc->byte_size();
    void* tuple_buffer_ = batch->tuple_data_pool()->TryAllocate(tuple_buffer_size);
    DCHECK(tuple_buffer_ != NULL);
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_buffer_);

    bzero(tuple_buffer_, tuple_buffer_size);
    for (int i = 0; i < batch_size; ++i) {
      int idx = batch->AddRow();
      TupleRow* row = batch->GetRow(idx);
      row->SetTuple(0, tuple);

      for (int j = 0; j < tuple_desc->slots().size(); j++) {
        DCHECK(tuple->GetSlot(tuple_desc->slots()[j]->tuple_offset()) != NULL);
        void* slot = tuple->GetSlot(tuple_desc->slots()[j]->tuple_offset());
        switch(j) {
          case 0: {
            int32_t* int_slot = reinterpret_cast<int32_t*>(slot);
            *int_slot = first_row + i;
            break;
          }
          case 1: {
            int32_t* int_slot = reinterpret_cast<int32_t*>(slot);
            *int_slot = (first_row + i) * 2;
            break;
          }
          case 2: {
            string value = strings::Substitute("hello_$0", first_row + i);
            Slice slice(value);

            char* buffer = reinterpret_cast<char*>(
                batch->tuple_data_pool()->TryAllocate(slice.size()));
            DCHECK(buffer != NULL);
            memcpy(buffer, slice.data(), slice.size());
            reinterpret_cast<StringValue*>(slot)->ptr = buffer;
            reinterpret_cast<StringValue*>(slot)->len = slice.size();
            break;
          }
          default:
            DCHECK(false) << "Wrong number of slots.";
        }
      }
      batch->CommitLastRow();
      tuple = reinterpret_cast<Tuple*>(tuple + tuple_desc->byte_size());
    }
    return batch;
  }

  void Verify(int num_columns, int expected_num_rows) {
    kudu::client::KuduScanner scanner(kudu_test_helper_.table().get());
    scanner.SetReadMode(kudu::client::KuduScanner::READ_AT_SNAPSHOT);
    scanner.SetOrderMode(kudu::client::KuduScanner::ORDERED);
    KUDU_ASSERT_OK(scanner.Open());
    int row_idx = 0;
    while (scanner.HasMoreRows()) {
      vector<kudu::client::KuduRowResult> rows;
      KUDU_ASSERT_OK(scanner.NextBatch(&rows));
      BOOST_FOREACH(const kudu::client::KuduRowResult& row, rows) {
        switch(num_columns) {
          case 1:
            ASSERT_EQ(row.ToString(), strings::Substitute(
                "(int32 key=$0, int32 int_val=NULL, string string_val=NULL)", row_idx));
            break;
          case 2:
            ASSERT_EQ(row.ToString(), strings::Substitute(
                "(int32 key=$0, int32 int_val=$1, string string_val=NULL)",
                row_idx, row_idx * 2));
            break;
          case 3:
            ASSERT_EQ(row.ToString(), strings::Substitute(
                "(int32 key=$0, int32 int_val=$1, string string_val=hello_$2)",
                row_idx, row_idx * 2, row_idx));
            break;
        }
        ++row_idx;
      }
    }
    ASSERT_EQ(row_idx, expected_num_rows);
  }

  void InsertAndVerify(int num_columns) {
    const int kNumRowsPerBatch = 10;

    BuildRuntimeStateForInsert(num_columns);
    vector<TExpr> exprs;
    CreateTExpr(num_columns, &exprs);
    KuduTableSink sink(*row_desc_, exprs, data_sink_);
    ASSERT_OK(sink.Prepare(&runtime_state_));
    ASSERT_OK(sink.Open(&runtime_state_));
    ASSERT_OK(sink.Send(&runtime_state_,
        CreateRowBatch(0, kNumRowsPerBatch, num_columns), false));
    ASSERT_OK(sink.Send(&runtime_state_,
        CreateRowBatch(kNumRowsPerBatch, kNumRowsPerBatch, num_columns), true));
    sink.Close(&runtime_state_);
    Verify(num_columns, 2 * kNumRowsPerBatch);
  }

  virtual void TearDown() {
    kudu_test_helper_.DeleteTable();
  }

 protected:
  KuduTestHelper kudu_test_helper_;
  MemTracker mem_tracker_;
  ObjectPool obj_pool_;
  ExecEnv exec_env_;
  RuntimeState runtime_state_;
  TDataSink data_sink_;
  TTableDescriptor t_tbl_desc_;
  DescriptorTbl* desc_tbl_;
  RowDescriptor* row_desc_;
};

TEST_F(KuduTableSinkTest, TestInsertJustKey) {
  InsertAndVerify(1);
}

TEST_F(KuduTableSinkTest, TestInsertTwoCols) {
  InsertAndVerify(2);
}

TEST_F(KuduTableSinkTest, TestInsertAllCols) {
  InsertAndVerify(3);
}

} // namespace impala

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  impala::InitKuduLogging();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
