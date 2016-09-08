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

#include "exec/kudu-testutil.h"

#include "common/init.h"
#include "common/object-pool.h"
#include "codegen/llvm-codegen.h"
#include "exec/kudu-scan-node.h"
#include "exec/kudu-util.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"
#include "gutil/strings/split.h"
#include "runtime/descriptors.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/runtime-state.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/test-macros.h"
#include "util/cpu-info.h"
#include "util/test-info.h"

DEFINE_bool(skip_delete_table, false, "Skips deleting the tables at the end of the tests.");
DEFINE_string(use_existing_table, "", "The name of the existing table to use.");
DECLARE_bool(pick_only_leaders_for_tests);

using apache::thrift::ThriftDebugString;
using strings::Substitute;

namespace impala {

static const char* BASE_TABLE_NAME = "TestScanNodeTable";
// The id of the slot that contains the key, in the test schema.
const int KEY_SLOT_ID = 1;

// TODO: Remove after the Kudu EE tests are providing sufficient coverage (IMPALA-3718).
class KuduScanNodeTest : public testing::Test {
 public:
  virtual void SetUp() {
    // Create a Kudu client and the table (this will abort the test here
    // if a Kudu cluster is not available).
    kudu_test_helper_.CreateClient();
    mem_tracker_.reset(new MemTracker());
    exec_env_.reset(new ExecEnv());

    // Initialize the environment/runtime so that we can use a scan node in
    // isolation.
    Status s = exec_env_->InitForFeTests();
    DCHECK(s.ok());
    exec_env_->disk_io_mgr()->Init(mem_tracker_.get());

    FLAGS_pick_only_leaders_for_tests = true;
    if (FLAGS_use_existing_table != "") FLAGS_skip_delete_table = true;
  }

  virtual void TearDown() {
    if (kudu_test_helper_.table() && !FLAGS_skip_delete_table) {
      kudu_test_helper_.DeleteTable();
    }
  }

  void BuildRuntimeStateForScans(int num_cols_materialize) {
    obj_pool_.reset(new ObjectPool());

    // If we had a RuntimeState previously we need to unregister it from the thread
    // pool, or the new RuntimeState will only have access to half the threads.
    if (runtime_state_) {
      exec_env_->thread_mgr()->UnregisterPool(runtime_state_->resource_pool());
    }

    runtime_state_.reset(new RuntimeState(TExecPlanFragmentParams(), "", exec_env_.get()));
    runtime_state_->InitMemTrackers(TUniqueId(), NULL, -1);

    TKuduScanNode kudu_scan_node_;
    kudu_node_.reset(new TPlanNode);

    kudu_node_->__set_kudu_scan_node(kudu_scan_node_);
    kudu_node_->__set_node_type(TPlanNodeType::KUDU_SCAN_NODE);
    kudu_node_->__set_limit(-1);
    kudu_node_->row_tuples.push_back(0);
    kudu_node_->nullable_tuples.push_back(false);

    kudu_test_helper_.CreateTableDescriptor(num_cols_materialize, &desc_tbl_);

    runtime_state_->set_desc_tbl(desc_tbl_);
  }

  void SetUpScanner(KuduScanNode* scan_node, const vector<TScanRangeParams>& params) {
    scan_node->SetScanRanges(params);
    ASSERT_OK(scan_node->Prepare(runtime_state_.get()));
    ASSERT_OK(scan_node->Open(runtime_state_.get()));
  }

  void VerifyBatch(RowBatch* batch, int first_row, int last_row,
      int num_materialized_cols, int num_notnull_cols) {

    if (num_materialized_cols == 0) return;
    string batch_as_string = PrintBatch(batch);

    DCHECK_GE(num_materialized_cols, 0);
    DCHECK_LE(num_materialized_cols, kudu_test_helper_.test_schema().num_columns());

    string base_row = "[(";
    if (num_materialized_cols >= 1) base_row.append("$0");
    if (num_materialized_cols >= 2) base_row.append(" $1");
    if (num_materialized_cols >= 3) base_row.append(" $2");
    base_row.append(")]");

    vector<string> rows = strings::Split(batch_as_string, "\n", strings::SkipEmpty());
    ASSERT_EQ(rows.size(), last_row - first_row);
    for (int i = 0; i < rows.size(); ++i) {
      int idx = first_row + i;
      string row;
      switch(num_materialized_cols) {
        case 1: row = Substitute(base_row, idx); break;
        case 2: {
          if (num_notnull_cols > 1) {
            row = Substitute(base_row, idx, idx * 2);
            break;
          } else {
            row = Substitute(base_row, idx, "null");
            break;
          }
        }
        case 3: {
          if (num_notnull_cols > 2) {
            row = Substitute(base_row, idx, idx * 2, Substitute("hello_$0", idx));
            break;
          } else {
            row = Substitute(base_row, idx, "null", "null");
            break;
          }
        }
      }
      ASSERT_EQ(rows[i], row);
    }
  }

  static const int NO_LIMIT = -1;
  static const int DEFAULT_ROWS_PER_BATCH = 1024;

  struct batch_less_than {
    int tuple_offset;

    int GetKeyValue(RowBatch* batch) const {
      return *reinterpret_cast<int32_t*>(
          batch->GetRow(0)->GetTuple(0)->GetSlot(tuple_offset));
    }

    inline bool operator() (RowBatch* left, RowBatch* right) const {
      return GetKeyValue(left) < GetKeyValue(right);
    }
  };

  void CreateScanRangeParams(int num_cols_to_materialize,
      vector<TScanRangeParams>* params) {
    DCHECK_GE(num_cols_to_materialize, 0);
    DCHECK_LE(num_cols_to_materialize, 3);

    kudu::client::KuduScanTokenBuilder b(kudu_test_helper_.table().get());
    vector<string> col_names;
    if (num_cols_to_materialize > 0) col_names.push_back("key");
    if (num_cols_to_materialize > 1) col_names.push_back("int_val");
    if (num_cols_to_materialize > 2) col_names.push_back("string_val");
    KUDU_ASSERT_OK(b.SetProjectedColumnNames(col_names));

    vector<kudu::client::KuduScanToken*> tokens;
    KUDU_ASSERT_OK(b.Build(&tokens));

    for (kudu::client::KuduScanToken* token: tokens) {
      TScanRange range;
      string buf;
      KUDU_ASSERT_OK(token->Serialize(&buf));
      range.__set_kudu_scan_token(buf);

      TScanRangeParams scan_range_params;
      scan_range_params.__set_is_cached(false);
      scan_range_params.__set_is_remote(false);
      scan_range_params.__set_volume_id(0);
      scan_range_params.__set_scan_range(range);
      params->push_back(scan_range_params);
    }
  }

  void ScanAndVerify(int first_row, int expected_num_rows, int num_cols_to_materialize,
      int num_notnull_cols = 3, int limit = NO_LIMIT, bool verify = true) {
    vector<TScanRangeParams> params;
    CreateScanRangeParams(num_cols_to_materialize, &params);

    BuildRuntimeStateForScans(num_cols_to_materialize);
    if (limit != NO_LIMIT) kudu_node_->__set_limit(limit);

    KuduScanNode scanner(obj_pool_.get(), *kudu_node_, *desc_tbl_);
    SetUpScanner(&scanner, params);

    int num_rows = 0;
    bool eos = false;
    vector<RowBatch*> row_batches;

    do {
      RowBatch* batch = obj_pool_->Add(new RowBatch(scanner.row_desc(),
          DEFAULT_ROWS_PER_BATCH, mem_tracker_.get()));
      ASSERT_OK(scanner.GetNext(runtime_state_.get(), batch, &eos));

      if (batch->num_rows() == 0) {
        ASSERT_TRUE(eos);
        break;
      }

      row_batches.push_back(batch);
      num_rows += batch->num_rows();
    } while(!eos);

    if (verify) {
      batch_less_than comp;
      comp.tuple_offset = runtime_state_->desc_tbl().
          GetSlotDescriptor(KEY_SLOT_ID)->tuple_offset();

      std::sort(row_batches.begin(), row_batches.end(), comp);
      for (RowBatch* batch: row_batches) {
        VerifyBatch(batch, first_row, first_row + batch->num_rows(),
            num_cols_to_materialize, num_notnull_cols);
        first_row += batch->num_rows();
      }
    }

    ASSERT_EQ(expected_num_rows, num_rows);
    scanner.Close(runtime_state_.get());
  }

 protected:
  KuduTestHelper kudu_test_helper_;
  boost::scoped_ptr<MemTracker> mem_tracker_;
  boost::scoped_ptr<ObjectPool> obj_pool_;
  boost::scoped_ptr<ExecEnv> exec_env_;
  boost::scoped_ptr<RuntimeState> runtime_state_;
  boost::scoped_ptr<TPlanNode> kudu_node_;
  TTableDescriptor t_tbl_desc_;
  DescriptorTbl* desc_tbl_;
};

TEST_F(KuduScanNodeTest, TestScanNode) {
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  const int NUM_ROWS = DEFAULT_ROWS_PER_BATCH * 5;
  const int FIRST_ROW = 0;

  // Insert NUM_ROWS rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS);


  // Test materializing all, some, or none of the slots.
  ScanAndVerify(FIRST_ROW, NUM_ROWS, 3);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, 2);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, 0, /* 0 non-null cols */0, NO_LIMIT,
      /* Don't verify */false);
}

TEST_F(KuduScanNodeTest, TestScanNullColValues) {
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  const int NUM_ROWS = 1000;
  const int FIRST_ROW = 0;

  // Insert NUM_ROWS rows for this test but only the keys, i.e. the 2nd
  // and 3rd columns are null.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS, 0, 1);

  // Try scanning including and not including the null columns.
  ScanAndVerify(FIRST_ROW, NUM_ROWS, 3, 1);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, 2, 1);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, 1, 1);
}

// Test for a bug where we would mishandle getting an empty string from
// Kudu and wrongfully return a MEM_LIMIT_EXCEEDED.
TEST_F(KuduScanNodeTest, TestScanEmptyString) {
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  std::tr1::shared_ptr<KuduSession> session = kudu_test_helper_.client()->NewSession();
  KUDU_ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(10000);
  gscoped_ptr<KuduInsert> insert(kudu_test_helper_.table()->NewInsert());
  KUDU_ASSERT_OK(insert->mutable_row()->SetInt32(0, 10));
  KUDU_ASSERT_OK(insert->mutable_row()->SetString(2, ""));
  KUDU_ASSERT_OK(session->Apply(insert.release()));
  KUDU_ASSERT_OK(session->Flush());
  ASSERT_FALSE(session->HasPendingOperations());

  int num_cols_to_materialize = 3;
  BuildRuntimeStateForScans(num_cols_to_materialize);
  KuduScanNode scanner(obj_pool_.get(), *kudu_node_, *desc_tbl_);

  vector<TScanRangeParams> params;
  CreateScanRangeParams(num_cols_to_materialize, &params);
  SetUpScanner(&scanner, params);
  RowBatch* batch = obj_pool_->Add(
      new RowBatch(scanner.row_desc(), DEFAULT_ROWS_PER_BATCH, mem_tracker_.get()));
  bool eos = false;
  int total_num_rows = 0;

  // Need to call GetNext() a total of 3 times to allow for:
  // 1) the row batch containing the single row
  // 2) an empty row batch (since there are scanners created for both tablets)
  // 3) a final call which returns eos.
  // The first two may occur in any order and are checked in the for loop below.
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(scanner.GetNext(runtime_state_.get(), batch, &eos));
    ASSERT_FALSE(eos);
    if (batch->num_rows() > 0) {
      total_num_rows += batch->num_rows();
      ASSERT_EQ(PrintBatch(batch), "[(10 null )]\n");
    }
    batch->Reset();
  }
  ASSERT_EQ(1, total_num_rows);

  // One last call to find the batch queue empty and GetNext() returns eos.
  ASSERT_OK(scanner.GetNext(runtime_state_.get(), batch, &eos));
  ASSERT_TRUE(eos);
  ASSERT_EQ(0, batch->num_rows());

  scanner.Close(runtime_state_.get());
}

// Test that scan limits are enforced.
TEST_F(KuduScanNodeTest, TestLimitsAreEnforced) {
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  const int NUM_ROWS = 1000;
  const int FIRST_ROW = 0;

  // Insert NUM_ROWS rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS);

  // Try scanning but limit the number of returned rows to several different values.
  int limit_rows_to = 0;
  ScanAndVerify(FIRST_ROW, limit_rows_to, 3, 3, limit_rows_to);
  limit_rows_to = 1;
  ScanAndVerify(FIRST_ROW, limit_rows_to, 3, 3, limit_rows_to);
  limit_rows_to = 2000;
  ScanAndVerify(FIRST_ROW, 1000, 3, 3, limit_rows_to);
}

} // namespace impala

int main(int argc, char** argv) {
  if (!impala::KuduClientIsSupported()) return 0;
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  impala::InitKuduLogging();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
