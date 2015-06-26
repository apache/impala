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
#include "exec/kudu-scan-node.h"
#include "exec/kudu-util.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"
#include "gutil/strings/split.h"
#include "runtime/descriptors.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/test-macros.h"
#include "util/cpu-info.h"
#include "util/test-info.h"

using apache::thrift::ThriftDebugString;
using strings::Substitute;

namespace impala {

static const char* BASE_TABLE_NAME = "TestScanNodeTable";

class KuduScanNodeTest : public testing::Test {
 public:
  KuduScanNodeTest()
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

  virtual void TearDown() {
    kudu_test_helper_.DeleteTable();
  }

  void BuildRuntimeStateForScans(int num_cols_materialize) {
    TKuduScanNode kudu_scan_node_;
    kudu_node_.__set_kudu_scan_node(kudu_scan_node_);
    kudu_node_.__set_node_type(TPlanNodeType::KUDU_SCAN_NODE);
    kudu_node_.__set_limit(-1);

    kudu_test_helper_.CreateTableDescriptor(num_cols_materialize, &desc_tbl_);

    row_desc_ = obj_pool_.Add(
        new RowDescriptor(*desc_tbl_,
                          boost::assign::list_of(0),
                          boost::assign::list_of(false)));

    runtime_state_.set_desc_tbl(desc_tbl_);
  }

  void SetUpScanner(KuduScanNode* scanner, vector<TScanRangeParams>* params) {
    scanner->SetScanRanges(*params);
    ASSERT_OK(scanner->Prepare(&runtime_state_));
    ASSERT_OK(scanner->Open(&runtime_state_));
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

  static const int kNoLimit = -1;

  void ScanAndVerify(int expected_num_rows, int expected_num_batches,
      int num_cols_to_materialize, int num_notnull_cols = 3, int limit = kNoLimit) {
    BuildRuntimeStateForScans(num_cols_to_materialize);
    if (limit != kNoLimit) kudu_node_.__set_limit(limit);

    KuduScanNode scanner(&obj_pool_, kudu_node_, *desc_tbl_);

    // TODO test more than one range. This was tested but left out of the current
    // version as Kudu is currently changing the way it addresses start/stop keys (stop
    // key is now inclusive and will become exclusive).
    vector<TScanRangeParams> params;
    AddScanRange("", "", &params);

    SetUpScanner(&scanner, &params);

    int num_rows = 0;
    int num_batches = 0;
    int batch_size;
    // Even if 'limit' is equal to 0 we still need a batch size > 0.
    if (limit == 0) {
      batch_size = 1;
    } else {
      batch_size = expected_num_rows / expected_num_batches;
    }

    bool eos = false;

    do {
      RowBatch* batch = obj_pool_.Add(new RowBatch(*row_desc_, batch_size,
                                                   &mem_tracker_));
      ASSERT_OK(scanner.GetNext(&runtime_state_, batch, &eos));

      if (batch->num_rows() == 0) {
        ASSERT_TRUE(eos);
        break;
      }

      ASSERT_EQ(batch->num_rows(), batch_size);
      VerifyBatch(batch, batch_size * num_batches, batch_size * (num_batches + 1),
          num_cols_to_materialize, num_notnull_cols);
      num_rows += batch->num_rows();
      ++num_batches;
    } while(!eos);

    ASSERT_EQ(expected_num_rows, num_rows);
    ASSERT_EQ(expected_num_batches, num_batches);
  }

  void AddScanRange(const string& start_key, const string& stop_key,
      vector<TScanRangeParams>* params) {

    TKuduKeyRange kudu_key_range;
    kudu_key_range.__set_startKey(start_key);
    kudu_key_range.__set_stopKey(stop_key);

    TScanRange scan_range;
    scan_range.__set_kudu_key_range(kudu_key_range);

    TScanRangeParams scan_range_params;
    scan_range_params.__set_is_cached(false);
    scan_range_params.__set_is_remote(false);
    scan_range_params.__set_volume_id(0);
    scan_range_params.__set_scan_range(scan_range);

    params->push_back(scan_range_params);
  }

 protected:
  KuduTestHelper kudu_test_helper_;
  MemTracker mem_tracker_;
  ObjectPool obj_pool_;
  ExecEnv exec_env_;
  RuntimeState runtime_state_;
  TPlanNode kudu_node_;
  TTableDescriptor t_tbl_desc_;
  DescriptorTbl* desc_tbl_;
  RowDescriptor* row_desc_;
};

TEST_F(KuduScanNodeTest, TestScanNode) {

  const int kNumRows = 1000;
  const int kNumBatches = 10;

  // Insert kNumRows rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   kNumRows);

  // Test materializing all, some, or none of the slots
  ScanAndVerify(kNumRows, kNumBatches, 3);
  ScanAndVerify(kNumRows, kNumBatches, 2);
  ScanAndVerify(kNumRows, kNumBatches, 0);
}

TEST_F(KuduScanNodeTest, TestScanNullColValues) {

  const int kNumRows = 1000;
  const int kNumBatches = 10;

  // Insert kNumRows rows for this test but only the keys, i.e. the 2nd
  // and 3rd columns are null.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   kNumRows, 0, 1);

  // Now try scanning including and not including the null columns.
  ScanAndVerify(kNumRows, kNumBatches, 3, 1);
  ScanAndVerify(kNumRows, kNumBatches, 2, 1);
  ScanAndVerify(kNumRows, kNumBatches, 1, 1);
}

// Test for a bug where we would mishandle getting an empty string from
// Kudu and wrongfully return a MEM_LIMIT_EXCEEDED.
TEST_F(KuduScanNodeTest, TestScanEmptyString) {

  std::tr1::shared_ptr<KuduSession> session = kudu_test_helper_.client()->NewSession();
  KUDU_ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
  session->SetTimeoutMillis(10000);
  gscoped_ptr<KuduInsert> insert(kudu_test_helper_.table()->NewInsert());
  KUDU_ASSERT_OK(insert->mutable_row()->SetInt32(0, 10));
  KUDU_ASSERT_OK(insert->mutable_row()->SetString(2, ""));
  session->Apply(insert.release());
  KUDU_ASSERT_OK(session->Flush());
  ASSERT_FALSE(session->HasPendingOperations());

  BuildRuntimeStateForScans(3);
  KuduScanNode scanner(&obj_pool_, kudu_node_, *desc_tbl_);
  vector<TScanRangeParams> params;
  AddScanRange("", "", &params);
  SetUpScanner(&scanner, &params);
  bool eos = false;
  RowBatch* batch = obj_pool_.Add(new RowBatch(*row_desc_, 10, &mem_tracker_));
  ASSERT_OK(scanner.GetNext(&runtime_state_, batch, &eos));
  ASSERT_EQ(1, batch->num_rows());
  ASSERT_TRUE(eos);
  ASSERT_EQ(PrintBatch(batch), "[(10 null )]\n");
}

// Test that scan limits are enforced.
TEST_F(KuduScanNodeTest, TestLimitsAreEnforced) {
  const int kNumRows = 1000;

  // Insert kNumRows rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   kNumRows);

  // Try scanning but limit the number of returned rows to several different values.
  int limit_rows_to = 0;
  ScanAndVerify(limit_rows_to, 0, 3, 3, limit_rows_to);
  limit_rows_to = 1;
  ScanAndVerify(limit_rows_to, 1, 3, 3, limit_rows_to);
  limit_rows_to = 2000;
  ScanAndVerify(1000, 1, 3, 3, limit_rows_to);
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
