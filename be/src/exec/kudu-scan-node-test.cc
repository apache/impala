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
    kudu_scan_node_.__set_pushable_conjuncts(pushable_conjuncts_);
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

  void SetUpScanner(KuduScanNode* scan_node, vector<TScanRangeParams>* params) {
    scan_node->SetScanRanges(*params);
    ASSERT_OK(scan_node->Prepare(&runtime_state_));
    ASSERT_OK(scan_node->Open(&runtime_state_));
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

  void ScanAndVerify(int first_row, int expected_num_rows, int expected_num_batches,
      int num_cols_to_materialize, int num_notnull_cols = 3, int limit = NO_LIMIT,
      bool verify = true) {
    BuildRuntimeStateForScans(num_cols_to_materialize);
    if (limit != NO_LIMIT) kudu_node_.__set_limit(limit);

    KuduScanNode scanner(&obj_pool_, kudu_node_, *desc_tbl_);

    // TODO test more than one range. This was tested but left out of the current
    // version as Kudu is currently changing the way it addresses start/stop keys (stop
    // key is now inclusive and will become exclusive).
    vector<TScanRangeParams> params;
    AddScanRange("", "", &params);

    SetUpScanner(&scanner, &params);

    int num_rows = 0;
    int num_batches = 0;
    bool eos = false;

    do {
      RowBatch* batch = obj_pool_.Add(new RowBatch(*row_desc_, DEFAULT_ROWS_PER_BATCH,
          &mem_tracker_));
      ASSERT_OK(scanner.GetNext(&runtime_state_, batch, &eos));

      if (batch->num_rows() == 0) {
        ASSERT_TRUE(eos);
        break;
      }

      if (verify) {
        VerifyBatch(batch, first_row, first_row + batch->num_rows(),
            num_cols_to_materialize, num_notnull_cols);
      }
      num_rows += batch->num_rows();
      first_row += batch->num_rows();
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
  vector<TExpr> pushable_conjuncts_;
};

TEST_F(KuduScanNodeTest, TestScanNode) {

  const int NUM_BATCHES = 5;
  const int NUM_ROWS = DEFAULT_ROWS_PER_BATCH * NUM_BATCHES;
  const int FIRST_ROW = 0;

  // Insert NUM_ROWS rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS);

  // Test materializing all, some, or none of the slots.
  ScanAndVerify(FIRST_ROW, NUM_ROWS, NUM_BATCHES, 3);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, NUM_BATCHES, 2);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, NUM_BATCHES, 0);
}

TEST_F(KuduScanNodeTest, TestScanNullColValues) {

  const int NUM_ROWS = 1000;
  const int NUM_BATCHES = 1;
  const int FIRST_ROW = 0;

  // Insert NUM_ROWS rows for this test but only the keys, i.e. the 2nd
  // and 3rd columns are null.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS, 0, 1);

  // Try scanning including and not including the null columns.
  ScanAndVerify(FIRST_ROW, NUM_ROWS, NUM_BATCHES, 3, 1);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, NUM_BATCHES, 2, 1);
  ScanAndVerify(FIRST_ROW, NUM_ROWS, NUM_BATCHES, 1, 1);
}

namespace {

// Sets a binary predicate, based on 'type', to the plan node. This will be transformed
// into a range predicate by KuduScanNode.
void AddExpressionNodesToExpression(TExpr* expression, int slot_id, const string& op_name,
    TExprNodeType::type constant_type, const void* value) {

  vector<TExprNode> nodes;

  // Build the op node.
  TExprNode function_node;
  function_node.__set_node_type(TExprNodeType::FUNCTION_CALL);
  TFunctionName function_name;
  function_name.__set_function_name(op_name);
  TFunction function;
  function.__set_name(function_name);
  function_node.__set_fn(function);

  nodes.push_back(function_node);

  // Add the slof ref.
  TExprNode slot_ref_node;
  slot_ref_node.__set_node_type(TExprNodeType::SLOT_REF);
  TSlotRef slot_ref;
  slot_ref.__set_slot_id(slot_id);
  slot_ref_node.__set_slot_ref(slot_ref);

  nodes.push_back(slot_ref_node);

  TExprNode constant_node;
  constant_node.__set_node_type(constant_type);
  // Add the constant part.
  switch(constant_type) {
    // We only add the boilerplate for the types used in the test schema.
    case TExprNodeType::INT_LITERAL: {
      TIntLiteral int_literal;
      int_literal.__set_value(*reinterpret_cast<const int64_t*>(value));
      constant_node.__set_int_literal(int_literal);
      break;
    }
    case TExprNodeType::STRING_LITERAL: {
      TStringLiteral string_literal;
      string_literal.__set_value(*reinterpret_cast<const string*>(value));
      constant_node.__set_string_literal(string_literal);
      break;
    }
    default:
      LOG(FATAL) << "Unsupported function type";
  }

  nodes.push_back(constant_node);
  expression->__set_nodes(nodes);
}

} // anonymous namespace


// Test a >= predicate on the Key.
TEST_F(KuduScanNodeTest, TestPushIntGEPredicateOnKey) {
  const int NUM_ROWS_TO_INSERT = 1000;
  const int NUM_BATCHES = 1;
  const int SLOT_ID = 2;
  const int MAT_COLS = 3;
  const int FIRST_ROW = 500;
  const int EXPECTED_NUM_ROWS = 500;

  // Insert kNumRows rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS_TO_INSERT);

  const int64_t PREDICATE_VALUE = 500;

  // Now test having a pushable predicate on the key.
  TExpr conjunct;
  AddExpressionNodesToExpression(&conjunct, SLOT_ID, KuduScanNode::GE_FN,
      TExprNodeType::INT_LITERAL, &PREDICATE_VALUE);
  pushable_conjuncts_.push_back(conjunct);

  ScanAndVerify(FIRST_ROW, EXPECTED_NUM_ROWS, NUM_BATCHES, NUM_BATCHES, MAT_COLS);
}

// Test a == predicate on the 2nd column.
TEST_F(KuduScanNodeTest, TestPushIntEQPredicateOn2ndColumn) {
  const int NUM_ROWS_TO_INSERT = 1000;
  const int NUM_BATCHES = 1;
  const int SLOT_ID = 3;
  const int MAT_COLS = 3;

  const int FIRST_ROW = 500;
  const int EXPECTED_NUM_ROWS = 1;

  // Insert kNumRows rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS_TO_INSERT);

  // When rows are added col2 is 2 * col1 so we multiply by two to get back
  // first row = 500.
  const int64_t PREDICATE_VAL = FIRST_ROW * 2;

  // Now test having a pushable predicate on the 2nd column.
  TExpr conjunct;
  AddExpressionNodesToExpression(&conjunct, SLOT_ID, KuduScanNode::EQ_FN,
      TExprNodeType::INT_LITERAL, &PREDICATE_VAL);
  pushable_conjuncts_.push_back(conjunct);

  ScanAndVerify(FIRST_ROW, EXPECTED_NUM_ROWS, NUM_BATCHES, NUM_BATCHES, MAT_COLS);
}

TEST_F(KuduScanNodeTest, TestPushStringLEPredicateOn3rdColumn) {
  const int NUM_ROWS_TO_INSERT = 1000;
  const int NUM_BATCHES = 1;
  const int SLOT_ID = 4;
  const int MAT_COLS = 3;
  // This predicate won't return consecutive rows so we just assert on the count.
  const bool VERIFY_ROWS = false;

  const int FIRST_ROW = 0;
  const string PREDICATE_VAL = "hello_500";

  // We expect 448 rows as Kudu will lexicographically compare the predicate value to
  // column values. We can expect to obtain rows 1-500, inclusive, except numbers
  // 6-9 (hello_6 > hello_500) and numbers 51-99 for a total of 52 excluded numbers.
  const int EXPECTED_NUM_ROWS = 448;


  // Insert kNumRows rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS_TO_INSERT);

  // Now test having a pushable predicate on the third column.
  TExpr conjunct;
  AddExpressionNodesToExpression(&conjunct, SLOT_ID, KuduScanNode::LE_FN,
      TExprNodeType::STRING_LITERAL, &PREDICATE_VAL);
  pushable_conjuncts_.push_back(conjunct);

  ScanAndVerify(FIRST_ROW, EXPECTED_NUM_ROWS, NUM_BATCHES, MAT_COLS, MAT_COLS, NO_LIMIT,
                VERIFY_ROWS);
}

TEST_F(KuduScanNodeTest, TestPushTwoPredicatesOnNonMaterializedColumn) {
  const int NUM_ROWS_TO_INSERT = 1000;
  const int NUM_BATCHES = 1;
  const int SLOT_ID = 3;
  const int MAT_COLS = 1;

  const int FIRST_ROW = 251;

  const int EXPECTED_NUM_ROWS = 500;

  // Insert kNumRows rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS_TO_INSERT);


  const int64_t PREDICATE_VAL_LOW = 501;
  const int64_t PREDICATE_VAL_HIGH = 1500;

  // Now test having a pushable predicate on the 2nd column, but materialize only the key
  // (e.g. select key from test_table where int_val >= 250 and int_val <= 750).
  TExpr conjunct_low;
  AddExpressionNodesToExpression(&conjunct_low, SLOT_ID, KuduScanNode::GE_FN,
      TExprNodeType::INT_LITERAL, &PREDICATE_VAL_LOW);

  TExpr conjunct_high;
  AddExpressionNodesToExpression(&conjunct_high, SLOT_ID, KuduScanNode::LE_FN,
      TExprNodeType::INT_LITERAL, &PREDICATE_VAL_HIGH);

  pushable_conjuncts_.push_back(conjunct_low);
  pushable_conjuncts_.push_back(conjunct_high);

  ScanAndVerify(FIRST_ROW, EXPECTED_NUM_ROWS, NUM_BATCHES, MAT_COLS);
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
  const int NUM_ROWS = 1000;
  const int FIRST_ROW = 0;

  // Insert NUM_ROWS rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS);

  // Try scanning but limit the number of returned rows to several different values.
  int limit_rows_to = 0;
  ScanAndVerify(FIRST_ROW, limit_rows_to, 0, 3, 3, limit_rows_to);
  limit_rows_to = 1;
  ScanAndVerify(FIRST_ROW, limit_rows_to, 1, 3, 3, limit_rows_to);
  limit_rows_to = 2000;
  ScanAndVerify(FIRST_ROW, 1000, 1, 3, 3, limit_rows_to);
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
