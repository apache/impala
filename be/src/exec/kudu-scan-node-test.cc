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

DEFINE_bool(run_scan_bench, false, "Whether to run the Kudu scan micro benchmarks. Note:"
    " These tests are very slow.");
DEFINE_bool(bench_strings, true, "Whether to scan string columns in benchmarks.");
DEFINE_int32(bench_num_rows, 10 * 1000 * 1000, "Num rows to insert in benchmarks.");
DEFINE_int32(bench_num_splits, 40, "Num tablets to create in benchmarks");
DEFINE_int32(bench_num_runs, 10, "Num times to run each benchmark.");
DEFINE_bool(skip_delete_table, false, "Skips deleting the tables at the end of the tests.");
DEFINE_string(use_existing_table, "", "The name of the existing table to use.");
DECLARE_bool(pick_only_leaders_for_tests);

using apache::thrift::ThriftDebugString;
using strings::Substitute;

namespace impala {

static const char* BASE_TABLE_NAME = "TestScanNodeTable";
// The id of the slot that contains the key, in the test schema.
const int KEY_SLOT_ID = 1;

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
    kudu_scan_node_.__set_kudu_conjuncts(pushable_conjuncts_);

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

  void ScanAndVerify(const vector<TScanRangeParams>& params, int first_row,
      int expected_num_rows, int num_cols_to_materialize,
      int num_notnull_cols = 3, int limit = NO_LIMIT, bool verify = true) {
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

  // Adds a new TScanRangeParams to 'params'.
  // If start_key and/or stop_key are '-1' an empty key string is passed
  // instead.
  void AddScanRange(int start_key, int stop_key,
      vector<TScanRangeParams>* params) {

    string encoded_start_key;
    if (start_key != -1) {
      scoped_ptr<KuduPartialRow> start_key_row(kudu_test_helper_.test_schema().NewRow());
      start_key_row->SetInt32(0, start_key);
      start_key_row->EncodeRowKey(&encoded_start_key);
    } else {
      encoded_start_key = "";
    }

    string encoded_stop_key;
    if (stop_key != -1) {
      gscoped_ptr<KuduPartialRow> stop_key_row(kudu_test_helper_.test_schema().NewRow());
      stop_key_row->SetInt32(0, stop_key);
      stop_key_row->EncodeRowKey(&encoded_stop_key);
    } else {
      encoded_stop_key = "";
    }

    TKuduKeyRange kudu_key_range;
    kudu_key_range.__set_range_start_key(encoded_start_key);
    kudu_key_range.__set_range_stop_key(encoded_stop_key);

    TScanRange scan_range;
    scan_range.__set_kudu_key_range(kudu_key_range);

    TScanRangeParams scan_range_params;
    scan_range_params.__set_is_cached(false);
    scan_range_params.__set_is_remote(false);
    scan_range_params.__set_volume_id(0);
    scan_range_params.__set_scan_range(scan_range);

    params->push_back(scan_range_params);
  }

  void DoBenchmarkScan(const string& name, const vector<TScanRangeParams>& params,
                       int num_cols);

 protected:
  KuduTestHelper kudu_test_helper_;
  boost::scoped_ptr<MemTracker> mem_tracker_;
  boost::scoped_ptr<ObjectPool> obj_pool_;
  boost::scoped_ptr<ExecEnv> exec_env_;
  boost::scoped_ptr<RuntimeState> runtime_state_;
  boost::scoped_ptr<TPlanNode> kudu_node_;
  TTableDescriptor t_tbl_desc_;
  DescriptorTbl* desc_tbl_;
  vector<TExpr> pushable_conjuncts_;
};

TEST_F(KuduScanNodeTest, TestScanNode) {
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  const int NUM_ROWS = DEFAULT_ROWS_PER_BATCH * 5;
  const int FIRST_ROW = 0;

  // Insert NUM_ROWS rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS);


  // Test having multiple scan ranges, the range is split by tablet boundaries. Default
  // split is at '5'.
  int mid_key = 5;
  vector<TScanRangeParams> params;
  AddScanRange(FIRST_ROW, mid_key, &params);
  AddScanRange(mid_key, NUM_ROWS, &params);

  // Test materializing all, some, or none of the slots.
  ScanAndVerify(params, FIRST_ROW, NUM_ROWS, 3);
  ScanAndVerify(params, FIRST_ROW, NUM_ROWS, 2);
  ScanAndVerify(params, FIRST_ROW, NUM_ROWS, 0, /* 0 non-null cols */0, NO_LIMIT,
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

  vector<TScanRangeParams> params;
  AddScanRange(-1, -1, &params);

  // Try scanning including and not including the null columns.
  ScanAndVerify(params, FIRST_ROW, NUM_ROWS, 3, 1);
  ScanAndVerify(params, FIRST_ROW, NUM_ROWS, 2, 1);
  ScanAndVerify(params, FIRST_ROW, NUM_ROWS, 1, 1);
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
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  const int NUM_ROWS_TO_INSERT = 1000;
  const int SLOT_ID = 1;
  const int MAT_COLS = 3;
  const int FIRST_ROW = 500;
  const int EXPECTED_NUM_ROWS = 500;

  // Insert kNumRows rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   NUM_ROWS_TO_INSERT);

  const int64_t PREDICATE_VALUE = 500;

  // Test having a pushable predicate on the key (key >= PREDICATE_VALUE).
  TExpr conjunct;
  AddExpressionNodesToExpression(&conjunct, SLOT_ID, KuduScanNode::GE_FN,
      TExprNodeType::INT_LITERAL, &PREDICATE_VALUE);
  pushable_conjuncts_.push_back(conjunct);

  vector<TScanRangeParams> params;
  AddScanRange(-1, -1, &params);

  ScanAndVerify(params, FIRST_ROW, EXPECTED_NUM_ROWS, MAT_COLS);
}

// Test a == predicate on the 2nd column.
TEST_F(KuduScanNodeTest, TestPushIntEQPredicateOn2ndColumn) {
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  const int NUM_ROWS_TO_INSERT = 1000;
  const int SLOT_ID = 2;
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

  vector<TScanRangeParams> params;
  AddScanRange(-1, -1, &params);

  ScanAndVerify(params, FIRST_ROW, EXPECTED_NUM_ROWS, MAT_COLS);
}

TEST_F(KuduScanNodeTest, TestPushStringLEPredicateOn3rdColumn) {
  kudu_test_helper_.CreateTable(BASE_TABLE_NAME);
  const int NUM_ROWS_TO_INSERT = 1000;
  const int SLOT_ID = 3;
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

  vector<TScanRangeParams> params;
  AddScanRange(-1, -1, &params);

  ScanAndVerify(params, FIRST_ROW, EXPECTED_NUM_ROWS, MAT_COLS, MAT_COLS,
                NO_LIMIT, VERIFY_ROWS);
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
  session->Apply(insert.release());
  KUDU_ASSERT_OK(session->Flush());
  ASSERT_FALSE(session->HasPendingOperations());

  BuildRuntimeStateForScans(3);
  KuduScanNode scanner(obj_pool_.get(), *kudu_node_, *desc_tbl_);
  vector<TScanRangeParams> params;
  AddScanRange(-1, -1, &params);
  SetUpScanner(&scanner, params);
  bool eos = false;
  RowBatch* batch = obj_pool_->Add(
      new RowBatch(scanner.row_desc(), DEFAULT_ROWS_PER_BATCH, mem_tracker_.get()));
  ASSERT_OK(scanner.GetNext(runtime_state_.get(), batch, &eos));
  ASSERT_EQ(1, batch->num_rows());

  ASSERT_OK(scanner.GetNext(runtime_state_.get(), NULL, &eos));
  ASSERT_TRUE(eos);
  ASSERT_EQ(PrintBatch(batch), "[(10 null )]\n");
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

  vector<TScanRangeParams> params;
  AddScanRange(-1, -1, &params);

  // Try scanning but limit the number of returned rows to several different values.
  int limit_rows_to = 0;
  ScanAndVerify(params, FIRST_ROW, limit_rows_to, 3, 3, limit_rows_to);
  limit_rows_to = 1;
  ScanAndVerify(params, FIRST_ROW, limit_rows_to, 3, 3, limit_rows_to);
  limit_rows_to = 2000;
  ScanAndVerify(params, FIRST_ROW, 1000, 3, 3, limit_rows_to);
}

void KuduScanNodeTest::DoBenchmarkScan(const string& name,
    const vector<TScanRangeParams>& params, int num_cols) {

  const int NUM_ROWS = FLAGS_bench_num_rows;
  const int FIRST_ROW = 0;

  double avg = 0;
  int num_runs = 0;

  LOG(INFO) << "===== Starting benchmark: " << name;
  for (int i = 0; i < FLAGS_bench_num_runs; ++i) {
    MonotonicStopWatch watch;
    watch.Start();
    ScanAndVerify(params, FIRST_ROW, NUM_ROWS, num_cols, 3, NO_LIMIT, false);
    watch.Stop();
    int64_t total = watch.ElapsedTime();
    avg += total;
    ++num_runs;
    LOG(INFO) << "Run took: " << (total / (1000 * 1000)) << " msecs.";
  }

  avg = (avg / num_runs) / (1000 * 1000);
  LOG(INFO) << "===== Benchmark: " << name << " took(avg): " << avg << " msecs.";
}

TEST_F(KuduScanNodeTest, BenchmarkScanNode) {
  if (!FLAGS_run_scan_bench) return;

  // Generate some more splits and insert a lot more rows.
  const int NUM_ROWS = FLAGS_bench_num_rows;
  const int NUM_SPLITS = FLAGS_bench_num_splits;
  const int FIRST_ROW = 0;

  if (FLAGS_use_existing_table == "") {
    vector<const KuduPartialRow*> split_rows;
    for (int i = 1; i < NUM_SPLITS; ++i) {
       int split_key = (NUM_ROWS / NUM_SPLITS) * i;
       KuduPartialRow* row = kudu_test_helper_.test_schema().NewRow();
       row->SetInt32(0, split_key);
       split_rows.push_back(row);
    }
    kudu_test_helper_.CreateTable(BASE_TABLE_NAME, &split_rows);
    // Insert NUM_ROWS rows for this test.
    kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                     kudu_test_helper_.table().get(),
                                     NUM_ROWS);
    LOG(INFO) << "Inserted: " << NUM_ROWS << " rows into " << NUM_SPLITS << " tablets.";
  } else {
    kudu_test_helper_.OpenTable(FLAGS_use_existing_table);
  }

  // TODO We calculate the scan ranges based on the test params and not by actually
  // querying the tablet servers since Kudu doesn't have an API to get them.
  vector<TScanRangeParams> params;
  int previous_split_key = -1;
  for (int i = 1; i < NUM_SPLITS; ++i) {
     int split_key = (NUM_ROWS / NUM_SPLITS) * i;
     AddScanRange(previous_split_key, split_key, &params);
     previous_split_key = split_key;
     if (i == NUM_SPLITS -1) {
       AddScanRange(split_key, -1, &params);
     }
  }

  LOG(INFO) << "Warming up scans.";

  // Scan all columns once to warmup.
  ScanAndVerify(params, FIRST_ROW, NUM_ROWS, 3, 3, NO_LIMIT, false);

  DoBenchmarkScan("No cols", params, 0);
  DoBenchmarkScan("Key only", params, 1);
  DoBenchmarkScan("Int cols", params, 2);
  DoBenchmarkScan("All cols with strings", params, 3);
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
