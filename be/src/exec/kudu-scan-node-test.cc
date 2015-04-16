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
#include <string>
#include <tr1/memory>
#include <vector>

#include "common/init.h"
#include "common/object-pool.h"
#include "codegen/llvm-codegen.h"
#include "exec/kudu-scan-node.h"
#include "exec/kudu-util.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Types_types.h"
#include "gutil/strings/split.h"
#include "kudu/client/client.h"
#include "kudu/client/encoded_key.h"
#include "kudu/client/schema.h"
#include "kudu/common/partial_row.h"
#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "service/fe-support.h"
#include "testutil/desc-tbl-builder.h"
#include "testutil/test-macros.h"
#include "util/cpu-info.h"
#include "util/test-info.h"

#include "common/names.h"

using apache::thrift::ThriftDebugString;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduEncodedKey;
using kudu::client::KuduEncodedKeyBuilder;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::KuduPartialRow;
using kudu::Slice;

typedef kudu::Status KuduStatus;
typedef impala::Status ImpalaStatus;

namespace impala {

static const char* BASE_TABLE_NAME = "TestScanNodeTable";

#define KUDU_ASSERT_OK(status) do { \
    KuduStatus _s = status; \
    if (_s.ok()) { \
      SUCCEED(); \
    } else { \
      FAIL() << "Bad Kudu Status: " << _s.ToString();  \
    } \
  } while (0);


// Helper class to assist in tests agains a Kudu cluster, namely with
// table creation/deletion with insertion of rows.
class KuduTestHelper {
 public:

  void CreateClient() {
    LOG(INFO) << "Creating Kudu client.";
    KUDU_ASSERT_OK(KuduClientBuilder()
                   .add_master_server_addr("127.0.0.1:7051")
                   .Build(&client_));
  }

  void CreateTable(const string& table_name_prefix, const KuduSchema& schema) {
    // Kudu's table delete functionality is in flux, meaning a table may reappear
    // after being deleted. To work around this we add the time in milliseconds to
    // the required table name, making it unique. When Kudu's delete table functionality
    // is solid we should change this to avoid creating, and possibly leaving, many
    // similar tables in the local Kudu test cluster. See KUDU-676
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int64_t millis = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    table_name_ = strings::Substitute("$0-$1", table_name_prefix, millis);
    schema_ = schema;

    while(true) {
      LOG(INFO) << "Creating Kudu table: " << table_name_;
      kudu::Status s = client_->NewTableCreator()->table_name(table_name_)
                             .schema(&schema_)
                             .num_replicas(3)
                             .split_keys(GenerateSplitKeys())
                             .Create();
      if (s.IsAlreadyPresent()) {
        LOG(INFO) << "Table existed, deleting. " << table_name_;
        KUDU_ASSERT_OK(client_->DeleteTable(table_name_));
        sleep(1);
        continue;
      }
      KUDU_CHECK_OK(s);
      KUDU_ASSERT_OK(client_->OpenTable(table_name_, &client_table_));
      break;
    }
  }

  gscoped_ptr<KuduInsert> BuildTestRow(KuduTable* table, int index) {
    gscoped_ptr<KuduInsert> insert = table->NewInsert();
    KuduPartialRow* row = insert->mutable_row();
    KUDU_CHECK_OK(row->SetInt32(0, index));
    KUDU_CHECK_OK(row->SetInt32(1, index * 2));
    KUDU_CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello_%d", index))));
    return insert.Pass();
  }

  void InsertTestRows(KuduClient* client, KuduTable* table, int num_rows,
      int first_row = 0) {
    std::tr1::shared_ptr<KuduSession> session = client->NewSession();
    KUDU_ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(10000);
    for (int i = first_row; i < num_rows + first_row; i++) {
      KUDU_ASSERT_OK(session->Apply(BuildTestRow(table, i).Pass()));
    }
    KUDU_ASSERT_OK(session->Flush());
    ASSERT_FALSE(session->HasPendingOperations());
  }


  void DeleteTable() {
    LOG(INFO) << "Deleting Kudu table: " << table_name_;
    KUDU_ASSERT_OK(client_->DeleteTable(table_name_));
  }

  vector<string> GenerateSplitKeys() {
    vector<string> keys;
    KuduEncodedKeyBuilder builder(schema_);
    int val = 5;
    builder.AddColumnKey(&val);
    gscoped_ptr<KuduEncodedKey> key(builder.BuildEncodedKey());
    keys.push_back(key->ToString());
    return keys;
  }

  const string& table_name() const {
    return table_name_;
  }

  const std::tr1::shared_ptr<KuduClient>& client() const {
    return client_;
  }

  const scoped_refptr<KuduTable>& table() const {
    return client_table_;
  }

 private:
  string table_name_;
  KuduSchema schema_;;
  std::tr1::shared_ptr<KuduClient> client_;
  scoped_refptr<KuduTable> client_table_;
};

class KuduScanNodeTest : public testing::Test {
 public:
  KuduScanNodeTest()
      : runtime_state_(TPlanFragmentInstanceCtx(), "", &exec_env_) {}

  virtual void SetUp() {
    // Create a Kudu client and the table (this will abort the test here
    // if a Kudu cluster is not available).
    kudu_test_helper_.CreateClient();

    vector<KuduColumnSchema> column_schemas;
    column_schemas.push_back(KuduColumnSchema("key", KuduColumnSchema::INT32));
    column_schemas.push_back(KuduColumnSchema("int_val", KuduColumnSchema::INT32, true));
    column_schemas.push_back(KuduColumnSchema("string_val", KuduColumnSchema::STRING,
                                              true));
    schema_ = KuduSchema(column_schemas, 1);

    kudu_test_helper_.CreateTable(BASE_TABLE_NAME, schema_);

    // Initialize the environment/runtime so that we can use a scan node in
    // isolation.
    DCHECK_OK(exec_env_.InitForFeTests());
    runtime_state_.InitMemTrackers(TUniqueId(), NULL, -1);

    TKuduScanNode kudu_scan_node_;
    kudu_node_.__set_kudu_scan_node(kudu_scan_node_);
    kudu_node_.__set_node_type(TPlanNodeType::KUDU_SCAN_NODE);
    kudu_node_.__set_limit(-1);

    DescriptorTblBuilder desc_builder(&obj_pool_);
    desc_builder.DeclareTuple() << TYPE_INT << TYPE_INT << TYPE_STRING;

    TKuduTable t_kudu_table;
    t_kudu_table.__set_table_name(kudu_test_helper_.table_name());
    t_kudu_table.__set_master_addresses(vector<string>(1, "0.0.0.0:7051"));
    t_kudu_table.__set_key_columns(boost::assign::list_of("key"));

    TTableDescriptor t_tbl_desc;
    t_tbl_desc.__set_id(0);
    t_tbl_desc.__set_tableType(::impala::TTableType::KUDU_TABLE);
    t_tbl_desc.__set_kuduTable(t_kudu_table);
    t_tbl_desc.__set_colNames(boost::assign::list_of("key")("int_val")("string_val"));

    desc_builder.SetTableDescriptor(t_tbl_desc);

    desc_tbl_ = desc_builder.Build();

    row_desc_ = obj_pool_.Add(
        new RowDescriptor(*desc_tbl_,
                          boost::assign::list_of(0),
                          boost::assign::list_of(false)));

    runtime_state_.set_desc_tbl(desc_tbl_);
    exec_env_.disk_io_mgr()->Init(&mem_tracker_);
  }

  virtual void TearDown() {
    kudu_test_helper_.DeleteTable();
  }

  void VerifyBatch(RowBatch* batch, int first_row, int last_row) {
    string batch_as_string = PrintBatch(batch);
    vector<string> rows = strings::Split(batch_as_string, "\n", strings::SkipEmpty());
    ASSERT_EQ(rows.size(), last_row - first_row);
    for (int i = 0; i < rows.size(); ++i) {
      int idx = first_row + i;
      ASSERT_EQ(rows[i], strings::Substitute("[($0 $1 hello_$2)]", idx, idx * 2, idx));
    }
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
  KuduSchema schema_;
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

  // Insert 1000 rows for this test.
  kudu_test_helper_.InsertTestRows(kudu_test_helper_.client().get(),
                                   kudu_test_helper_.table().get(),
                                   kNumRows);

  KuduScanNode scanner(&obj_pool_, kudu_node_, *desc_tbl_);

  // TODO test more than one range. This was tested but left out of the current
  // version as Kudu is currently changing the way it addresses start/stop keys (stop
  // key is now inclusive and will become exclusive).
  vector<TScanRangeParams> params;
  AddScanRange("", "", &params);

  scanner.SetScanRanges(params);

  ASSERT_OK(scanner.Prepare(&runtime_state_));
  ASSERT_OK(scanner.Open(&runtime_state_));

  int num_rows = 0;
  int num_batches = 0;
  int batch_size = kNumRows / kNumBatches;

  bool eos = false;

  do {
    RowBatch* batch = obj_pool_.Add(new RowBatch(*row_desc_, batch_size, &mem_tracker_));
    ASSERT_OK(scanner.GetNext(&runtime_state_, batch, &eos));

    if (batch->num_rows() == 0) {
      ASSERT_TRUE(eos);
      break;
    }

    ASSERT_EQ(batch->num_rows(), batch_size);
    VerifyBatch(batch, batch_size * num_batches, batch_size * (num_batches + 1));
    num_rows += batch->num_rows();
    ++num_batches;
  } while(!eos);

  ASSERT_EQ(kNumRows, num_rows);
  ASSERT_EQ(kNumBatches, num_batches);
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
