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

#ifndef IMPALA_EXEC_KUDU_TESTUTIL_H
#define IMPALA_EXEC_KUDU_TESTUTIL_H

#include <gtest/gtest.h>
#include <kudu/client/client.h>
#include <kudu/util/slice.h>
#include <kudu/util/status.h>
#include <string>
#include <tr1/memory>
#include <vector>

#include "gutil/gscoped_ptr.h"
#include "runtime/exec-env.h"
#include "testutil/desc-tbl-builder.h"

#include "common/names.h"

typedef kudu::Status KuduStatus;
typedef impala::Status ImpalaStatus;

namespace impala {

using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduInsert;
using kudu::client::KuduSchema;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::KuduPartialRow;
using kudu::Slice;

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

    vector<KuduColumnSchema> column_schemas;
    column_schemas.push_back(KuduColumnSchema("key", KuduColumnSchema::INT32));
    column_schemas.push_back(KuduColumnSchema("int_val", KuduColumnSchema::INT32, true));
    column_schemas.push_back(KuduColumnSchema("string_val", KuduColumnSchema::STRING,
                                              true));
    test_schema_ = KuduSchema(column_schemas, 1);
  }

  void CreateTable(const string& table_name_prefix) {
    // Kudu's table delete functionality is in flux, meaning a table may reappear
    // after being deleted. To work around this we add the time in milliseconds to
    // the required table name, making it unique. When Kudu's delete table functionality
    // is solid we should change this to avoid creating, and possibly leaving, many
    // similar tables in the local Kudu test cluster. See KUDU-676
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int64_t millis = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    table_name_ = strings::Substitute("$0-$1", table_name_prefix, millis);

    while(true) {
      LOG(INFO) << "Creating Kudu table: " << table_name_;
      kudu::Status s = client_->NewTableCreator()->table_name(table_name_)
                             .schema(&test_schema_)
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

  gscoped_ptr<KuduInsert> BuildTestRow(KuduTable* table, int index, int num_cols) {
    DCHECK_GT(num_cols, 0);
    DCHECK_LE(num_cols, 3);
    gscoped_ptr<KuduInsert> insert(table->NewInsert());
    KuduPartialRow* row = insert->mutable_row();
    KUDU_CHECK_OK(row->SetInt32(0, index));
    if (num_cols > 1) KUDU_CHECK_OK(row->SetInt32(1, index * 2));
    if (num_cols > 2) {
      KUDU_CHECK_OK(row->SetStringCopy(2, Slice(StringPrintf("hello_%d", index))));
    }
    return insert.Pass();
  }

  void InsertTestRows(KuduClient* client, KuduTable* table, int num_rows,
      int first_row = 0, int num_cols = 3) {
    std::tr1::shared_ptr<KuduSession> session = client->NewSession();
    KUDU_ASSERT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(10000);
    for (int i = first_row; i < num_rows + first_row; i++) {
      KUDU_ASSERT_OK(session->Apply(BuildTestRow(table, i, num_cols).release()));
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
    KuduPartialRow* key = test_schema_.NewRow();
    key->SetInt32(0, 5);
    keys.push_back(key->ToEncodedRowKeyOrDie());
    return keys;
  }

  const string& table_name() const {
    return table_name_;
  }

  const std::tr1::shared_ptr<KuduClient>& client() const {
    return client_;
  }

  const std::tr1::shared_ptr<KuduTable>& table() const {
    return client_table_;
  }

  const KuduSchema& test_schema() {
    return test_schema_;
  }

  // Creates a test descriptor table based on the test schema.
  // The returned DescriptorTbl will be allocated in this classe's object pool.
  void CreateTableDescriptor(int num_cols_materialize, DescriptorTbl** desc_tbl) {
    DescriptorTblBuilder desc_builder(&obj_pool_);
    DCHECK_GE(num_cols_materialize, 0);
    DCHECK_LE(num_cols_materialize, test_schema_.num_columns());

    desc_builder.DeclareTuple()
        .AddSlot(TYPE_INT, num_cols_materialize > 0)
        .AddSlot(TYPE_INT, num_cols_materialize > 1)
        .AddSlot(TYPE_STRING, num_cols_materialize > 2);

    TKuduTable t_kudu_table;
    t_kudu_table.__set_table_name(table_name());
    t_kudu_table.__set_master_addresses(vector<string>(1, "0.0.0.0:7051"));
    t_kudu_table.__set_key_columns(boost::assign::list_of("key"));

    TTableDescriptor t_tbl_desc;
    t_tbl_desc.__set_id(0);
    t_tbl_desc.__set_tableType(::impala::TTableType::KUDU_TABLE);
    t_tbl_desc.__set_kuduTable(t_kudu_table);
    t_tbl_desc.__set_colNames(boost::assign::list_of("key")("int_val")("string_val"));

    desc_builder.SetTableDescriptor(t_tbl_desc);

    *desc_tbl = desc_builder.Build();
  }

 private:
  string table_name_;
  KuduSchema test_schema_;;
  ObjectPool obj_pool_;
  std::tr1::shared_ptr<KuduClient> client_;
  std::tr1::shared_ptr<KuduTable> client_table_;
};

} // namespace impala

#endif /* IMPALA_EXEC_KUDU_TESTUTIL_H */
