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
#include <limits>
#include <memory>

#include <kudu/client/client.h>
#include <kudu/client/write_op.h>

#include "gutil/stl_util.h"
#include "util/kudu-status-util.h"

using kudu::Slice;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduSession;
using kudu::client::KuduTable;
using kudu::client::sp::shared_ptr;
using std::cerr;
using std::endl;

namespace impala {

// Utility program to facilitate complex column insertion into Kudu table.
// This script specifically tailored to insert into Kudu table with following columns:
// (
//   id TINYINT PRIMARY KEY,
//   array_INT ARRAY<INT>,
//   array_TIMESTAMP ARRAY<TIMESTAMP>,
//   array_VARCHAR ARRAY<VARCHAR(1)>,
//   array_DECIMAL ARRAY<DECIMAL(18,18)>,
//   array_DOUBLE ARRAY<DOUBLE>,
//   array_BINARY ARRAY<BINARY>,
//   array_BOOLEAN ARRAY<BOOLEAN>
// )
//
// The destination table must be empty before this program run.

// Same as in tests/conftest.py
constexpr const char* KUDU_MASTER_DEFAULT_ADDR = "localhost:7051";
const char* KUDU_TEST_TABLE_NAME;

const vector<int32_t> INT32_ARRAY = {
    std::numeric_limits<int32_t>::lowest(), -1, std::numeric_limits<int32_t>::max()};
const vector<int64_t> TIMESTAMP_ARRAY = {
    -17987443200000000, // See MIN_DATE_AS_UNIX_TIME in be/src/runtime/timestamp-test.cc
    -1L,
    253402300799999999, // See MAX_DATE_AS_UNIX_TIME in be/src/runtime/timestamp-test.cc
};
// To test multi-byte characters.
const vector<Slice> UTF8_ARRAY = {u8"Σ", u8"π", u8"λ"};
const vector<int64_t> DECIMAL18_ARRAY = {
    -999'999'999'999'999'999, // 18 digits
    -1L,
    999'999'999'999'999'999, // 18 digits
};
// See StringParser::StringToFloatInternal() for how the special values are generated.
const vector<double> DOUBLE_ARRAY = {
    -std::numeric_limits<double>::infinity(),
    -std::numeric_limits<double>::quiet_NaN(),
    std::numeric_limits<double>::infinity()
};
const vector<bool> BOOL_ARRAY = {true, false, true};

// 'id' starts from 0, same as Python's range().
int id = 0;

kudu::Status KuduInsertNulls(
    const shared_ptr<KuduSession>& session, const shared_ptr<KuduTable>& table) {
  std::unique_ptr<KuduInsert> insert(table->NewInsert());
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetInt8("id", id));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetNull("array_int"));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetNull("array_timestamp"));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetNull("array_varchar"));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetNull("array_decimal"));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetNull("array_double"));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetNull("array_binary"));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetNull("array_boolean"));
  KUDU_RETURN_NOT_OK(session->Apply(insert.release()));
  ++id;
  return kudu::Status::OK();
}

// Generates a vector whose length is the same as 'non_null' using the data in 'array'.
template <typename T>
vector<T> repeat(const vector<T>& array, const vector<bool>& non_null) {
  vector<T> result;
  result.reserve(non_null.size());
  for (size_t i = 0UL; i < non_null.size(); ++i) {
    result.push_back(array[i % array.size()]);
  }
  return result;
}

kudu::Status KuduInsertArrays(const shared_ptr<KuduSession>& session,
    const shared_ptr<KuduTable>& table, const vector<bool>& non_null) {
  std::unique_ptr<KuduInsert> insert(table->NewInsert());
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetInt8("id", id));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetArrayInt32(
      "array_int", repeat(INT32_ARRAY, non_null), non_null));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetArrayUnixTimeMicros(
      "array_timestamp", repeat(TIMESTAMP_ARRAY, non_null), non_null));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetArrayVarchar(
      "array_varchar", repeat(UTF8_ARRAY, non_null), non_null));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetArrayUnscaledDecimal(
      "array_decimal", repeat(DECIMAL18_ARRAY, non_null), non_null));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetArrayDouble(
      "array_double", repeat(DOUBLE_ARRAY, non_null), non_null));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetArrayBinary(
      "array_binary", repeat(UTF8_ARRAY, non_null), non_null));
  KUDU_RETURN_NOT_OK(insert->mutable_row()->SetArrayBool(
      "array_boolean", repeat(BOOL_ARRAY, non_null), non_null));
  KUDU_RETURN_NOT_OK(session->Apply(insert.release()));
  ++id;
  return kudu::Status::OK();
}

kudu::Status RunKuduArrayInsert() {
  shared_ptr<KuduClient> client;
  // Connect to the cluster.
  KUDU_RETURN_NOT_OK(KuduClientBuilder()
          .add_master_server_addr(KUDU_MASTER_DEFAULT_ADDR)
          .Build(&client));
  shared_ptr<KuduTable> table;
  KUDU_RETURN_NOT_OK(client->OpenTable(KUDU_TEST_TABLE_NAME, &table));

  shared_ptr<KuduSession> session = client->NewSession();
  KUDU_RETURN_NOT_OK(session->SetFlushMode(KuduSession::MANUAL_FLUSH));

  // The array slot is NULL.
  KUDU_RETURN_NOT_OK(KuduInsertNulls(session, table));

  // The array is not empty and no element is NULL.
  KUDU_RETURN_NOT_OK(KuduInsertArrays(session, table, {true, true, true}));

  // The array is empty.
  KUDU_RETURN_NOT_OK(KuduInsertArrays(session, table, {}));

  // Array element at the start is NULL.
  KUDU_RETURN_NOT_OK(KuduInsertArrays(session, table, {false, true, true}));

  // Array element at the middle is NULL.
  KUDU_RETURN_NOT_OK(KuduInsertArrays(session, table, {true, false, true}));

  // Array element at the end is NULL.
  KUDU_RETURN_NOT_OK(KuduInsertArrays(session, table, {true, true, false}));

  // The array is longer than those in the previous rows.
  KUDU_RETURN_NOT_OK(KuduInsertArrays(session, table, {true, true, true, true, true}));

  kudu::Status status = session->Flush();
  if (status.ok()) return status;
  vector<KuduError*> errors;
  ElementDeleter drop(&errors);
  bool overflowed;
  session->GetPendingErrors(&errors, &overflowed);
  for (const KuduError* error : errors) {
    cerr << "Error: " << error->status().ToString() << endl;
  }
  return status;
}
} // namespace impala

int main(int argc, char** argv) {
  // Example usage:
  //   kudu-array-inserter impala::functional_kudu.kudu_array
  assert(argc == 2);
  impala::KUDU_TEST_TABLE_NAME = argv[1];
  kudu::Status status = impala::RunKuduArrayInsert();
  if (!status.ok()) {
    cerr << "Error: " << status.ToString() << endl;
    return 1;
  }
  return 0;
}
