// Copyright 2012 Cloudera Inc.
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

#include "testutil/impalad-query-executor.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string.hpp>

#include "common/logging.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-util.h"

DEFINE_string(impalad, "localhost:21000", "host:port of impalad process");
DECLARE_int32(num_nodes);

#include "common/names.h"

using boost::algorithm::is_any_of;
using boost::algorithm::split;
using namespace Apache::Hadoop::Hive;
using namespace beeswax;

namespace impala {

ImpaladQueryExecutor::ImpaladQueryExecutor()
  : query_in_progress_(false),
    current_row_(0),
    eos_(false) {
}

ImpaladQueryExecutor::~ImpaladQueryExecutor() {
  Close();
}

Status ImpaladQueryExecutor::Setup() {
  DCHECK(!FLAGS_impalad.empty());
  vector<string> elems;
  split(elems, FLAGS_impalad, is_any_of(":"));
  DCHECK_EQ(elems.size(), 2);
  int port = atoi(elems[1].c_str());
  DCHECK_GT(port, 0);

  client_.reset(new ThriftClient<ImpalaServiceClient>(elems[0], port));

  // Wait for up to 10s for the server to start, polling at 50ms intervals
  RETURN_IF_ERROR(WaitForServer(elems[0], port, 200, 50));

  RETURN_IF_ERROR(client_->Open());

  return Status::OK();
}

Status ImpaladQueryExecutor::Close() {
  if (!query_in_progress_) return Status::OK();
  try {
    client_->iface()->close(query_handle_);
  } catch (BeeswaxException& e) {
    stringstream ss;
    ss << e.SQLState << ": " << e.message;
    return Status(ss.str());
  }
  query_in_progress_ = false;
  return Status::OK();
}

Status ImpaladQueryExecutor::Exec(
    const string& query_string, vector<FieldSchema>* col_schema) {
  // close anything that ran previously
  Close();
  Query query;
  query.query = query_string;
  query.configuration = exec_options_;
  query.hadoop_user = "impala_test_user";
  query_results_.data.clear();

  // TODO: catch exception and return error code
  // LogContextId of "" will ask the Beeswax service to assign a new id but Beeswax
  // does not provide a constant for it.
  ResultsMetadata resultsMetadata;
  try {
    client_->iface()->executeAndWait(query_handle_, query, "");
    client_->iface()->get_results_metadata(resultsMetadata, query_handle_);
  } catch (BeeswaxException& e) {
    stringstream ss;
    ss << e.SQLState << ": " << e.message;
    return Status(ss.str());
  }
  current_row_ = 0;
  query_in_progress_ = true;
  if (col_schema != NULL) *col_schema = resultsMetadata.schema.fieldSchemas;
  return Status::OK();
}

Status ImpaladQueryExecutor::FetchResult(RowBatch** batch) {
  return Status::OK();
}

Status ImpaladQueryExecutor::FetchResult(string* row) {
  // If we have not fetched any data, or we've returned all the data, fetch more rows
  // from ImpalaServer
  if (!query_results_.__isset.data || current_row_ >= query_results_.data.size()) {
    try {
      client_->iface()->fetch(query_results_, query_handle_, false, 0);
    } catch (BeeswaxException& e) {
      stringstream ss;
      ss << e.SQLState << ": " << e.message;
      return Status(ss.str());
    }
    current_row_ = 0;
  }

  DCHECK(query_results_.ready);

  // Set the return row if we have data
  if (query_results_.data.size() > 0) {
    *row = query_results_.data.at(current_row_);
    ++current_row_;
  } else {
    *row = "";
  }

  // Set eos_ to true after the we have returned the last row from the last batch.
  if (current_row_  >= query_results_.data.size() && !query_results_.has_more) {
    eos_ = true;
  }

  return Status::OK();
}

Status ImpaladQueryExecutor::FetchResult(vector<void*>* row) {
  return Status("ImpaladQueryExecutor::FetchResult(vector<void*>) not supported");
}

string ImpaladQueryExecutor::ErrorString() const {
  return "";
}

string ImpaladQueryExecutor::FileErrors() const {
  return "";
}

// Return the explain plan for the query
Status ImpaladQueryExecutor::Explain(const string& query_string, string* explain_plan) {
  Query query;
  query.query = query_string;

  try {
    client_->iface()->explain(query_explanation_, query);
    *explain_plan = query_explanation_.textual;
  } catch (BeeswaxException& e) {
    stringstream ss;
    ss << e.SQLState << ": " << e.message;
    return Status(ss.str());
  }
  return Status::OK();
}

RuntimeProfile* ImpaladQueryExecutor::query_profile() {
  // TODO: make query profile part of TFetchResultsResult so that we can
  // return it here
  return NULL;
}

}
