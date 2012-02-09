// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "testutil/impalad-query-executor.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <protocol/TDebugProtocol.h>

#include <protocol/TBinaryProtocol.h>
#include <protocol/TDebugProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

DEFINE_string(impalad, "", "host:port of impalad process");
DECLARE_int32(num_nodes);

using namespace std;
using namespace boost;
using namespace boost::algorithm;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

namespace impala {

ImpaladQueryExecutor::ImpaladQueryExecutor(): current_row_(0), eos_(false) {
}

ImpaladQueryExecutor::~ImpaladQueryExecutor() {
}

Status ImpaladQueryExecutor::Setup() {
  DCHECK(!FLAGS_impalad.empty());
  vector<string> elems;
  split(elems, FLAGS_impalad, is_any_of(":"));
  DCHECK_EQ(elems.size(), 2);
  int port = atoi(elems[1].c_str());
  DCHECK_GT(port, 0);
  socket_.reset(new TSocket(elems[0], port));
  transport_.reset(new TBufferedTransport(socket_));
  protocol_.reset(new TBinaryProtocol(transport_));
  client_.reset(new ImpalaServiceClient(protocol_));

  try {
    transport_->open();
  } catch (TTransportException& e) {
    return Status(e.what());
  }

  return Status::OK;
}

Status ImpaladQueryExecutor::Exec(
    const string& query, vector<PrimitiveType>* col_types) {
  TQueryRequest request;
  request.stmt = query;
  request.returnAsAscii = true;
  request.numNodes = FLAGS_num_nodes;
  TRunQueryResult run_query_result;
  client_->RunQuery(run_query_result, request);
  if (run_query_result.status.status_code != TStatusCode::OK) {
    return Status(run_query_result.status);
  }
  query_id_ = run_query_result.query_id;

  // prime query_result_
  TFetchResultsResult fetch_result;
  client_->FetchResults(fetch_result, query_id_);
  if (fetch_result.status.status_code != TStatusCode::OK) {
    return Status(fetch_result.status);
  }
  query_result_ = fetch_result.query_result;
  current_row_ = 0;
  return Status::OK;
}

Status ImpaladQueryExecutor::FetchResult(RowBatch** batch) {
  return Status::OK;
}

Status ImpaladQueryExecutor::FetchResult(string* row) {
  if (current_row_ == query_result_.rows.size()) {
    if (query_result_.eos) {
      *row = "";
      eos_ = true;
      return Status::OK;
    }

    TFetchResultsResult fetch_result;
    client_->FetchResults(fetch_result, query_id_);
    VLOG(2) << ThriftDebugString(fetch_result);
    if (fetch_result.status.status_code != TStatusCode::OK) {
      return Status(fetch_result.status);
    }
    query_result_ = fetch_result.query_result;
    // if we get back an empty batch, we must have hit eos
    DCHECK(!query_result_.rows.empty() || query_result_.eos);
    if (query_result_.rows.empty()) {
      *row = "";
      eos_ = true;
      return Status::OK;
    }
    current_row_ = 0;
  }

  row->clear();
  DCHECK_LT(current_row_, query_result_.rows.size());
  vector<TColumnValue>& col_vals = query_result_.rows[current_row_].colVals;
  for (vector<TColumnValue>::iterator val = col_vals.begin(); val != col_vals.end();
       ++val) {
    if (val != col_vals.begin()) row->append(", ");
    row->append(val->stringVal);
  }
  ++current_row_;
  return Status::OK;
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

RuntimeProfile* ImpaladQueryExecutor::query_profile() {
  // TODO: make query profile part of TFetchResultsResult so that we can
  // return it here
  return NULL;
}

}
