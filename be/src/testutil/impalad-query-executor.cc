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
using namespace beeswax;

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
    const string& query_string, vector<PrimitiveType>* col_types) {
  Query query;
  query.query = query_string;

  // TODO: catch exception and return error code
  // LogContextId of "" will ask the Beeswax service to assign a new id but Beeswax
  // does not provide a constant for it.
  try {
    client_->executeAndWait(query_handle_, query, "");
  } catch (BeeswaxException& e) {
    stringstream ss;
    ss << e.SQLState << ": " << e.message;
    return Status(ss.str());
  }
  current_row_ = 0;
  return Status::OK;
}

Status ImpaladQueryExecutor::FetchResult(RowBatch** batch) {
  return Status::OK;
}

Status ImpaladQueryExecutor::FetchResult(string* row) {
  client_->fetch(query_results_, query_handle_, false, 1);

  // We've implemented fetch as sync. So, it always returns with result.
  DCHECK(query_results_.ready);
  if (query_results_.data.size() > 0) {
    *row = query_results_.data.at(0);
    ++exec_stats_.num_rows_;
  } else {
    *row = "";
  }

  eos_ = !query_results_.has_more;
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

// Return the explain plan for the query
Status ImpaladQueryExecutor::Explain(const string& query_string, string* explain_plan) {
  Query query;
  query.query = query_string;

  try {
    client_->explain(query_explanation_, query);
    *explain_plan = query_explanation_.textual;
  } catch (BeeswaxException& e) {
    stringstream ss;
    ss << e.SQLState << ": " << e.message;
    return Status(ss.str());
  }
  return Status::OK;
}

RuntimeProfile* ImpaladQueryExecutor::query_profile() {
  // TODO: make query profile part of TFetchResultsResult so that we can
  // return it here
  return NULL;
}

}
