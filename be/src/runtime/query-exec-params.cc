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

#include "runtime/query-exec-params.h"

#include "common/logging.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/admission_control_service.pb.h"
#include "util/uid-util.h"

namespace impala {

QueryExecParams::QueryExecParams(
    const TExecRequest& exec_request, const QuerySchedulePB& query_schedule)
  : exec_request_(exec_request), query_schedule_(query_schedule) {
  TUniqueIdToUniqueIdPB(exec_request_.query_exec_request.query_ctx.query_id, &query_id_);

  int num_fragments = 0;
  for (const TPlanExecInfo& plan_exec_info : query_exec_request().plan_exec_info) {
    num_fragments += plan_exec_info.fragments.size();
  }

  fragments_.resize(num_fragments);
  // Populate 'fragments_' with references into 'exec_request_'.
  for (const TPlanExecInfo& plan_exec_info : query_exec_request().plan_exec_info) {
    for (const TPlanFragment& fragment : plan_exec_info.fragments) {
      fragments_[fragment.idx] = &fragment;
    }
  }
  DCHECK(!fragments_.empty());
}

QueryExecParams::QueryExecParams(const UniqueIdPB& query_id,
    const TExecRequest& exec_request, const QuerySchedulePB& query_schedule)
  : query_id_(query_id), exec_request_(exec_request), query_schedule_(query_schedule) {}

bool QueryExecParams::HasResultSink(const TPlanFragment* fragment) const {
  return fragment != nullptr &&
      fragment->output_sink.type == TDataSinkType::TABLE_SINK &&
      fragment->output_sink.table_sink.type == TTableSinkType::HDFS &&
      fragment->output_sink.table_sink.hdfs_table_sink.is_result_sink;
}

bool QueryExecParams::HasResultSink() const {
  return HasResultSink(GetCoordFragmentImpl());
}

const TPlanFragment* QueryExecParams::GetCoordFragmentImpl() const {
  // Only have coordinator fragment for statements that return rows.
  if (query_exec_request().stmt_type != TStmtType::QUERY) return nullptr;
  DCHECK_GE(query_exec_request().plan_exec_info.size(), 1);
  DCHECK_GE(query_exec_request().plan_exec_info[0].fragments.size(), 1);
  const TPlanFragment* fragment = &query_exec_request().plan_exec_info[0].fragments[0];
  DCHECK_EQ(fragment->partition.type, TPartitionType::UNPARTITIONED);
  return fragment;
}

const TPlanFragment* QueryExecParams::GetCoordFragment() const {
  const TPlanFragment* fragment = GetCoordFragmentImpl();
  if (HasResultSink(fragment)) return nullptr;
  return fragment;
}

int QueryExecParams::GetNumFragmentInstances() const {
  int total = 0;
  for (const BackendExecParamsPB& backend_exec_params :
      query_schedule_.backend_exec_params()) {
    total += backend_exec_params.instance_params().size();
  }
  return total;
}

} // namespace impala
