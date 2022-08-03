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

#include "exec/kudu/kudu-scan-node-mt.h"

#include <thrift/protocol/TDebugProtocol.h>
#include <vector>

#include "exec/kudu/kudu-scanner.h"
#include "exec/kudu/kudu-util.h"

#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

namespace impala {

KuduScanNodeMt::KuduScanNodeMt(ObjectPool* pool, const ScanPlanNode& pnode,
    const DescriptorTbl& descs)
    : KuduScanNodeBase(pool, pnode, descs),
      scan_token_(nullptr) {
  DCHECK(KuduIsAvailable());
}

KuduScanNodeMt::~KuduScanNodeMt() {
  DCHECK(is_closed());
}

Status KuduScanNodeMt::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(KuduScanNodeBase::Open(state));
  scanner_.reset(new KuduScanner(this, runtime_state_));
  RETURN_IF_ERROR(scanner_->Open());
  return Status::OK();
}

Status KuduScanNodeMt::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  DCHECK(row_batch != NULL);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  *eos = false;

  bool scan_token_eos = scan_token_ == nullptr;
  while (scan_token_eos) {
    scan_token_ = GetNextScanToken();
    if (scan_token_ == nullptr) {
      runtime_profile_->StopPeriodicCounters();
      scanner_->Close();
      scanner_.reset();
      *eos = true;
      return Status::OK();
    }
    RETURN_IF_ERROR(scanner_->OpenNextScanToken(*scan_token_, &scan_token_eos));
  }

  bool scanner_eos = false;
  RETURN_IF_ERROR(scanner_->GetNext(row_batch, &scanner_eos));
  if (scanner_eos) {
    scan_ranges_complete_counter_->Add(1);
    scan_token_ = nullptr;
  }
  scanner_->KeepKuduScannerAlive();

  if (CheckLimitAndTruncateRowBatchIfNeeded(row_batch, eos)) {
    scan_token_ = nullptr;
    runtime_profile_->StopPeriodicCounters();
    scanner_->Close();
    scanner_.reset();
  }
  COUNTER_SET(rows_returned_counter_, rows_returned());

  return Status::OK();
}

void KuduScanNodeMt::Close(RuntimeState* state) {
  if (is_closed()) return;
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  if (scanner_.get() != nullptr) scanner_->Close();
  scanner_.reset();
  KuduScanNodeBase::Close(state);
}

}  // namespace impala
