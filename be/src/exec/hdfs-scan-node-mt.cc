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

#include "exec/hdfs-scan-node-mt.h"

#include <sstream>

#include "exec/exec-node-util.h"
#include "exec/scanner-context.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/exec-env.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

#include "common/names.h"

using namespace impala::io;

namespace impala {

HdfsScanNodeMt::HdfsScanNodeMt(
    ObjectPool* pool, const HdfsScanPlanNode& pnode, const DescriptorTbl& descs)
  : HdfsScanNodeBase(pool, pnode, pnode.tnode_->hdfs_scan_node, descs),
    scan_range_(NULL),
    scanner_(NULL) {}

HdfsScanNodeMt::~HdfsScanNodeMt() {
}

Status HdfsScanNodeMt::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(HdfsScanNodeBase::Prepare(state));
  return Status::OK();
}

Status HdfsScanNodeMt::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedOpenEventAdder ea(this);
  RETURN_IF_ERROR(HdfsScanNodeBase::Open(state));
  DCHECK(!initial_ranges_issued_.Load());
  shared_state_->AddCancellationHook(state);
  RETURN_IF_ERROR(IssueInitialScanRanges(state));
  return Status::OK();
}

Status HdfsScanNodeMt::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  ScopedGetNextEventAdder ea(this, eos);
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));
  *eos = false;

  DCHECK(scan_range_ == NULL || scanner_ != NULL);
  if (scan_range_ == NULL || scanner_->eos()) {
    if (scanner_ != NULL && scanner_->eos()) {
      scanner_->Close(row_batch);
      scanner_.reset();
    }
    int64_t scanner_reservation = buffer_pool_client()->GetReservation();
    RETURN_IF_ERROR(StartNextScanRange(filter_ctxs_, &scanner_reservation, &scan_range_));
    if (scan_range_ == nullptr) {
      *eos = true;
      StopAndFinalizeCounters();
      return Status::OK();
    }
    ScanRangeMetadata* metadata =
        static_cast<ScanRangeMetadata*>(scan_range_->meta_data());
    HdfsPartitionDescriptor* partition =
        hdfs_table_->GetPartition(metadata->partition_id);
    DCHECK(partition != nullptr);
    scanner_ctx_.reset(new ScannerContext(runtime_state_, this, buffer_pool_client(),
        scanner_reservation, partition, filter_ctxs(), expr_results_pool()));
    scanner_ctx_->AddStream(scan_range_, scanner_reservation);
    Status status = CreateAndOpenScanner(partition, scanner_ctx_.get(), &scanner_);
    if (!status.ok()) {
      DCHECK(scanner_ == NULL);
      // Avoid leaking unread buffers in the scan range.
      scan_range_->Cancel(status);
      return status;
    }
  }

  // We only need one row per partition. Limit the capacity to prevent the scanner
  // materialising extra rows.
  if (is_partition_key_scan_) row_batch->limit_capacity(1);
  Status status = scanner_->GetNext(row_batch);
  if (!status.ok()) {
    scanner_->Close(row_batch);
    scanner_.reset();
    return status;
  }
  InitNullCollectionValues(row_batch);

  if (CheckLimitAndTruncateRowBatchIfNeeded(row_batch, eos)) {
    scan_range_ = NULL;
    scanner_->Close(row_batch);
    scanner_.reset();
    *eos = true;
  } else if (row_batch->num_rows() > 0 && is_partition_key_scan_) {
    // Only return one from each scan range.
    scanner_->Close(row_batch);
    scanner_.reset();
    scan_range_ = nullptr;
    return Status::OK();
  }
  COUNTER_SET(rows_returned_counter_, rows_returned());

  if (*eos) StopAndFinalizeCounters();
  return Status::OK();
}

Status HdfsScanNodeMt::CreateAndOpenScanner(HdfsPartitionDescriptor* partition,
    ScannerContext* context, scoped_ptr<HdfsScanner>* scanner) {
  Status status = CreateAndOpenScannerHelper(partition, context, scanner);
  if (!status.ok() && scanner->get() != nullptr) {
    scanner->get()->Close(nullptr);
    scanner->reset();
  }
  return status;
}

void HdfsScanNodeMt::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (scanner_.get() != nullptr) scanner_->Close(nullptr);
  scanner_.reset();
  scanner_ctx_.reset();
  HdfsScanNodeBase::Close(state);
}

Status HdfsScanNodeMt::AddDiskIoRanges(
    const vector<ScanRange*>& ranges, EnqueueLocation enqueue_location) {
  DCHECK(!shared_state_->progress().done())
      << "Don't call AddScanRanges() after all ranges finished.";
  DCHECK_GT(shared_state_->RemainingScanRangeSubmissions(), 0);
  DCHECK_GT(ranges.size(), 0);
  bool at_front = false;
  if (enqueue_location == EnqueueLocation::HEAD) {
    at_front = true;
  }
  shared_state_->EnqueueScanRange(ranges, at_front);
  return Status::OK();
}

Status HdfsScanNodeMt::GetNextScanRangeToRead(
    io::ScanRange** scan_range, bool* needs_buffers) {
  RETURN_IF_ERROR(shared_state_->GetNextScanRange(runtime_state_, scan_range));
  if (*scan_range != nullptr) {
    RETURN_IF_ERROR(reader_context_->StartScanRange(*scan_range, needs_buffers));
  }
  return Status::OK();
}
}
