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

#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "gen-cpp/PlanNodes_types.h"

using std::stringstream;

namespace impala {

HdfsScanNodeMt::HdfsScanNodeMt(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : HdfsScanNodeBase(pool, tnode, descs),
      scan_range_(NULL),
      scanner_(NULL) {
}

HdfsScanNodeMt::~HdfsScanNodeMt() {
}

Status HdfsScanNodeMt::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(HdfsScanNodeBase::Prepare(state));
  // Return an error if this scan node has been assigned a range that is not supported
  // because the scanner of the corresponding file format does implement GetNext().
  for (const auto& files: per_type_files_) {
    if (!files.second.empty() && files.first != THdfsFileFormat::PARQUET
        && files.first != THdfsFileFormat::TEXT) {
      stringstream msg;
      msg << "Unsupported file format with HdfsScanNodeMt: " << files.first;
      return Status(msg.str());
    }
  }
  return Status::OK();
}

Status HdfsScanNodeMt::Open(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(HdfsScanNodeBase::Open(state));
  DCHECK(!initial_ranges_issued_);
  RETURN_IF_ERROR(IssueInitialScanRanges(state));
  return Status::OK();
}

Status HdfsScanNodeMt::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
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
    RETURN_IF_ERROR(
        runtime_state_->io_mgr()->GetNextRange(reader_context_, &scan_range_));
    if (scan_range_ == NULL) {
      *eos = true;
      StopAndFinalizeCounters();
      return Status::OK();
    }
    ScanRangeMetadata* metadata =
        static_cast<ScanRangeMetadata*>(scan_range_->meta_data());
    int64_t partition_id = metadata->partition_id;
    HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
    scanner_ctx_.reset(new ScannerContext(
        runtime_state_, this, partition, scan_range_, filter_ctxs(),
        expr_results_pool()));
    Status status = CreateAndOpenScanner(partition, scanner_ctx_.get(), &scanner_);
    if (!status.ok()) {
      DCHECK(scanner_ == NULL);
      // Avoid leaking unread buffers in the scan range.
      scan_range_->Cancel(status);
      return status;
    }
  }

  Status status = scanner_->GetNext(row_batch);
  if (!status.ok()) {
    scanner_->Close(row_batch);
    scanner_.reset();
    num_owned_io_buffers_.Add(-row_batch->num_io_buffers());
    return status;
  }
  InitNullCollectionValues(row_batch);

  num_rows_returned_ += row_batch->num_rows();
  if (ReachedLimit()) {
    int num_rows_over = num_rows_returned_ - limit_;
    row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
    num_rows_returned_ -= num_rows_over;
    scan_range_ = NULL;
    scanner_->Close(row_batch);
    scanner_.reset();
    *eos = true;
  }
  COUNTER_SET(rows_returned_counter_, num_rows_returned_);
  num_owned_io_buffers_.Add(-row_batch->num_io_buffers());

  if (*eos) StopAndFinalizeCounters();
  return Status::OK();
}

void HdfsScanNodeMt::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (scanner_.get() != nullptr) scanner_->Close(nullptr);
  scanner_.reset();
  scanner_ctx_.reset();
  HdfsScanNodeBase::Close(state);
}

}
