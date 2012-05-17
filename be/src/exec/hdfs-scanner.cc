// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/hdfs-scanner.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include "exec/text-converter.h"
#include "exec/hdfs-byte-stream.h"
#include "exec/hdfs-scan-node.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
#include "util/sse-util.h"
#include "util/string-parser.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace std;
using namespace boost;
using namespace impala;

HdfsScanner::HdfsScanner(HdfsScanNode* scan_node, const TupleDescriptor* tuple_desc,
                         Tuple* template_tuple, MemPool* tuple_pool)
    : scan_node_(scan_node),
      num_rows_returned_(0),
      tuple_buffer_(NULL),
      tuple_byte_size_(tuple_desc->byte_size()),
      tuple_idx_(0),
      tuple_pool_(tuple_pool),
      tuple_(NULL),
      num_errors_in_file_(0),
      current_byte_stream_(NULL),
      template_tuple_(template_tuple),
      has_noncompact_strings_(!scan_node->compact_data() &&
                              !tuple_desc->string_slots().empty()),
      current_scan_range_(NULL) {
}

HdfsScanner::~HdfsScanner() {
}

Status HdfsScanner::Prepare(RuntimeState* state, ByteStream* byte_stream) {
  DCHECK(byte_stream != NULL);
  current_byte_stream_ = byte_stream;

  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;
  return Status::OK;
}

void HdfsScanner::AllocateTupleBuffer(RowBatch* row_batch) {
  if (tuple_byte_size_ == 0) {
    tuple_buffer_size_ = 0;
  } else if (tuple_ == NULL) {
    // create new tuple buffer for row_batch
    tuple_buffer_size_ = row_batch->capacity() * tuple_byte_size_;
    tuple_buffer_ = tuple_pool_->Allocate(tuple_buffer_size_);
    bzero(tuple_buffer_, tuple_buffer_size_);
    tuple_ = reinterpret_cast<Tuple*>(tuple_buffer_);
  }
}

Status HdfsScanner::InitCurrentScanRange(RuntimeState* state, HdfsScanRange* range,
                                         ByteStream* byte_stream) {
  current_scan_range_ = range;
  return Status::OK;
}

// In this code path, no slots were materialized from the input files.  The only
// slots are from partition keys.  This lets us simplify writing out the batches.
//   1. template_tuple_ is the complete tuple.
//   2. Eval conjuncts against the tuple.
//   3. If it passes, stamp out 'num_tuples' copies of it into the row_batch.
Status HdfsScanner::WriteTuples(RuntimeState* state, RowBatch* row_batch, int num_tuples,
    int* row_idx) {
  COUNTER_SCOPED_TIMER(scan_node_->tuple_write_timer());

  DCHECK_GT(num_tuples, 0);

  // Make a row and evaluate the row
  if (*row_idx == RowBatch::INVALID_ROW_INDEX) {
    *row_idx = row_batch->AddRow();
  }
  TupleRow* current_row = row_batch->GetRow(*row_idx);
  current_row->SetTuple(tuple_idx_, template_tuple_);
  if (!scan_node_->EvalConjunctsForScanner(current_row)) {
    return Status::OK;
  }
  // Add first tuple
  row_batch->CommitLastRow();
  *row_idx = RowBatch::INVALID_ROW_INDEX;
  scan_node_->IncrNumRowsReturned();
  ++num_rows_returned_;
  --num_tuples;

  // All the tuples in this batch are identical and can be stamped out
  // from template_tuple_.  Write out tuples up to the limit_.
  if (scan_node_->limit() != -1) {
    num_tuples = min(num_tuples,
                     static_cast<int>(scan_node_->limit() - num_rows_returned_));
  }
  DCHECK_LE(num_tuples, row_batch->capacity() - row_batch->num_rows());
  for (int n = 0; n < num_tuples; ++n) {
    DCHECK(!row_batch->IsFull());
    *row_idx = row_batch->AddRow();
    DCHECK(*row_idx != RowBatch::INVALID_ROW_INDEX);
    TupleRow* current_row = row_batch->GetRow(*row_idx);
    current_row->SetTuple(tuple_idx_, template_tuple_);
    row_batch->CommitLastRow();
  }
  num_rows_returned_ += num_tuples;
  scan_node_->IncrNumRowsReturned(num_tuples);
  *row_idx = RowBatch::INVALID_ROW_INDEX;
  return Status::OK;
}
