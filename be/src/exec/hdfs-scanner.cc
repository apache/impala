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

const char* FieldLocation::LLVM_CLASS_NAME = "struct.impala::FieldLocation";

HdfsScanner::HdfsScanner(HdfsScanNode* scan_node, RuntimeState* state, 
                         Tuple* template_tuple, MemPool* tuple_pool)
    : scan_node_(scan_node),
      state_(state),
      tuple_buffer_(NULL),
      tuple_byte_size_(scan_node->tuple_desc()->byte_size()),
      tuple_idx_(0),
      tuple_pool_(tuple_pool),
      tuple_(NULL),
      num_errors_in_file_(0),
      current_byte_stream_(NULL),
      template_tuple_(template_tuple),
      has_noncompact_strings_(!scan_node->compact_data() &&
                              !scan_node->tuple_desc()->string_slots().empty()),
      current_scan_range_(NULL),
      num_null_bytes_(scan_node->tuple_desc()->num_null_bytes()) {
}

HdfsScanner::~HdfsScanner() {
}

Status HdfsScanner::Prepare() {
  // TODO(marcel): add int tuple_idx_[] indexed by TupleId somewhere in runtime-state.h
  tuple_idx_ = 0;
  return Status::OK;
}

void HdfsScanner::AllocateTupleBuffer(RowBatch* row_batch) {
  if (tuple_byte_size_ == 0) {
    tuple_byte_size_ = 0;
  } else if (tuple_ == NULL) {
    // create new tuple buffer for row_batch
    tuple_buffer_size_ = row_batch->capacity() * tuple_byte_size_;
    tuple_buffer_ = tuple_pool_->Allocate(tuple_buffer_size_);
    tuple_ = reinterpret_cast<Tuple*>(tuple_buffer_);
  }
}

Status HdfsScanner::InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
    HdfsScanRange* range, Tuple* template_tuple, ByteStream* byte_stream) {
  // There is only one case where the scanner would get a new template_tuple
  // memory location.  Most of the time, the template_tuple is copied into
  // the tuple being materialized so the template tuple memory can be reused.  
  // However, if there are no materialized slots (partition key only), then the 
  // template tuple address is directly put into the tuplerows.  In this case we need
  // a new memory location for the template tuple.  The memory savings is
  // not important but the address of template_tuple is baked into the codegen
  // functions.  In the case with no materialized slots, there is no codegen
  // function so it is not a problem.
  DCHECK(scan_node_->materialized_slots().size() == 0 || 
      template_tuple == template_tuple_);
  DCHECK(byte_stream != NULL);
  current_scan_range_ = range;
  template_tuple_ = template_tuple;
  current_byte_stream_ = byte_stream;
  has_noncompact_strings_ = (!scan_node_->compact_data() &&
                             !scan_node_->tuple_desc()->string_slots().empty());
  return Status::OK;
}

// In this code path, no slots were materialized from the input files.  The only
// slots are from partition keys.  This lets us simplify writing out the batches.
//   1. template_tuple_ is the complete tuple.
//   2. Eval conjuncts against the tuple.
//   3. If it passes, stamp out 'num_tuples' copies of it into the row_batch.
int HdfsScanner::WriteEmptyTuples(RowBatch* row_batch, int num_tuples) {
  // Cap the number of result tuples up at the limit
  if (scan_node_->limit() != -1) {
    num_tuples = min(num_tuples,
        static_cast<int>(scan_node_->limit() - scan_node_->rows_returned()));
  }
  DCHECK_GT(num_tuples, 0);

  if (template_tuple_ == NULL) {
    // No slots from partitions keys or slots.  This is count(*).  Just add the
    // number of rows to the batch.
    row_batch->AddRows(num_tuples);
    row_batch->CommitRows(num_tuples);
  } else {
    // Make a row and evaluate the row
    int row_idx = row_batch->AddRow();
    
    TupleRow* current_row = row_batch->GetRow(row_idx);
    current_row->SetTuple(tuple_idx_, template_tuple_);
    if (!scan_node_->EvalConjunctsForScanner(current_row)) {
      return 0;
    }
    // Add first tuple
    row_batch->CommitLastRow();
    scan_node_->IncrNumRowsReturned();
    --num_tuples;

    DCHECK_LE(num_tuples, row_batch->capacity() - row_batch->num_rows());

    for (int n = 0; n < num_tuples; ++n) {
      DCHECK(!row_batch->IsFull());
      row_idx = row_batch->AddRow();
      DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
      TupleRow* current_row = row_batch->GetRow(row_idx);
      current_row->SetTuple(tuple_idx_, template_tuple_);
      row_batch->CommitLastRow();
    }
  } 
  scan_node_->IncrNumRowsReturned(num_tuples);
  return num_tuples;
}
  
void HdfsScanner::ReportColumnParseError(const SlotDescriptor* desc, 
    const char* data, int len) {
  if (state_->LogHasSpace()) {
    state_->error_stream()
      << "Error converting column: " 
      << desc->col_pos() - scan_node_->num_partition_keys()
      << " TO " << TypeToString(desc->type()) 
      << " (Data is: " << string(data,len) << ")" << endl;
  }
}
