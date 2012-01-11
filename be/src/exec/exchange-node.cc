// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "exec/exchange-node.h"

#include <glog/logging.h>

#include "runtime/data-stream-mgr.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "gen-cpp/PlanNodes_types.h"

using namespace impala;
using namespace std;

ExchangeNode::ExchangeNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : ExecNode(pool, tnode, descs),
    num_senders_(tnode.exchange_node.num_senders),
    stream_recvr_(NULL) {
}

Status ExchangeNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Prepare(state));
  // TODO: figure out appropriate buffer size
  stream_recvr_.reset(state->stream_mgr()->CreateRecvr(
    state->desc_tbl(), state->query_id(), id_, num_senders_, 1024 * 1024));
  return Status::OK;
}

Status ExchangeNode::Open(RuntimeState* state) {
  return Status::OK;
}

Status ExchangeNode::GetNext(RuntimeState* state, RowBatch* output_batch, bool* eos) {
  RowBatch* input_batch = stream_recvr_->GetBatch();
  output_batch->Reset();
  *eos = (input_batch == NULL);
  if (*eos) return Status::OK;

  // We assume that we can always move the entire input batch into the output batch
  // (if that weren't the case, the code would be more complicated).
  DCHECK_GE(output_batch->capacity(), input_batch->capacity());

  // copy all rows and attach all mempools from the input batch
  DCHECK(input_batch->row_desc().IsPrefixOf(output_batch->row_desc()));
  for (int i = 0; i < input_batch->num_rows(); ++i) {
    TupleRow* src = input_batch->GetRow(i);
    int j = output_batch->AddRow();
    DCHECK_EQ(i, j);
    TupleRow* dest = output_batch->GetRow(i);
    // this works as expected if rows from input_batch form a prefix of
    // rows in output_batch
    input_batch->CopyRow(src, dest);
    output_batch->CommitLastRow();
  }
  input_batch->TransferTupleData(output_batch);
  return Status::OK;
}

Status ExchangeNode::Close(RuntimeState* state) {
  return Status::OK;
}

void ExchangeNode::DebugString(int indentation_level, std::stringstream* out) const {
  *out << string(indentation_level * 2, ' ');
  *out << "ExchangeNode(#senders=" << num_senders_;
  ExecNode::DebugString(indentation_level, out);
  *out << ")";
}

