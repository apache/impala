// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXEC_EXCHANGE_NODE_H
#define IMPALA_EXEC_EXCHANGE_NODE_H

#include <boost/scoped_ptr.hpp>
#include "exec/exec-node.h"
#include "runtime/data-stream-recvr.h"

namespace impala {

// Receiver node for data streams. This simply feeds row batches received from the
// data stream into the execution tree.
// The data stream is created in Prepare() and closed in the d'tor.
class ExchangeNode : public ExecNode {
 public:
  ExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // the number of senders needs to be set after the c'tor, because it's not
  // recorded in TPlanNode, and before calling Prepare()
  void set_num_senders(int num_senders) { num_senders_ = num_senders; }

 protected:
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  int num_senders_;  // needed for stream_recvr_ construction
  boost::scoped_ptr<DataStreamRecvr> stream_recvr_;
};

};

#endif

