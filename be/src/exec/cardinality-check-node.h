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


#ifndef IMPALA_EXEC_CARDINALITY_CHECK_NODE_H
#define IMPALA_EXEC_CARDINALITY_CHECK_NODE_H

#include "exec/exec-node.h"
#include <boost/scoped_ptr.hpp>

namespace impala {

class CardinalityCheckPlanNode : public PlanNode {
 public:
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~CardinalityCheckPlanNode(){}
};

/// Node that returns an error if its child produces more than a single row.
/// If successful, this node returns a deep copy of its single input row.
///
/// Note that this node must be a blocking node. It would be incorrect to return rows
/// before the single row constraint has been validated because downstream exec nodes
/// might produce results and incorrectly return them to the client. If the child of this
/// node produces more than one row it means the SQL query is semantically invalid, so no
/// rows must be returned to the client.

class CardinalityCheckNode : public ExecNode {
 public:
  CardinalityCheckNode(ObjectPool* pool, const CardinalityCheckPlanNode& pnode,
      const DescriptorTbl& descs);

  virtual Status Prepare(RuntimeState* state) override;
  virtual Status Open(RuntimeState* state) override;
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  virtual void Close(RuntimeState* state) override;
 private:
  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// Row batch that contains a single row from child
  boost::scoped_ptr<RowBatch> row_batch_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  // The associated SQL statement for error reporting
  std::string display_statement_;
};

}

#endif
