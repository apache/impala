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

#ifndef IMPALA_EXPRS_KUDU_PARTITION_EXPR_H_
#define IMPALA_EXPRS_KUDU_PARTITION_EXPR_H_

#include <kudu/client/client.h>
#include <kudu/common/partial_row.h>

#include "exprs/scalar-expr.h"

namespace impala {

class KuduTableDescriptor;
class ScalarExprEvaluator;
class TExprNode;
class TKuduPartitionExpr;

/// Expr that calls into the Kudu client to determine the partition index for rows.
/// Returns -1 if the row doesn't have a partition or if an error is encountered.
/// The children of this Expr produce the values for the partition columns.
class KuduPartitionExpr : public ScalarExpr {
 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  KuduPartitionExpr(const TExprNode& node);

  virtual Status Init(const RowDescriptor& row_desc, bool is_entry_point,
      RuntimeState* state) override WARN_UNUSED_RESULT;

  virtual IntVal GetIntValInterpreted(
      ScalarExprEvaluator* eval, const TupleRow* row) const override;

  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;

 private:
  TKuduPartitionExpr tkudu_partition_expr_;

  /// Descriptor of the table to use the partiitoning scheme from. Set in Prepare().
  KuduTableDescriptor* table_desc_;

  /// Used to call into Kudu to determine partitions. Set in Prepare().
  std::unique_ptr<kudu::client::KuduPartitioner> partitioner_;

  /// Stores the col values for each row that is partitioned.
  std::unique_ptr<kudu::KuduPartialRow> row_;
};

} // namespace impala

#endif // IMPALA_EXPRS_KUDU_PARTITION_EXPR_H_
