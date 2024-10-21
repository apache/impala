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

#pragma once

#include <vector>

#include "exec/exec-node.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/types.h"

namespace impala {

class Tuple;
class TupleRow;
class FragmentState;
class DescriptorTbl;
class IcebergMergeCase;
class IcebergMergeCasePlan;
class ScalarExprEvaluator;
class ScalarExpr;
class RuntimeState;
class RowBatch;

/// Plan node for Iceberg merge node, contains plans for merge cases, scalar expression
/// for checking row presence, Iceberg-related virtual columns, and tuple identifiers
/// for merge action and for the target table.
class IcebergMergePlanNode : public PlanNode {
 public:
  Status Init(const TPlanNode& tnode, FragmentState* state) override;
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;

  IcebergMergePlanNode() = default;
  ~IcebergMergePlanNode() override = default;

  IcebergMergePlanNode(const IcebergMergePlanNode& other) = delete;
  IcebergMergePlanNode(IcebergMergePlanNode&& other) = delete;
  auto operator=(const IcebergMergePlanNode& other) = delete;
  auto operator=(IcebergMergePlanNode&& other) = delete;

  /// Plans for each merge case
  std::vector<IcebergMergeCasePlan*> merge_case_plans_;

  /// Expr that signals whether the merge row contains the
  /// target tuple, the source tuple, or both.
  ScalarExpr* row_present_ = nullptr;

  /// Exprs used to identify the position of each target record
  std::vector<ScalarExpr*> position_meta_exprs_;

  /// Exprs used to identify the partitioning properties of a record
  std::vector<ScalarExpr*> partition_meta_exprs_;

  /// The identifier of the merge action tuple that contains the information whether
  /// the output row should be updated, deleted or inserted.
  TTupleId merge_action_tuple_id_ = -1;

  /// Identifier of the target table's tuple
  TTupleId target_tuple_id_ = -1;
};

// Node that evaluates merge rows targeting Iceberg tables. The node receives the rows
// from the preceding join operator, the row consists of a 'row_present' column, the
// columns of the target table, the virtual columns of the target table and the columns
// of the source table. The output of the node uses a similar row descriptor, the target
// table's tuple is set, the source expression's tuples are emptied, and a new merge
// action tuple is filled based on the evaluation of the row.
class IcebergMergeNode : public ExecNode {
 public:
  IcebergMergeNode(
      ObjectPool* pool, const IcebergMergePlanNode& pnode, const DescriptorTbl& descs);

  Status Prepare(RuntimeState* state) override;
  Status Open(RuntimeState* state) override;
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;
  Status Reset(RuntimeState* state, RowBatch* row_batch) override;
  void Close(RuntimeState* state) override;
  const std::vector<ScalarExprEvaluator*>& PositionMetaEvals();
  const std::vector<ScalarExprEvaluator*>& PartitionMetaEvals();

 private:
  /// Processes the incoming row batch row-by-row, first check whether the incoming row
  /// is a duplicate, the second check tests if the row contains both the target and
  /// the source, or just the source. In the 'BOTH' case, the 'WHEN MATCHED' cases are
  /// tested by their filter evaluators. The evaluation respects the order of cases as
  /// they are defined in the query. In the 'SOURCE' case, the
  /// 'WHEN NOT MATCHED (BY TARGET)' cases are checked, in the 'TARGET' case the
  /// 'WHEN NOT MATCHED BY SOURCE' cases are checked similarly to the 'BOTH' case.
  /// The 'selected_case' pointer stores the first matched case. If the 'selected_case'
  /// is set, then a new row is added to the output row batch, and the output
  /// expressions are evaluated into the new row. The merge action is also set
  /// derived from the type of the selected case.
  Status EvaluateCases(RowBatch* output_batch);
  void AddRow(RowBatch* output_batch, IcebergMergeCase* merge_case, TupleRow* row);
  bool CheckCase(const IcebergMergeCase * merge_case, TupleRow* row);
  bool IsDuplicateRow(TupleRow* actual_row);

  std::vector<IcebergMergeCase*> matched_cases_;
  std::vector<IcebergMergeCase*> not_matched_by_target_cases_;
  std::vector<IcebergMergeCase*> not_matched_by_source_cases_;
  std::vector<IcebergMergeCase*> all_cases_;
  std::unique_ptr<RowBatch> child_row_batch_;
  int child_row_idx_;
  bool child_eos_;
  ScalarExpr* row_present_;
  std::vector<ScalarExpr*> position_meta_exprs_;
  std::vector<ScalarExpr*> partition_meta_exprs_;
  std::vector<ScalarExprEvaluator*> position_meta_evaluators_;
  std::vector<ScalarExprEvaluator*> partition_meta_evaluators_;
  ScalarExprEvaluator* row_present_evaluator_;

  /// Pointer to the last processed tuple from target table, used for duplicate filtering
  Tuple* previous_row_target_tuple_ = nullptr;

  /// Index of the merge action tuple in the row descriptor
  int merge_action_tuple_idx_ = -1;

  /// Index of the target tuple in the row descriptor
  int target_tuple_idx_ = -1;

  /// Type of the merge action tuple
  inline static const ColumnType merge_action_tuple_type_ = ColumnType(TYPE_TINYINT);
};

class IcebergMergeCasePlan {
 public:
  IcebergMergeCasePlan() = default;
  ~IcebergMergeCasePlan() = default;
  Status Init(const TIcebergMergeCase& tmerge_case, FragmentState* state,
      const RowDescriptor* row_desc);

  IcebergMergeCasePlan(const IcebergMergeCasePlan& other) = delete;
  IcebergMergeCasePlan(IcebergMergeCasePlan&& other) = delete;
  auto operator=(const IcebergMergeCasePlan& other) = delete;
  auto operator=(IcebergMergeCasePlan&& other) = delete;

  /// Filter conjuncts applied after matching the case
  std::vector<ScalarExpr*> filter_conjuncts_;
  std::vector<ScalarExpr*> output_exprs_;
  TMergeCaseType::type case_type_{};
  TMergeMatchType::type match_type_{};
};

class IcebergMergeCase {
 public:
  IcebergMergeCase(const IcebergMergeCasePlan* pcase);
  IcebergMergeCase() = delete;
  ~IcebergMergeCase() = default;
  IcebergMergeCase(const IcebergMergeCase& other) = delete;
  IcebergMergeCase(IcebergMergeCase&& other) = delete;
  auto operator=(const IcebergMergeCase& other) = delete;
  auto operator=(IcebergMergeCase&& other) = delete;

  Status Prepare(RuntimeState* state, IcebergMergeNode& parent);
  Status Open(RuntimeState* state);
  void Close(RuntimeState* state);

  [[nodiscard]] TIcebergMergeSinkAction::type SinkAction() const {
    if (case_type_ == TMergeCaseType::DELETE) {
      return TIcebergMergeSinkAction::DELETE;
    }
    if (case_type_ == TMergeCaseType::INSERT) {
      return TIcebergMergeSinkAction::DATA;
    }
    DCHECK(case_type_ == TMergeCaseType::UPDATE);
    return TIcebergMergeSinkAction::BOTH;
  }

  std::vector<ScalarExpr*> filter_conjuncts_;
  std::vector<ScalarExpr*> output_exprs_;
  TMergeCaseType::type case_type_{};
  TMergeMatchType::type match_type_{};

  std::vector<ScalarExprEvaluator*> filter_evaluators_;
  std::vector<ScalarExprEvaluator*> output_evaluators_;
  std::vector<ScalarExprEvaluator*> combined_evaluators_;
};

} // namespace impala
