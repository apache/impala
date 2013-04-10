// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.List;

import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.cloudera.impala.thrift.TTupleIsNullPredicate;
import com.google.common.base.Preconditions;

/**
 * Internal expr that returns true if all of the given tuples are NULL, otherwise false.
 * Used to make exprs originating from an inline view nullable in an outer join.
 * The given tupleIds must be materialized and nullable at the appropriate PlanNode.
 */
public class TupleIsNullPredicate extends Predicate {

  private final List<TupleId> tupleIds;

  public TupleIsNullPredicate(List<TupleId> tupleIds) {
    Preconditions.checkState(tupleIds != null && !tupleIds.isEmpty());
    this.tupleIds = tupleIds;
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.TUPLE_IS_NULL_PRED;
    msg.tuple_is_null_pred = new TTupleIsNullPredicate();
    for (TupleId tid: tupleIds) {
      msg.tuple_is_null_pred.addToTuple_ids(tid.asInt());
    }
  }

  public List<TupleId> getTupleIds() {
    return tupleIds;
  }
}
