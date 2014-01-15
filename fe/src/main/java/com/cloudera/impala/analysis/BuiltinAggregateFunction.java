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

import java.util.ArrayList;

import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAggregateFunction;
import com.cloudera.impala.thrift.TAggregationOp;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.google.common.base.Preconditions;

/**
 * Internal representation of a builtin aggregate function.
 */
public class BuiltinAggregateFunction extends Function {
  // TODO: this is effectively a catalog of builtin aggregate functions.
  // We should move this to something in the catalog instead of having it
  // here like this.
  public enum Operator {
    COUNT("COUNT", TAggregationOp.COUNT, ColumnType.BIGINT),
    MIN("MIN", TAggregationOp.MIN, null),
    MAX("MAX", TAggregationOp.MAX, null),
    DISTINCT_PC("DISTINCT_PC", TAggregationOp.DISTINCT_PC,
        // TODO: this needs to switch to CHAR(64)
        ColumnType.STRING),
    DISTINCT_PCSA("DISTINCT_PCSA", TAggregationOp.DISTINCT_PCSA,
        // TODO: this needs to switch to CHAR(64)
        ColumnType.STRING),
    SUM("SUM", TAggregationOp.SUM, null),
    AVG("AVG", TAggregationOp.INVALID, null),
    GROUP_CONCAT("GROUP_CONCAT", TAggregationOp.GROUP_CONCAT,
        // TODO: this needs to switch to CHAR(16)
        ColumnType.STRING),

    // NDV is the external facing name (i.e. queries should always be written with NDV)
    // The current implementation of NDV is hyperloglog (but we could change this without
    // external query changes if we find a better algorithm).
    NDV("NDV", TAggregationOp.HLL,
        // TODO: this needs to switch to CHAR(64)
        ColumnType.STRING);


    private final String description;
    private final TAggregationOp thriftOp;

    // The intermediate type for this function if it is constant regardless of
    // input type. Set to null if it can only be determined during analysis.
    private final ColumnType intermediateType;

    private Operator(String description, TAggregationOp thriftOp,
        ColumnType intermediateType) {
      this.description = description;
      this.thriftOp = thriftOp;
      this.intermediateType = intermediateType;
    }

    @Override
    public String toString() { return description; }
    public TAggregationOp toThrift() { return thriftOp; }
    public ColumnType intermediateType() { return intermediateType; }
  }

  private final Operator op_;
  // TODO: this is not used yet until the planner understand this.
  private ColumnType intermediateType_;

  public BuiltinAggregateFunction(Operator op, ArrayList<ColumnType> argTypes,
      ColumnType retType, ColumnType intermediateType) throws AnalysisException {
    super(FunctionName.CreateBuiltinName(op.toString()), argTypes, retType, false);
    Preconditions.checkState(intermediateType != null);
    Preconditions.checkState(op != null);
    intermediateType.analyze();
    op_ = op;
    intermediateType_ = intermediateType;
    setBinaryType(TFunctionBinaryType.BUILTIN);
  }

  @Override
  public TFunction toThrift() {
    TFunction fn = super.toThrift();
    // TODO: for now, just put the op_ enum as the id.
    fn.setId(op_.thriftOp.ordinal());
    fn.setAggregate_fn(new TAggregateFunction(intermediateType_.toThrift()));
    return fn;
  }

  public Operator op() { return op_; }
  public ColumnType getIntermediateType() { return intermediateType_; }
  public void setIntermediateType(ColumnType t) { intermediateType_ = t; }
}
