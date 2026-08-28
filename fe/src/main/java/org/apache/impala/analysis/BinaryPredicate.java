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

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.extdatasource.thrift.TComparisonOp;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TExprNodeType;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;

/**
 * Most predicates with two operands.
 *
 */
public class BinaryPredicate extends Predicate {
  // true if this BinaryPredicate is inferred from slot equivalences, false otherwise.
  private boolean isInferred_ = false;

  private ExprId betweenExprId_ = null;
  private double betweenSelectivity_ = -1;

  public enum Operator {
    EQ("=", "eq", TComparisonOp.EQ),
    NE("!=", "ne", TComparisonOp.NE),
    LE("<=", "le", TComparisonOp.LE),
    GE(">=", "ge", TComparisonOp.GE),
    LT("<", "lt", TComparisonOp.LT),
    GT(">", "gt", TComparisonOp.GT),
    DISTINCT_FROM("IS DISTINCT FROM", "distinctfrom", TComparisonOp.DISTINCT_FROM),
    NOT_DISTINCT("IS NOT DISTINCT FROM", "notdistinct", TComparisonOp.NOT_DISTINCT),
    // Same as EQ, except it returns True if both sides are NULL. There is no backend
    // function for this. The functionality is embedded in the hash-join implementation.
    NULL_MATCHING_EQ("=", "null_matching_eq", TComparisonOp.EQ);

    private final String description_;
    private final String name_;
    private final TComparisonOp thriftOp_;

    private Operator(String description, String name, TComparisonOp thriftOp) {
      this.description_ = description;
      this.name_ = name;
      this.thriftOp_ = thriftOp;
    }

    @Override
    public String toString() { return description_; }
    public String getName() { return name_; }
    public TComparisonOp getThriftOp() { return thriftOp_; }
    public boolean isEquivalence() { return this == EQ || this == NOT_DISTINCT; }
    public boolean isSqlEquivalence() { return this == EQ; }


    /**
     * Test if the operator specifies a single range.
    **/
    public boolean isSingleRange() {
      return this == EQ || this == LE || this == GE || this == LT || this == GT;
    }

    public Operator converse() {
      switch (this) {
        case EQ: return EQ;
        case NE: return NE;
        case LE: return GE;
        case GE: return LE;
        case LT: return GT;
        case GT: return LT;
        case DISTINCT_FROM: return DISTINCT_FROM;
        case NOT_DISTINCT: return NOT_DISTINCT;
        case NULL_MATCHING_EQ:
          throw new IllegalStateException("Not implemented");
        default: throw new IllegalStateException("Invalid operator");
      }
    }
  }

  public static final com.google.common.base.Predicate<BinaryPredicate>
      IS_RANGE_PREDICATE = new com.google.common.base.Predicate<BinaryPredicate>() {
        @Override
        public boolean apply(BinaryPredicate arg) {
          return arg.getOp() == Operator.LT
              || arg.getOp() == Operator.LE
              || arg.getOp() == Operator.GT
              || arg.getOp() == Operator.GE;
        }
      };
  public static final com.google.common.base.Predicate<BinaryPredicate> IS_EQ_PREDICATE =
      new com.google.common.base.Predicate<BinaryPredicate>() {
        @Override
        public boolean apply(BinaryPredicate arg) {
          return arg.getOp() == Operator.EQ;
        }
      };

  public static void initBuiltins(Db db) {
    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.EQ.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.NE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.LE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.GE.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.LT.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
      db.addBuiltin(ScalarFunction.createBuiltinOperator(
          Operator.GT.getName(), Lists.newArrayList(t, t), Type.BOOLEAN));
    }
  }

  private Operator op_;

  public Operator getOp() { return op_; }
  public void setOp(Operator op) { op_ = op; }

  public BinaryPredicate(Operator op, Expr e1, Expr e2) {
    super();
    this.op_ = op;
    Preconditions.checkNotNull(e1);
    children_.add(e1);
    Preconditions.checkNotNull(e2);
    children_.add(e2);
  }

  protected BinaryPredicate(BinaryPredicate other) {
    super(other);
    op_ = other.op_;
    isInferred_ = other.isInferred_;
    betweenExprId_ = other.betweenExprId_;
    betweenSelectivity_ = other.betweenSelectivity_;
  }

  public boolean isNullMatchingEq() { return op_ == Operator.NULL_MATCHING_EQ; }

  public boolean isInferred() { return isInferred_; }
  public void setIsInferred() { isInferred_ = true; }

  public boolean hasIdenticalOperands() {
    return getChild(0) != null && getChild(0).equals(getChild(1));
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return getChild(0).toSql(options) + " " + op_.toString() + " "
        + getChild(1).toSql(options);
  }

  @Override
  protected void toThrift(TExprNode msg) {
    Preconditions.checkState(children_.size() == 2);
    // Cannot serialize a nested predicate.
    Preconditions.checkState(!contains(Subquery.class));
    // This check is important because we often clone and/or evaluate predicates,
    // and it's easy to get the casting logic wrong, e.g., cloned predicates
    // with expr substitutions need to be re-analyzed with reanalyze().
    Preconditions.checkState(getChild(0).getType().getPrimitiveType() ==
                             getChild(1).getType().getPrimitiveType(),
        "child 0 type: " + getChild(0).getType() +
        " child 1 type: " + getChild(1).getType());
    msg.node_type = TExprNodeType.FUNCTION_CALL;
  }

  @Override
  public String debugString() {
    MoreObjects.ToStringHelper toStrHelper = MoreObjects.toStringHelper(this);
    toStrHelper.add("op", op_).addValue(super.debugString());
    if (isAuxExpr()) toStrHelper.add("isAux", true);
    if (isInferred_) toStrHelper.add("isInferred", true);
    if (derivedFromBetween()) {
      toStrHelper.add("betweenExprId", betweenExprId_);
      toStrHelper.add("betweenSelectivity", betweenSelectivity_);
    }
    return toStrHelper.toString();
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    convertNumericLiteralsFromDecimal(analyzer);
    String opName = op_.getName().equals("null_matching_eq") ? "eq" : op_.getName();
    fn_ = getBuiltinFunction(analyzer, opName, collectChildReturnTypes(),
        CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    if (fn_ == null) {
      throw new AnalysisException("operands of type " + getChild(0).getType().toSql() +
          " and " + getChild(1).getType().toSql()  + " are not comparable: " + toSql());
    }
    Preconditions.checkState(fn_.getReturnType().isBoolean());

    List<Expr> subqueries = new ArrayList<>();
    collectAll(Predicates.instanceOf(Subquery.class), subqueries);
    if (subqueries.size() > 1) {
      // TODO Remove that restriction when we add support for independent subquery
      // evaluation.
      throw new AnalysisException("Multiple subqueries are not supported in binary " +
          "predicates: " + toSql());
    }
    if (contains(ExistsPredicate.class)) {
      throw new AnalysisException("EXISTS subquery predicates are not " +
          "supported in binary predicates: " + toSql());
    }

    List<InPredicate> inPredicates = new ArrayList<>();
    collect(InPredicate.class, inPredicates);
    for (InPredicate inPredicate: inPredicates) {
      if (inPredicate.contains(Subquery.class)) {
        throw new AnalysisException("IN subquery predicates are not supported in " +
            "binary predicates: " + toSql());
      }
    }

    if (!contains(Subquery.class)) {
      // Don't perform any casting for predicates with subqueries here. Any casting
      // required will be performed when the subquery is unnested.
      castForFunctionCall(true, analyzer.getRegularCompatibilityLevel());
    }

    // Determine selectivity
    computeSelectivity();
  }

  protected void computeSelectivity() {
    // TODO: Compute selectivity for nested predicates.
    // TODO: Improve estimation using histograms.
    if (hasValidSelectivityHint()) {
      return;
    }

    Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
    if (!isSingleColumnPredicate(slotRefRef, null)) {
      return;
    }
    boolean rChildIsNull = Expr.IS_NULL_LITERAL.apply(getChild(1));
    long distinctValues = slotRefRef.getRef().getNumDistinctValues();
    if (distinctValues < 0) {
      // Lack of statistics to estimate the selectivity.
      return;
    } else if ((distinctValues == 0 && (op_ == Operator.EQ || op_ == Operator.NE))
        || (rChildIsNull && (op_ == Operator.EQ || op_ == Operator.NE))) {
      // If the table is empty, then distinctValues is 0. This case is equivalent
      // to selectivity is 0.
      // For case <column> = NULL or <column> != NULL, all values are false
      selectivity_ = 0.0;
      return;
    }

    if (op_ == Operator.EQ || op_ == Operator.NOT_DISTINCT) {
      selectivity_ = 1.0 / distinctValues;
    } else if (op_ == Operator.NE || op_ == Operator.DISTINCT_FROM) {
      // For case <column> IS DISTINCT FROM NULL, all non-null values are true
      if (op_ == Operator.DISTINCT_FROM && rChildIsNull) {
        selectivity_ = 1.0;
      } else {
        // avoid 0.0 selectivity if ndv == 1 (IMPALA-11301).
        selectivity_ = distinctValues == 1 ? 0.5 : (1.0 - 1.0 / distinctValues);
      }
    } else {
      return;
    }

    SlotDescriptor slotDesc = slotRefRef.getRef().getDesc();
    if (slotDesc.getStats().hasNullsStats()) {
      FeTable table = slotDesc.getParent().getTable();
      if (table != null && table.getNumRows() > 0) {
        long numRows = table.getNumRows();
        long numNulls = slotDesc.getStats().getNumNulls();
        if (op_ == Operator.EQ || op_ == Operator.NE
            || (op_ == Operator.DISTINCT_FROM && rChildIsNull)
            || (op_ == Operator.NOT_DISTINCT && !rChildIsNull)) {
          // For =, !=, "is distinct from null" and "is not distinct from non-null",
          // all null values are false.
          selectivity_ *= (double) (numRows - numNulls) / numRows;
        } else if (op_ == Operator.NOT_DISTINCT && rChildIsNull) {
          // For is not distinct from null, only null values are true
          selectivity_ = numNulls / numRows;
        } else if (op_ == Operator.DISTINCT_FROM && !rChildIsNull) {
          // For is distinct from not-null, null values are true, So need to add it
          selectivity_ = selectivity_ * (double) (numRows - numNulls) / numRows +
              numNulls / numRows;
        }
      }
    }

    selectivity_ = Math.max(0, Math.min(1, selectivity_));
  }

  @Override
  protected float computeEvalCost() {
    if (!hasChildCosts()) return UNKNOWN_COST;
    if (getChild(0).getType().isFixedLengthType()) {
      return getChildCosts() + BINARY_PREDICATE_COST;
    } else if (getChild(0).getType().isStringType()) {
      return getChildCosts() +
          (float) (getAvgStringLength(getChild(0)) + getAvgStringLength(getChild(1))) *
              BINARY_PREDICATE_COST;
    } else {
      //TODO(tmarshall): Handle other var length types here.
      return getChildCosts() + VAR_LEN_BINARY_PREDICATE_COST;
    }
  }

  /**
   * If predicate is of the form "<slotref> <op> <expr>", returns expr,
   * otherwise returns null. Slotref may be wrapped in a CastExpr.
   * TODO: revisit CAST handling at the caller
   */
  public Expr getSlotBinding(SlotId id) {
    // BinaryPredicates are normalized, so we only need to check the left operand.
    SlotRef slotRef = getChild(0).unwrapSlotRef(false);
    if (slotRef != null && slotRef.getSlotId() == id) return getChild(1);
    return null;
  }

  /**
   * If e is an equality predicate between two slots that only require implicit
   * casts, returns those two slots; otherwise returns null.
   */
  public static Pair<SlotId, SlotId> getEqSlots(Expr e) {
    if (!(e instanceof BinaryPredicate)) return null;
    return ((BinaryPredicate) e).getEqSlots();
  }

  /**
   * If this is an equality predicate between two slots that only require implicit
   * casts, returns those two slots; otherwise returns null.
   */
  @Override
  public Pair<SlotId, SlotId> getEqSlots() {
    if (op_ != Operator.EQ) return null;
    SlotRef lhs = getChild(0).unwrapSlotRef(true);
    if (lhs == null) return null;
    SlotRef rhs = getChild(1).unwrapSlotRef(true);
    if (rhs == null) return null;
    return new Pair<SlotId, SlotId>(lhs.getSlotId(), rhs.getSlotId());
  }

  /**
   * If predicate is of the form "<SlotRef> op <Expr>", returns the SlotRef, otherwise
   * returns null.
   */
  @Override
  public SlotRef getBoundSlot() { return getChild(0).unwrapSlotRef(true); }

  /**
   * Negates a BinaryPredicate.
   */
  @Override
  public Expr negate() {
    Operator newOp = null;
    switch (op_) {
      case EQ:
        newOp = Operator.NE;
        break;
      case NE:
        newOp = Operator.EQ;
        break;
      case LT:
        newOp = Operator.GE;
        break;
      case LE:
        newOp = Operator.GT;
        break;
      case GE:
        newOp = Operator.LT;
        break;
      case GT:
        newOp = Operator.LE;
        break;
      case DISTINCT_FROM:
        newOp = Operator.NOT_DISTINCT;
        break;
      case NOT_DISTINCT:
        newOp = Operator.DISTINCT_FROM;
        break;
      case NULL_MATCHING_EQ:
        throw new IllegalStateException("Not implemented");
    }
    return new BinaryPredicate(newOp, getChild(0), getChild(1));
  }

  /**
   * Swaps the first and second child in-place and sets the operation to its converse.
   */
  public void reverse() {
    Collections.swap(children_, 0, 1);
    op_ = op_.converse();
  }

  @Override
  protected boolean localEquals(Expr that) {
    return super.localEquals(that) && op_.equals(((BinaryPredicate)that).op_);
  }

  @Override
  protected int localHash() {
    return Objects.hash(super.localHash(), op_);
  }

  @Override
  public Expr clone() { return new BinaryPredicate(this); }

  public void setBetweenSelectivity(ExprId betweenExprId, double betweenSelectivity) {
    betweenExprId_ = betweenExprId;
    betweenSelectivity_ = betweenSelectivity;
  }

  public boolean derivedFromBetween() { return betweenExprId_ != null; }
  public ExprId getBetweenExprId() { return betweenExprId_; }
  public double getBetweenSelectivity() { return betweenSelectivity_; }
}
