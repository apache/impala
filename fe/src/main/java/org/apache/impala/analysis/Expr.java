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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.catalog.Catalog;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.TreeNode;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Root of the expr node hierarchy.
 *
 */
abstract public class Expr extends TreeNode<Expr> implements ParseNode, Cloneable {
  private final static Logger LOG = LoggerFactory.getLogger(Expr.class);

  // Limits on the number of expr children and the depth of an expr tree. These maximum
  // values guard against crashes due to stack overflows (IMPALA-432) and were
  // experimentally determined to be safe.
  public final static int EXPR_CHILDREN_LIMIT = 10000;
  // The expr depth limit is mostly due to our recursive implementation of clone().
  public final static int EXPR_DEPTH_LIMIT = 1000;

  // Name of the function that needs to be implemented by every Expr that
  // supports negation.
  private final static String NEGATE_FN = "negate";

  // To be used where we cannot come up with a better estimate (selectivity_ is -1).
  public static double DEFAULT_SELECTIVITY = 0.1;

  // The relative costs of different Exprs. These numbers are not intended as a precise
  // reflection of running times, but as simple heuristics for ordering Exprs from cheap
  // to expensive.
  // TODO(tmwarshall): Get these costs in a more principled way, eg. with a benchmark.
  public final static float ARITHMETIC_OP_COST = 1;
  public final static float BINARY_PREDICATE_COST = 1;
  public final static float VAR_LEN_BINARY_PREDICATE_COST = 5;
  public final static float CAST_COST = 1;
  public final static float COMPOUND_PREDICATE_COST = 1;
  public final static float FUNCTION_CALL_COST = 10;
  public final static float IS_NOT_EMPTY_COST = 1;
  public final static float IS_NULL_COST = 1;
  public final static float LIKE_COST = 10;
  public final static float LITERAL_COST = 1;
  public final static float SLOT_REF_COST = 1;
  public final static float TIMESTAMP_ARITHMETIC_COST = 5;
  public final static float UNKNOWN_COST = -1;

  // To be used when estimating the cost of Exprs of type string where we don't otherwise
  // have an estimate of how long the strings produced by that Expr are.
  public final static int DEFAULT_AVG_STRING_LENGTH = 5;

  // returns true if an Expr is a non-analytic aggregate.
  private final static com.google.common.base.Predicate<Expr> isAggregatePredicate_ =
      new com.google.common.base.Predicate<Expr>() {
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr &&
              ((FunctionCallExpr)arg).isAggregateFunction();
        }
      };

  // Returns true if an Expr is a NOT CompoundPredicate.
  public final static com.google.common.base.Predicate<Expr> IS_NOT_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof CompoundPredicate &&
              ((CompoundPredicate)arg).getOp() == CompoundPredicate.Operator.NOT;
        }
      };

  // Returns true if an Expr is an OR CompoundPredicate.
  public final static com.google.common.base.Predicate<Expr> IS_OR_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof CompoundPredicate &&
              ((CompoundPredicate)arg).getOp() == CompoundPredicate.Operator.OR;
        }
      };

  // Returns true if an Expr is a scalar subquery
  public final static com.google.common.base.Predicate<Expr> IS_SCALAR_SUBQUERY =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg.isScalarSubquery();
        }
      };

  // Returns true if an Expr is an aggregate function that returns non-null on
  // an empty set (e.g. count).
  public final static com.google.common.base.Predicate<Expr>
      NON_NULL_EMPTY_AGG = new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr &&
              ((FunctionCallExpr)arg).returnsNonNullOnEmpty();
        }
      };

  // Returns true if an Expr is a builtin aggregate function.
  public final static com.google.common.base.Predicate<Expr> IS_BUILTIN_AGG_FN =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr &&
              ((FunctionCallExpr)arg).getFnName().isBuiltin();
        }
      };

  public final static com.google.common.base.Predicate<Expr> IS_TRUE_LITERAL =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof BoolLiteral && ((BoolLiteral)arg).getValue();
        }
      };

  public final static com.google.common.base.Predicate<Expr> IS_FALSE_LITERAL =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof BoolLiteral && !((BoolLiteral)arg).getValue();
        }
      };

  public final static com.google.common.base.Predicate<Expr> IS_EQ_BINARY_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) { return BinaryPredicate.getEqSlots(arg) != null; }
      };

  public final static com.google.common.base.Predicate<Expr> IS_NOT_EQ_BINARY_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof BinaryPredicate
              && ((BinaryPredicate) arg).getOp() != Operator.EQ
              && ((BinaryPredicate) arg).getOp() != Operator.NOT_DISTINCT;
        }
      };

  public final static com.google.common.base.Predicate<Expr> IS_BINARY_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) { return arg instanceof BinaryPredicate; }
      };

  public final static com.google.common.base.Predicate<Expr> IS_EXPR_EQ_LITERAL_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
    @Override
    public boolean apply(Expr arg) {
      return arg instanceof BinaryPredicate
          && ((BinaryPredicate) arg).getOp() == Operator.EQ
          && (((BinaryPredicate) arg).getChild(1).isLiteral());
    }
  };

  public final static com.google.common.base.Predicate<Expr>
      IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr
              && ((FunctionCallExpr) arg).isNondeterministicBuiltinFn();
        }
      };

  public final static com.google.common.base.Predicate<Expr> IS_UDF_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr
              && !((FunctionCallExpr) arg).getFnName().isBuiltin();
        }
      };

  // id that's unique across the entire query statement and is assigned by
  // Analyzer.registerConjuncts(); only assigned for the top-level terms of a
  // conjunction, and therefore null for most Exprs
  protected ExprId id_;

  // true if Expr is an auxiliary predicate that was generated by the plan generation
  // process to facilitate predicate propagation;
  // false if Expr originated with a query stmt directly
  private boolean isAuxExpr_ = false;

  protected Type type_;  // result of analysis

  protected boolean isOnClauseConjunct_; // set by analyzer

  // Flag to indicate whether to wrap this expr's toSql() in parenthesis. Set by parser.
  // Needed for properly capturing expr precedences in the SQL string.
  protected boolean printSqlInParens_ = false;

  // Estimated probability of a predicate evaluating to true. Set during analysis.
  // Between 0 and 1, or set to -1 if the selectivity could not be estimated.
  protected double selectivity_;

  // Estimated relative cost of evaluating this expression, including the costs of
  // its children. Set during analysis and used to sort conjuncts within a PlanNode.
  // Has a default value of -1 indicating unknown cost if the cost of this expression
  // or any of its children was not set, but it is required to be set for any
  // expression which may be part of a conjunct.
  protected float evalCost_;

  // estimated number of distinct values produced by Expr; invalid: -1
  // set during analysis
  protected long numDistinctValues_;

  // Cached value of IsConstant(), set during analyze() and valid if isAnalyzed_ is true.
  private boolean isConstant_;

  // The function to call. This can either be a scalar or aggregate function.
  // Set in analyze().
  protected Function fn_;

  // True after analysis successfully completed. Protected by accessors isAnalyzed() and
  // analysisDone().
  private boolean isAnalyzed_ = false;

  protected Expr() {
    super();
    type_ = Type.INVALID;
    selectivity_ = -1.0;
    evalCost_ = -1.0f;
    numDistinctValues_ = -1;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected Expr(Expr other) {
    id_ = other.id_;
    isAuxExpr_ = other.isAuxExpr_;
    type_ = other.type_;
    isAnalyzed_ = other.isAnalyzed_;
    isOnClauseConjunct_ = other.isOnClauseConjunct_;
    printSqlInParens_ = other.printSqlInParens_;
    selectivity_ = other.selectivity_;
    evalCost_ = other.evalCost_;
    numDistinctValues_ = other.numDistinctValues_;
    isConstant_ = other.isConstant_;
    fn_ = other.fn_;
    children_ = Expr.cloneList(other.children_);
  }

  public boolean isAnalyzed() { return isAnalyzed_; }
  public ExprId getId() { return id_; }
  protected void setId(ExprId id) { id_ = id; }
  public Type getType() { return type_; }
  public double getSelectivity() { return selectivity_; }
  public boolean hasSelectivity() { return selectivity_ >= 0; }
  public float getCost() {
    Preconditions.checkState(isAnalyzed_);
    return evalCost_;
  }
  public boolean hasCost() { return evalCost_ >= 0; }
  public long getNumDistinctValues() { return numDistinctValues_; }
  public boolean getPrintSqlInParens() { return printSqlInParens_; }
  public void setPrintSqlInParens(boolean b) { printSqlInParens_ = b; }
  public boolean isOnClauseConjunct() { return isOnClauseConjunct_; }
  public void setIsOnClauseConjunct(boolean b) { isOnClauseConjunct_ = b; }
  public boolean isAuxExpr() { return isAuxExpr_; }
  public void setIsAuxExpr() { isAuxExpr_ = true; }
  public Function getFn() { return fn_; }

  /**
   * Perform semantic analysis of node and all of its children.
   * Throws exception if any errors found.
   * @see org.apache.impala.parser.ParseNode#analyze(org.apache.impala.parser.Analyzer)
   */
  public final void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;

    // Check the expr child limit.
    if (children_.size() > EXPR_CHILDREN_LIMIT) {
      String sql = toSql();
      String sqlSubstr = sql.substring(0, Math.min(80, sql.length()));
      throw new AnalysisException(String.format("Exceeded the maximum number of child " +
          "expressions (%s).\nExpression has %s children:\n%s...",
          EXPR_CHILDREN_LIMIT, children_.size(), sqlSubstr));
    }

    // analyzer may be null for certain literal constructions (e.g. IntLiteral).
    if (analyzer != null) {
      analyzer.incrementCallDepth();
      // Check the expr depth limit. Do not print the toSql() to not overflow the stack.
      if (analyzer.getCallDepth() > EXPR_DEPTH_LIMIT) {
        throw new AnalysisException(String.format("Exceeded the maximum depth of an " +
            "expression tree (%s).", EXPR_DEPTH_LIMIT));
      }
    }
    for (Expr child: children_) {
      child.analyze(analyzer);
    }
    if (analyzer != null) analyzer.decrementCallDepth();
    computeNumDistinctValues();

    // Do all the analysis for the expr subclass before marking the Expr analyzed.
    analyzeImpl(analyzer);
    evalCost_ = computeEvalCost();
    analysisDone();
  }

  /**
   * Does subclass-specific analysis. Subclasses should override analyzeImpl().
   */
  abstract protected void analyzeImpl(Analyzer analyzer) throws AnalysisException;

  /**
   * Helper function to analyze this expr and assert that the analysis was successful.
   * TODO: This function could be used in many more places to clean up. Consider
   * adding an IAnalyzable interface or similar to and move this helper into Analyzer
   * such that non-Expr things can use the helper also.
   */
  public void analyzeNoThrow(Analyzer analyzer) {
    try {
      analyze(analyzer);
    } catch (AnalysisException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Compute and return evalcost of this expr given the evalcost of all children has been
   * computed. Should be called bottom-up whenever the structure of subtree is modified.
   */
  abstract protected float computeEvalCost();

  protected void computeNumDistinctValues() {
    if (isConstant()) {
      numDistinctValues_ = 1;
    } else {
      numDistinctValues_ = -1;

      // get the max number of distinct values over all children of this node
      for (Expr child: children_) {
        // A constant should not override a -1 from a SlotRef, so we only consider
        // non-constant expressions. This is functionally similar to considering
        // only the SlotRefs, except that it allows an Expr to override the values
        // that come out of its children.
        if (!child.isConstant()) {
          numDistinctValues_ = Math.max(numDistinctValues_, child.getNumDistinctValues());
        }
      }
    }
  }

  /**
   * Collects the returns types of the child nodes in an array.
   */
  protected Type[] collectChildReturnTypes() {
    Type[] childTypes = new Type[children_.size()];
    for (int i = 0; i < children_.size(); ++i) {
      childTypes[i] = children_.get(i).type_;
    }
    return childTypes;
  }

  /**
   * Looks up in the catalog the builtin for 'name' and 'argTypes'.
   * Returns null if the function is not found.
   */
  protected Function getBuiltinFunction(Analyzer analyzer, String name,
      Type[] argTypes, CompareMode mode) throws AnalysisException {
    FunctionName fnName = new FunctionName(Catalog.BUILTINS_DB, name);
    Function searchDesc = new Function(fnName, argTypes, Type.INVALID, false);
    return analyzer.getCatalog().getFunction(searchDesc, mode);
  }

  /**
   * Generates the necessary casts for the children of this expr to call fn_.
   * child(0) is cast to the function's first argument, child(1) to the second etc.
   * This does not do any validation and the casts are assumed to be safe.
   *
   * If ignoreWildcardDecimals is true, the function will not cast arguments that
   * are wildcard decimals. This is used for builtins where the cast is done within
   * the BE function.
   * Otherwise, if the function signature contains wildcard decimals, each wildcard child
   * argument will be cast to the highest resolution that can contain all of the child
   * wildcard arguments.
   * e.g. fn(decimal(*), decimal(*))
   *      called with fn(decimal(10,2), decimal(5,3))
   * both children will be cast to (11, 3).
   */
  protected void castForFunctionCall(boolean ignoreWildcardDecimals)
      throws AnalysisException {
    Preconditions.checkState(fn_ != null);
    Type[] fnArgs = fn_.getArgs();
    Type resolvedWildcardType = getResolvedWildCardType();
    for (int i = 0; i < children_.size(); ++i) {
      // For varargs, we must compare with the last type in fnArgs.argTypes.
      int ix = Math.min(fnArgs.length - 1, i);
      if (fnArgs[ix].isWildcardDecimal()) {
        if (children_.get(i).type_.isDecimal() && ignoreWildcardDecimals) continue;
        Preconditions.checkState(resolvedWildcardType != null);
        if (!children_.get(i).type_.equals(resolvedWildcardType)) {
          castChild(resolvedWildcardType, i);
        }
      } else if (!children_.get(i).type_.matchesType(fnArgs[ix])) {
        castChild(fnArgs[ix], i);
      }
    }
  }

  /**
   * Returns the max resolution type of all the wild card decimal types.
   * Returns null if there are no wild card types.
   */
  Type getResolvedWildCardType() throws AnalysisException {
    Type result = null;
    Type[] fnArgs = fn_.getArgs();
    for (int i = 0; i < children_.size(); ++i) {
      // For varargs, we must compare with the last type in fnArgs.argTypes.
      int ix = Math.min(fnArgs.length - 1, i);
      if (!fnArgs[ix].isWildcardDecimal()) continue;

      Type childType = children_.get(i).type_;
      Preconditions.checkState(!childType.isWildcardDecimal(),
          "Child expr should have been resolved.");
      Preconditions.checkState(childType.isScalarType(),
          "Function should not have resolved with a non-scalar child type.");
      ScalarType decimalType = (ScalarType) childType;
      if (result == null) {
        result = decimalType.getMinResolutionDecimal();
      } else {
        result = Type.getAssignmentCompatibleType(result, childType, false);
      }
    }
    if (result != null) {
      if (result.isNull()) {
        throw new AnalysisException(
            "Cannot resolve DECIMAL precision and scale from NULL type.");
      }
      Preconditions.checkState(result.isDecimal() && !result.isWildcardDecimal());
    }
    return result;
  }

  /**
   * Returns true if e is a CastExpr and the target type is a decimal.
   */
  private boolean isExplicitCastToDecimal(Expr e) {
    if (!(e instanceof CastExpr)) return false;
    CastExpr c = (CastExpr)e;
    return !c.isImplicit() && c.getType().isDecimal();
  }

  /**
   * Returns a clone of child with all decimal-typed NumericLiterals in it explicitly
   * cast to targetType.
   */
  private Expr convertDecimalLiteralsToFloat(Analyzer analyzer, Expr child,
      Type targetType) throws AnalysisException {
    if (!targetType.isFloatingPointType() && !targetType.isIntegerType()) return child;
    if (targetType.isIntegerType()) targetType = Type.DOUBLE;
    List<NumericLiteral> literals = Lists.newArrayList();
    child.collectAll(Predicates.instanceOf(NumericLiteral.class), literals);
    ExprSubstitutionMap smap = new ExprSubstitutionMap();
    for (NumericLiteral l: literals) {
      if (!l.getType().isDecimal()) continue;
      NumericLiteral castLiteral = (NumericLiteral) l.clone();
      castLiteral.explicitlyCastToFloat(targetType);
      smap.put(l, castLiteral);
    }
    return child.substitute(smap, analyzer, false);
  }

  /**
   * DECIMAL_V1:
   * ----------
   * This function applies a heuristic that casts literal child exprs of this expr from
   * decimal to floating point in certain circumstances to reduce processing cost. In
   * earlier versions of Impala's decimal support, it was much slower than floating point
   * arithmetic. The original rationale for the automatic casting follows.
   *
   * Decimal has a higher processing cost than floating point and we should not pay
   * the cost if the user does not require the accuracy. For example:
   * "select float_col + 1.1" would start out with 1.1 as a decimal(2,1) and the
   * float_col would be promoted to a high accuracy decimal. This function will identify
   * this case and treat 1.1 as a float.
   * In the case of "decimal_col + 1.1", 1.1 would remain a decimal.
   * In the case of "float_col + cast(1.1 as decimal(2,1))", the result would be a
   * decimal.
   *
   * Another way to think about it is that DecimalLiterals are analyzed as returning
   * decimals (of the narrowest precision/scale) and we later convert them to a floating
   * point type according to a heuristic that attempts to guess what the user intended.
   *
   * DECIMAL_V2:
   * ----------
   * This function does nothing. All decimal numeric literals are interpreted as decimals
   * and the normal expression typing rules apply.
   */
  protected void convertNumericLiteralsFromDecimal(Analyzer analyzer)
      throws AnalysisException {
    Preconditions.checkState(this instanceof ArithmeticExpr ||
        this instanceof BinaryPredicate);
    // This heuristic conversion is not part of DECIMAL_V2.
    if (analyzer.getQueryOptions().isDecimal_v2()) return;
    if (children_.size() == 1) return; // Do not attempt to convert for unary ops
    Preconditions.checkState(children_.size() == 2);
    Type t0 = getChild(0).getType();
    Type t1 = getChild(1).getType();
    boolean c0IsConstantDecimal = getChild(0).isConstant() && t0.isDecimal();
    boolean c1IsConstantDecimal = getChild(1).isConstant() && t1.isDecimal();
    if (c0IsConstantDecimal && c1IsConstantDecimal) return;
    if (!c0IsConstantDecimal && !c1IsConstantDecimal) return;

    // Only child(0) or child(1) is a const decimal. See if we can cast it to
    // the type of the other child.
    if (c0IsConstantDecimal && !isExplicitCastToDecimal(getChild(0))) {
      Expr c0 = convertDecimalLiteralsToFloat(analyzer, getChild(0), t1);
      setChild(0, c0);
    }
    if (c1IsConstantDecimal && !isExplicitCastToDecimal(getChild(1))) {
      Expr c1 = convertDecimalLiteralsToFloat(analyzer, getChild(1), t0);
      setChild(1, c1);
    }
  }

  /**
   * Helper function: analyze list of exprs
   */
  public static void analyze(List<? extends Expr> exprs, Analyzer analyzer)
      throws AnalysisException {
    if (exprs == null) return;
    for (Expr expr: exprs) {
      expr.analyze(analyzer);
    }
  }

  @Override
  public String toSql() {
    return (printSqlInParens_) ? "(" + toSqlImpl() + ")" : toSqlImpl();
  }

  /**
   * Returns a SQL string representing this expr. Subclasses should override this method
   * instead of toSql() to ensure that parenthesis are properly added around the toSql().
   */
  protected abstract String toSqlImpl();

  // Convert this expr, including all children, to its Thrift representation.
  public TExpr treeToThrift() {
    if (type_.isNull()) {
      // Hack to ensure BE never sees TYPE_NULL. If an expr makes it this far without
      // being cast to a non-NULL type, the type doesn't matter and we can cast it
      // arbitrarily.
      Preconditions.checkState(this instanceof NullLiteral || this instanceof SlotRef);
      return NullLiteral.create(ScalarType.BOOLEAN).treeToThrift();
    }
    TExpr result = new TExpr();
    treeToThriftHelper(result);
    return result;
  }

  // Append a flattened version of this expr, including all children, to 'container'.
  protected void treeToThriftHelper(TExpr container) {
    Preconditions.checkState(isAnalyzed_,
        "Must be analyzed before serializing to thrift. %s", this);
    Preconditions.checkState(!type_.isWildcardDecimal());
    // The BE should never see TYPE_NULL
    Preconditions.checkState(!type_.isNull(), "Expr has type null!");
    TExprNode msg = new TExprNode();
    msg.type = type_.toThrift();
    msg.is_constant = isConstant_;
    msg.num_children = children_.size();
    if (fn_ != null) {
      msg.setFn(fn_.toThrift());
      if (fn_.hasVarArgs()) msg.setVararg_start_idx(fn_.getNumArgs() - 1);
    }
    toThrift(msg);
    container.addToNodes(msg);
    for (Expr child: children_) {
      child.treeToThriftHelper(container);
    }
  }

  // Convert this expr into msg (excluding children), which requires setting
  // msg.op as well as the expr-specific field.
  protected abstract void toThrift(TExprNode msg);

  /**
   * Returns the product of the given exprs' number of distinct values or -1 if any of
   * the exprs have an invalid number of distinct values.
   */
  public static long getNumDistinctValues(List<Expr> exprs) {
    if (exprs == null || exprs.isEmpty()) return 0;
    long numDistinctValues = 1;
    for (Expr expr: exprs) {
      if (expr.getNumDistinctValues() == -1) {
        numDistinctValues = -1;
        break;
      }
      numDistinctValues *= expr.getNumDistinctValues();
    }
    return numDistinctValues;
  }

  public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
    List<TExpr> result = Lists.newArrayList();
    for (Expr expr: exprs) {
      result.add(expr.treeToThrift());
    }
    return result;
  }

  public static com.google.common.base.Predicate<Expr> isAggregatePredicate() {
    return isAggregatePredicate_;
  }

  public boolean isAggregate() {
    return isAggregatePredicate_.apply(this);
  }

  public List<String> childrenToSql() {
    List<String> result = Lists.newArrayList();
    for (Expr child: children_) {
      result.add(child.toSql());
    }
    return result;
  }

  public String debugString() {
    return (id_ != null ? "exprid=" + id_.toString() + " " : "") + debugString(children_);
  }

  public static String debugString(List<? extends Expr> exprs) {
    if (exprs == null || exprs.isEmpty()) return "";
    List<String> strings = Lists.newArrayList();
    for (Expr expr: exprs) {
      strings.add(expr.debugString());
    }
    return Joiner.on(" ").join(strings);
  }

  public static String toSql(List<? extends Expr> exprs) {
    if (exprs == null || exprs.isEmpty()) return "";
    List<String> strings = Lists.newArrayList();
    for (Expr expr: exprs) {
      strings.add(expr.toSql());
    }
    return Joiner.on(", ").join(strings);
  }

  /**
   * Returns true if two expressions are equal. The equality comparison works on analyzed
   * as well as unanalyzed exprs by ignoring implicit casts (see CastExpr.equals()).
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    // don't compare type, this could be called pre-analysis
    Expr expr = (Expr) obj;
    if (children_.size() != expr.children_.size()) return false;
    for (int i = 0; i < children_.size(); ++i) {
      if (!children_.get(i).equals(expr.children_.get(i))) return false;
    }
    if (fn_ == null && expr.fn_ == null) return true;
    if (fn_ == null || expr.fn_ == null) return false; // One null, one not
    // Both fn_'s are not null
    return fn_.equals(expr.fn_);
  }

  /**
   * Return true if l1[i].equals(l2[i]) for all i.
   */
  public static <C extends Expr> boolean equalLists(List<C> l1, List<C> l2) {
    if (l1.size() != l2.size()) return false;
    Iterator<C> l1Iter = l1.iterator();
    Iterator<C> l2Iter = l2.iterator();
    while (l1Iter.hasNext()) {
      if (!l1Iter.next().equals(l2Iter.next())) return false;
    }
    return true;
  }

  /**
   * Return true if l1 equals l2 when both lists are interpreted as sets.
   * TODO: come up with something better than O(n^2)?
   */
  public static <C extends Expr> boolean equalSets(List<C> l1, List<C> l2) {
    if (l1.size() != l2.size()) return false;
    return l1.containsAll(l2) && l2.containsAll(l1);
  }

  /**
   * Return true if l1 is a subset of l2.
   */
  public static <C extends Expr> boolean isSubset(List<C> l1, List<C> l2) {
    if (l1.size() > l2.size()) return false;
    return l2.containsAll(l1);
  }

  /**
   * Return the intersection of l1 and l2.599
   */
  public static <C extends Expr> List<C> intersect(List<C> l1, List<C> l2) {
    List<C> result = new ArrayList<C>();
    for (C element: l1) {
      if (l2.contains(element)) result.add(element);
    }
    return result;
  }

  /**
   * Compute the intersection of l1 and l2, given the smap, and
   * return the intersecting l1 elements in i1 and the intersecting l2 elements in i2.
   */
  public static void intersect(Analyzer analyzer,
      List<Expr> l1, List<Expr> l2, ExprSubstitutionMap smap,
      List<Expr> i1, List<Expr> i2) {
    i1.clear();
    i2.clear();
    List<Expr> s1List = Expr.substituteList(l1, smap, analyzer, false);
    Preconditions.checkState(s1List.size() == l1.size());
    List<Expr> s2List = Expr.substituteList(l2, smap, analyzer, false);
    Preconditions.checkState(s2List.size() == l2.size());
    for (int i = 0; i < s1List.size(); ++i) {
      Expr s1 = s1List.get(i);
      for (int j = 0; j < s2List.size(); ++j) {
        Expr s2 = s2List.get(j);
        if (s1.equals(s2)) {
          i1.add(l1.get(i));
          i2.add(l2.get(j));
          break;
        }
      }
    }
  }

  @Override
  public int hashCode() {
    if (id_ == null) {
      throw new UnsupportedOperationException("Expr.hashCode() is not implemented");
    } else {
      return id_.asInt();
    }
  }

  /**
   * Gather conjuncts from this expr and return them in a list.
   * A conjunct is an expr that returns a boolean, e.g., Predicates, function calls,
   * SlotRefs, etc. Hence, this method is placed here and not in Predicate.
   */
  public List<Expr> getConjuncts() {
    List<Expr> list = Lists.newArrayList();
    if (this instanceof CompoundPredicate
        && ((CompoundPredicate) this).getOp() == CompoundPredicate.Operator.AND) {
      // TODO: we have to convert CompoundPredicate.AND to two expr trees for
      // conjuncts because NULLs are handled differently for CompoundPredicate.AND
      // and conjunct evaluation.  This is not optimal for jitted exprs because it
      // will result in two functions instead of one. Create a new CompoundPredicate
      // Operator (i.e. CONJUNCT_AND) with the right NULL semantics and use that
      // instead
      list.addAll((getChild(0)).getConjuncts());
      list.addAll((getChild(1)).getConjuncts());
    } else {
      list.add(this);
    }
    return list;
  }

  /**
   * Returns an analyzed clone of 'this' with exprs substituted according to smap.
   * Removes implicit casts and analysis state while cloning/substituting exprs within
   * this tree, such that the returned result has minimal implicit casts and types.
   * Throws if analyzing the post-substitution expr tree failed.
   * If smap is null, this function is equivalent to clone().
   * If preserveRootType is true, the resulting expr tree will be cast if necessary to
   * the type of 'this'.
   */
  public Expr trySubstitute(ExprSubstitutionMap smap, Analyzer analyzer,
      boolean preserveRootType)
      throws AnalysisException {
    Expr result = clone();
    // Return clone to avoid removing casts.
    if (smap == null) return result;
    result = result.substituteImpl(smap, analyzer);
    result.analyze(analyzer);
    if (preserveRootType && !type_.equals(result.getType())) result = result.castTo(type_);
    return result;
  }

  /**
   * Returns an analyzed clone of 'this' with exprs substituted according to smap.
   * Removes implicit casts and analysis state while cloning/substituting exprs within
   * this tree, such that the returned result has minimal implicit casts and types.
   * Expects the analysis of the post-substitution expr to succeed.
   * If smap is null, this function is equivalent to clone().
   * If preserveRootType is true, the resulting expr tree will be cast if necessary to
   * the type of 'this'.
   */
  public Expr substitute(ExprSubstitutionMap smap, Analyzer analyzer,
      boolean preserveRootType) {
    try {
      return trySubstitute(smap, analyzer, preserveRootType);
    } catch (Exception e) {
      throw new IllegalStateException("Failed analysis after expr substitution.", e);
    }
  }

  public static ArrayList<Expr> trySubstituteList(Iterable<? extends Expr> exprs,
      ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootTypes)
          throws AnalysisException {
    if (exprs == null) return null;
    ArrayList<Expr> result = new ArrayList<Expr>();
    for (Expr e: exprs) {
      result.add(e.trySubstitute(smap, analyzer, preserveRootTypes));
    }
    return result;
  }

  public static ArrayList<Expr> substituteList(Iterable<? extends Expr> exprs,
      ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootTypes) {
    try {
      return trySubstituteList(exprs, smap, analyzer, preserveRootTypes);
    } catch (Exception e) {
      throw new IllegalStateException("Failed analysis after expr substitution.", e);
    }
  }

  /**
   * Recursive method that performs the actual substitution for try/substitute() while
   * removing implicit casts. Resets the analysis state in all non-SlotRef expressions.
   * Exprs that have non-child exprs which should be affected by substitutions must
   * override this method and apply the substitution to such exprs as well.
   */
  protected Expr substituteImpl(ExprSubstitutionMap smap, Analyzer analyzer) {
    if (isImplicitCast()) return getChild(0).substituteImpl(smap, analyzer);
    if (smap != null) {
      Expr substExpr = smap.get(this);
      if (substExpr != null) return substExpr.clone();
    }
    for (int i = 0; i < children_.size(); ++i) {
      children_.set(i, children_.get(i).substituteImpl(smap, analyzer));
    }
    // SlotRefs must remain analyzed to support substitution across query blocks. All
    // other exprs must be analyzed again after the substitution to add implicit casts
    // and for resolving their correct function signature.
    if (!(this instanceof SlotRef)) resetAnalysisState();
    return this;
  }

  /**
   * Set the expr to be analyzed and computes isConstant_.
   */
  protected void analysisDone() {
    Preconditions.checkState(!isAnalyzed_);
    // We need to compute the const-ness as the last step, since analysis may change
    // the result, e.g. by resolving function.
    isConstant_ = isConstantImpl();
    isAnalyzed_ = true;
  }

  /**
   * Resets the internal state of this expr produced by analyze().
   * Only modifies this expr, and not its child exprs.
   */
  protected void resetAnalysisState() { isAnalyzed_ = false; }

  /**
   * Resets the internal analysis state of this expr tree. Removes implicit casts.
   */
  public Expr reset() {
    if (isImplicitCast()) return getChild(0).reset();
    for (int i = 0; i < children_.size(); ++i) {
      children_.set(i, children_.get(i).reset());
    }
    resetAnalysisState();
    return this;
  }

  public static ArrayList<Expr> resetList(ArrayList<Expr> l) {
    for (int i = 0; i < l.size(); ++i) {
      l.set(i, l.get(i).reset());
    }
    return l;
  }

  /**
   * Creates a deep copy of this expr including its analysis state. The method is
   * abstract in this class to force new Exprs to implement it.
   */
  @Override
  public abstract Expr clone();

  /**
   * Create a deep copy of 'l'. The elements of the returned list are of the same
   * type as the input list.
   */
  public static <C extends Expr> ArrayList<C> cloneList(List<C> l) {
    Preconditions.checkNotNull(l);
    ArrayList<C> result = new ArrayList<C>(l.size());
    for (Expr element: l) {
      result.add((C) element.clone());
    }
    return result;
  }

  /**
   * Removes duplicate exprs (according to equals()).
   */
  public static <C extends Expr> void removeDuplicates(List<C> l) {
    if (l == null) return;
    List<C> origList = Lists.newArrayList(l);
    l.clear();
    for (C expr: origList) if (!l.contains(expr)) l.add(expr);
  }

  /**
   * Removes constant exprs
   */
  public static <C extends Expr> void removeConstants(List<C> l) {
    if (l == null) return;
    ListIterator<C> it = l.listIterator();
    while (it.hasNext()) {
      C e = it.next();
      if (e.isConstant()) it.remove();
    }
  }

  // Arbitrary max exprs considered for constant propagation due to O(n^2) complexity.
  private final static int CONST_PROPAGATION_EXPR_LIMIT = 200;

  /**
   * Propagates constant expressions of the form <slot ref> = <constant> to
   * other uses of slot ref in the given conjuncts; returns a BitSet with
   * bits set to true in all changed indices.  Only one round of substitution
   * is performed.  The candidates BitSet is used to determine which members of
   * conjuncts are considered for propagation.
   */
  private static BitSet propagateConstants(List<Expr> conjuncts, BitSet candidates,
      Analyzer analyzer) {
    Preconditions.checkState(conjuncts.size() <= candidates.size());
    BitSet changed = new BitSet(conjuncts.size());
    for (int i = candidates.nextSetBit(0); i >= 0; i = candidates.nextSetBit(i+1)) {
      if (!(conjuncts.get(i) instanceof BinaryPredicate)) continue;
      BinaryPredicate bp = (BinaryPredicate) conjuncts.get(i);
      if (bp.getOp() != BinaryPredicate.Operator.EQ) continue;
      SlotRef slotRef = bp.getBoundSlot();
      if (slotRef == null || !bp.getChild(1).isConstant()) continue;
      Expr subst = bp.getSlotBinding(slotRef.getSlotId());
      ExprSubstitutionMap smap = new ExprSubstitutionMap();
      smap.put(slotRef, subst);
      for (int j = 0; j < conjuncts.size(); ++j) {
        // Don't rewrite with our own substitution!
        if (j == i) continue;
        Expr toRewrite = conjuncts.get(j);
        Expr rewritten = toRewrite.substitute(smap, analyzer, true);
        if (!rewritten.equals(toRewrite)) {
          conjuncts.set(j, rewritten);
          changed.set(j, true);
        }
      }
    }
    return changed;
  }

  /*
   * Propagates constants, performs expr rewriting and removes duplicates.
   * Returns false if a contradiction has been implied, true otherwise.
   * Catches and logs, but ignores any exceptions thrown during rewrite, which
   * will leave conjuncts intact and rewritten as far as possible until the
   * exception.
   */
  public static boolean optimizeConjuncts(List<Expr> conjuncts, Analyzer analyzer) {
    Preconditions.checkNotNull(conjuncts);
    try {
      BitSet candidates = new BitSet(conjuncts.size());
      candidates.set(0, Math.min(conjuncts.size(), CONST_PROPAGATION_EXPR_LIMIT));
      int transfers = 0;

      // Constant propagation may make other slots constant, so repeat the process
      // until there are no more changes.
      while (!candidates.isEmpty()) {
        BitSet changed = propagateConstants(conjuncts, candidates, analyzer);
        candidates.clear();
        int pruned = 0;
        for (int i = changed.nextSetBit(0); i >= 0; i = changed.nextSetBit(i+1)) {
          // When propagating constants, we may de-normalize expressions, so we
          // must normalize binary predicates.  Any additional rules will be
          // applied by the rewriter.
          int index = i - pruned;
          Preconditions.checkState(index >= 0);
          ExprRewriter rewriter = analyzer.getExprRewriter();
          Expr rewritten = rewriter.rewrite(conjuncts.get(index), analyzer);
          // Re-analyze to add implicit casts and update cost
          rewritten.reset();
          rewritten.analyze(analyzer);
          if (!rewritten.isConstant()) {
            conjuncts.set(index, rewritten);
            if (++transfers < CONST_PROPAGATION_EXPR_LIMIT) candidates.set(index, true);
            continue;
          }
          // Remove constant boolean literal expressions.  N.B. - we may have
          // expressions determined to be constant which can not yet be discarded
          // because they can't be evaluated if expr rewriting is turned off.
          if (rewritten instanceof NullLiteral ||
              Expr.IS_FALSE_LITERAL.apply(rewritten)) {
            conjuncts.clear();
            conjuncts.add(rewritten);
            return false;
          }
          if (Expr.IS_TRUE_LITERAL.apply(rewritten)) {
            pruned++;
            conjuncts.remove(index);
          }
        }
      }
    } catch (AnalysisException e) {
      LOG.warn("Not able to analyze after rewrite: " + e.toString() + " conjuncts: " +
          Expr.debugString(conjuncts));
    }
    Expr.removeDuplicates(conjuncts);
    return true;
  }

  /**
   * Returns true if expr is fully bound by tid, otherwise false.
   */
  public boolean isBound(TupleId tid) {
    return isBoundByTupleIds(Lists.newArrayList(tid));
  }

  /**
   * Returns true if expr is fully bound by tids, otherwise false.
   */
  public boolean isBoundByTupleIds(List<TupleId> tids) {
    for (Expr child: children_) {
      if (!child.isBoundByTupleIds(tids)) return false;
    }
    return true;
  }

  /**
   * Returns true if expr is fully bound by slotIds, otherwise false.
   */
  public boolean isBoundBySlotIds(List<SlotId> slotIds) {
    for (Expr child: children_) {
      if (!child.isBoundBySlotIds(slotIds)) return false;
    }
    return true;
  }

  public static Expr getFirstBoundChild(Expr expr, List<TupleId> tids) {
    for (Expr child: expr.getChildren()) {
      if (child.isBoundByTupleIds(tids)) return child;
    }
    return null;
  }

  public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
    Set<TupleId> tupleIdSet = Sets.newHashSet();
    Set<SlotId> slotIdSet = Sets.newHashSet();
    getIdsHelper(tupleIdSet, slotIdSet);
    if (tupleIds != null) tupleIds.addAll(tupleIdSet);
    if (slotIds != null) slotIds.addAll(slotIdSet);
  }

  protected void getIdsHelper(Set<TupleId> tupleIds, Set<SlotId> slotIds) {
    for (Expr child: children_) {
      child.getIdsHelper(tupleIds, slotIds);
    }
  }

  public static <C extends Expr> void getIds(List<? extends Expr> exprs,
      List<TupleId> tupleIds, List<SlotId> slotIds) {
    if (exprs == null) return;
    for (Expr e: exprs) {
      e.getIds(tupleIds, slotIds);
    }
  }

  /**
   * @return true if this is an instance of LiteralExpr
   */
  public boolean isLiteral() {
    return this instanceof LiteralExpr;
  }

  /**
   * Returns true if this expression should be treated as constant. I.e. if the frontend
   * and backend should assume that two evaluations of the expression within a query will
   * return the same value. Examples of constant expressions include:
   * - Literal values like 1, "foo", or NULL
   * - Deterministic operators applied to constant arguments, e.g. 1 + 2, or
   *   concat("foo", "bar")
   * - Functions that should be always return the same value within a query but may
   *   return different values for different queries. E.g. now(), which we want to
   *   evaluate only once during planning.
   * May incorrectly return true if the expression is not analyzed.
   * TODO: isAnalyzed_ should be a precondition for isConstant(), since it is not always
   * possible to correctly determine const-ness before analysis (e.g. see
   * FunctionCallExpr.isConstant()).
   */
  public final boolean isConstant() {
    if (isAnalyzed_) return isConstant_;
    return isConstantImpl();
  }

  /**
   * Implements isConstant() - computes the value without using 'isConstant_'.
   */
  protected boolean isConstantImpl() {
    for (Expr expr : children_) {
      if (!expr.isConstant()) return false;
    }
    return true;
  }

  /**
   * @return true if this expr is either a null literal or a cast from
   * a null literal.
   */
  public boolean isNullLiteral() {
    if (this instanceof NullLiteral) return true;
    if (!(this instanceof CastExpr)) return false;
    Preconditions.checkState(children_.size() == 1);
    return children_.get(0).isNullLiteral();
  }

  /**
   * Return true if this expr is a scalar subquery.
   */
  public boolean isScalarSubquery() {
    Preconditions.checkState(isAnalyzed_);
    return this instanceof Subquery && getType().isScalarType();
  }

  /**
   * Checks whether this expr returns a boolean type or NULL type.
   * If not, throws an AnalysisException with an appropriate error message using
   * 'name' as a prefix. For example, 'name' could be "WHERE clause".
   * The error message only contains this.toSql() if printExpr is true.
   */
  public void checkReturnsBool(String name, boolean printExpr) throws AnalysisException {
    if (!type_.isBoolean() && !type_.isNull()) {
      throw new AnalysisException(
          String.format("%s%s requires return type 'BOOLEAN'. " +
              "Actual type is '%s'.", name, (printExpr) ? " '" + toSql() + "'" : "",
              type_.toString()));
    }
  }

  /**
   * Casts this expr to a specific target type. It checks the validity of the cast and
   * calls uncheckedCastTo().
   * @param targetType
   *          type to be cast to
   * @return cast expression, or converted literal,
   *         should never return null
   * @throws AnalysisException
   *           when an invalid cast is asked for, for example,
   *           failure to convert a string literal to a date literal
   */
  public final Expr castTo(Type targetType) throws AnalysisException {
    Type type = Type.getAssignmentCompatibleType(this.type_, targetType, false);
    Preconditions.checkState(type.isValid(), "cast %s to %s", this.type_, targetType);
    // If the targetType is NULL_TYPE then ignore the cast because NULL_TYPE
    // is compatible with all types and no cast is necessary.
    if (targetType.isNull()) return this;
    if (!targetType.isDecimal()) {
      // requested cast must be to assignment-compatible type
      // (which implies no loss of precision)
      Preconditions.checkArgument(targetType.equals(type),
          "targetType=" + targetType + " type=" + type);
    }
    return uncheckedCastTo(targetType);
  }

  /**
   * Create an expression equivalent to 'this' but returning targetType;
   * possibly by inserting an implicit cast,
   * or by returning an altogether new expression
   * or by returning 'this' with a modified return type'.
   * @param targetType
   *          type to be cast to
   * @return cast expression, or converted literal,
   *         should never return null
   * @throws AnalysisException
   *           when an invalid cast is asked for, for example,
   *           failure to convert a string literal to a date literal
   */
  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    return new CastExpr(targetType, this);
  }

  /**
   * Add a cast expression above child.
   * If child is a literal expression, we attempt to
   * convert the value of the child directly, and not insert a cast node.
   * @param targetType
   *          type to be cast to
   * @param childIndex
   *          index of child to be cast
   */
  public void castChild(Type targetType, int childIndex) throws AnalysisException {
    Expr child = getChild(childIndex);
    Expr newChild = child.castTo(targetType);
    setChild(childIndex, newChild);
  }


  /**
   * Convert child to to targetType, possibly by inserting an implicit cast, or by
   * returning an altogether new expression, or by returning 'this' with a modified
   * return type'.
   * @param targetType
   *          type to be cast to
   * @param childIndex
   *          index of child to be cast
   */
  protected void uncheckedCastChild(Type targetType, int childIndex)
      throws AnalysisException {
    Expr child = getChild(childIndex);
    Expr newChild = child.uncheckedCastTo(targetType);
    setChild(childIndex, newChild);
  }

  /**
   * Returns child expr if this expr is an implicit cast, otherwise returns 'this'.
   */
  public Expr ignoreImplicitCast() {
    if (isImplicitCast()) return getChild(0).ignoreImplicitCast();
    return this;
  }

  /**
   * Returns true if 'this' is an implicit cast expr.
   */
  public boolean isImplicitCast() {
    return this instanceof CastExpr && ((CastExpr) this).isImplicit();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
        .add("id", id_)
        .add("type", type_)
        .add("sel", selectivity_)
        .add("evalCost", evalCost_)
        .add("#distinct", numDistinctValues_)
        .toString();
  }

  /**
   * If 'this' is a SlotRef or a Cast that wraps a SlotRef, returns that SlotRef.
   * Otherwise returns null.
   */
  public SlotRef unwrapSlotRef(boolean implicitOnly) {
    Expr unwrappedExpr = unwrapExpr(implicitOnly);
    if (unwrappedExpr instanceof SlotRef) return (SlotRef) unwrappedExpr;
    return null;
  }

  /**
   * Returns the first child if this Expr is a CastExpr. Otherwise, returns 'this'.
   */
  public Expr unwrapExpr(boolean implicitOnly) {
    if (this instanceof CastExpr
        && (!implicitOnly || ((CastExpr) this).isImplicit())) {
      return children_.get(0);
    }
    return this;
  }

  /**
   * Returns the descriptor of the scan slot that directly or indirectly produces
   * the values of 'this' SlotRef. Traverses the source exprs of intermediate slot
   * descriptors to resolve materialization points (e.g., aggregations).
   * Returns null if 'e' or any source expr of 'e' is not a SlotRef or cast SlotRef.
   */
  public SlotDescriptor findSrcScanSlot() {
    SlotRef slotRef = unwrapSlotRef(false);
    if (slotRef == null) return null;
    SlotDescriptor slotDesc = slotRef.getDesc();
    if (slotDesc.isScanSlot()) return slotDesc;
    if (slotDesc.getSourceExprs().size() == 1) {
      return slotDesc.getSourceExprs().get(0).findSrcScanSlot();
    }
    // No known source expr, or there are several source exprs meaning the slot is
    // has no single source table.
    return null;
  }

  /**
   * Pushes negation to the individual operands of a predicate
   * tree rooted at 'root'.
   */
  public static Expr pushNegationToOperands(Expr root) {
    Preconditions.checkNotNull(root);
    if (Expr.IS_NOT_PREDICATE.apply(root)) {
      try {
        // Make sure we call function 'negate' only on classes that support it,
        // otherwise we may recurse infinitely.
        Method m = root.getChild(0).getClass().getDeclaredMethod(NEGATE_FN);
        return pushNegationToOperands(root.getChild(0).negate());
      } catch (NoSuchMethodException e) {
        // The 'negate' function is not implemented. Break the recursion.
        return root;
      }
    }

    if (root instanceof CompoundPredicate) {
      Expr left = pushNegationToOperands(root.getChild(0));
      Expr right = pushNegationToOperands(root.getChild(1));
      CompoundPredicate compoundPredicate =
        new CompoundPredicate(((CompoundPredicate)root).getOp(), left, right);
      compoundPredicate.setPrintSqlInParens(root.getPrintSqlInParens());
      return compoundPredicate;
    }

    return root;
  }

  /**
   * Negates a boolean Expr.
   */
  public Expr negate() {
    Preconditions.checkState(type_.getPrimitiveType() == PrimitiveType.BOOLEAN);
    return new CompoundPredicate(CompoundPredicate.Operator.NOT, this, null);
  }

  /**
   * Returns the subquery of an expr. Returns null if this expr does not contain
   * a subquery.
   *
   * TODO: Support predicates with more that one subqueries when we implement
   * the independent subquery evaluation.
   */
  public Subquery getSubquery() {
    if (!contains(Subquery.class)) return null;
    List<Subquery> subqueries = Lists.newArrayList();
    collect(Subquery.class, subqueries);
    Preconditions.checkState(subqueries.size() == 1);
    return subqueries.get(0);
  }

  /**
   * Returns true iff all of this Expr's children have their costs set.
   */
  protected boolean hasChildCosts() {
    for (Expr child : children_) {
      if (!child.hasCost()) return false;
    }
    return true;
  }

  /**
   * Computes and returns the sum of the costs of all of this Expr's children.
   */
  protected float getChildCosts() {
    float cost = 0;
    for (Expr child : children_) cost += child.getCost();
    return cost;
  }

  /**
   * Returns the average length of the values produced by an Expr
   * of type string. Returns a default for unknown lengths.
   */
  protected static double getAvgStringLength(Expr e) {
    Preconditions.checkState(e.getType().isStringType());
    Preconditions.checkState(e.isAnalyzed_);

    SlotRef ref = e.unwrapSlotRef(false);
    if (ref != null) {
      if (ref.getDesc() != null && ref.getDesc().getStats().getAvgSize() > 0) {
        return ref.getDesc().getStats().getAvgSize();
      } else {
        return DEFAULT_AVG_STRING_LENGTH;
      }
    } else if (e instanceof StringLiteral) {
      return ((StringLiteral) e).getValue().length();
    } else {
      // TODO(tmarshall): Extend this to support other string Exprs, such as
      // function calls that return string.
      return DEFAULT_AVG_STRING_LENGTH;
    }
  }

  /**
   * Generates a comma-separated string from the toSql() string representations of
   * 'exprs'.
   */
  public static String listToSql(List<Expr> exprs) {
    com.google.common.base.Function<Expr, String> toSql =
        new com.google.common.base.Function<Expr, String>() {
        @Override
        public String apply(Expr arg) {
          return arg.toSql();
        }
    };
    return Joiner.on(",").join(Iterables.transform(exprs, toSql));
  }
}
