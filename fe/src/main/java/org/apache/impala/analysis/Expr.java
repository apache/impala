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

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.impala.analysis.BinaryPredicate.Operator;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Function.CompareMode;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.SqlCastException;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.common.TreeNode;
import org.apache.impala.rewrite.ExprRewriter;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TExpr;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.util.MathUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Root of the expr node hierarchy.
 */
abstract public class Expr extends TreeNode<Expr> implements ParseNode, Cloneable {
  private static final Logger LOG = LoggerFactory.getLogger(Expr.class);

  // Limits on the number of expr children and the depth of an expr tree. These maximum
  // values guard against crashes due to stack overflows (IMPALA-432) and were
  // experimentally determined to be safe.
  public static final int EXPR_CHILDREN_LIMIT = 10000;
  // The expr depth limit is mostly due to our recursive implementation of clone().
  public static final int EXPR_DEPTH_LIMIT = 1000;

  // Name of the function that needs to be implemented by every Expr that
  // supports negation.
  private static final String NEGATE_FN = "negate";

  // To be used where we cannot come up with a better estimate (selectivity_ is -1).
  public static final double DEFAULT_SELECTIVITY = 0.1;

  // The relative costs of different Exprs. These numbers are not intended as a precise
  // reflection of running times, but as simple heuristics for ordering Exprs from cheap
  // to expensive.
  // TODO(tmwarshall): Get these costs in a more principled way, eg. with a benchmark.
  public static final float ARITHMETIC_OP_COST = 1;
  public static final float BINARY_PREDICATE_COST = 1;
  public static final float VAR_LEN_BINARY_PREDICATE_COST = 5;
  public static final float COMPOUND_PREDICATE_COST = 1;
  public static final float FUNCTION_CALL_COST = 10;
  public static final float IS_NOT_EMPTY_COST = 1;
  public static final float IS_NULL_COST = 1;
  public static final float LIKE_COST = 10;
  public static final float LITERAL_COST = 1;
  public static final float SLOT_REF_COST = 1;
  public static final float TIMESTAMP_ARITHMETIC_COST = 5;
  public static final float UNKNOWN_COST = -1;

  // Arbitrary max exprs considered for constant propagation due to O(n^2) complexity.
  private static final int CONST_PROPAGATION_EXPR_LIMIT = 200;

  // To be used when estimating the cost of Exprs of type string where we don't otherwise
  // have an estimate of how long the strings produced by that Expr are.
  public static final int DEFAULT_AVG_STRING_LENGTH = 5;

  // returns true if an Expr is a non-analytic aggregate.
  public static final com.google.common.base.Predicate<Expr> IS_AGGREGATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr &&
              ((FunctionCallExpr)arg).isAggregateFunction();
        }
      };

  // Returns true if an Expr is a NOT CompoundPredicate.
  public static final com.google.common.base.Predicate<Expr> IS_NOT_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof CompoundPredicate &&
              ((CompoundPredicate)arg).getOp() == CompoundPredicate.Operator.NOT;
        }
      };

  // Returns true if an Expr is an OR CompoundPredicate.
  public static final com.google.common.base.Predicate<Expr> IS_OR_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof CompoundPredicate &&
              ((CompoundPredicate)arg).getOp() == CompoundPredicate.Operator.OR;
        }
      };

  // Returns true if an Expr is a scalar subquery
  public static final com.google.common.base.Predicate<Expr> IS_SCALAR_SUBQUERY =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg.isScalarSubquery();
        }
      };

  // Returns true if an Expr has a subquery as a direct child.
  public static final com.google.common.base.Predicate<Expr> HAS_SUBQUERY_CHILD =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          for (Expr child : arg.getChildren()) {
            if (child instanceof Subquery) return true;
          }
          return false;
        }
      };

  // Returns true if an Expr is an aggregate function that returns non-null on
  // an empty set (e.g. count).
  public static final com.google.common.base.Predicate<Expr>
      NON_NULL_EMPTY_AGG = new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr &&
              ((FunctionCallExpr)arg).returnsNonNullOnEmpty();
        }
      };

  // Returns true if an Expr is a builtin aggregate function.
  public static final com.google.common.base.Predicate<Expr> IS_BUILTIN_AGG_FN =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr &&
              ((FunctionCallExpr)arg).getFnName().isBuiltin();
        }
      };

  // Returns true if an Expr is a user-defined aggregate function.
  public static final com.google.common.base.Predicate<Expr> IS_UDA_FN =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return IS_AGGREGATE.apply(arg) &&
              !((FunctionCallExpr)arg).getFnName().isBuiltin();
        }
      };

  public static final com.google.common.base.Predicate<Expr> IS_TRUE_LITERAL =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof BoolLiteral && ((BoolLiteral)arg).getValue();
        }
      };

  public static final com.google.common.base.Predicate<Expr> IS_FALSE_LITERAL =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof BoolLiteral && !((BoolLiteral)arg).getValue();
        }
      };

  public static final com.google.common.base.Predicate<Expr> IS_EQ_BINARY_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) { return BinaryPredicate.getEqSlots(arg) != null; }
      };

  public static final com.google.common.base.Predicate<Expr> IS_NOT_EQ_BINARY_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof BinaryPredicate
              && ((BinaryPredicate) arg).getOp() != Operator.EQ
              && ((BinaryPredicate) arg).getOp() != Operator.NOT_DISTINCT;
        }
      };

  public static final com.google.common.base.Predicate<Expr> IS_BINARY_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) { return arg instanceof BinaryPredicate; }
      };

  public static final com.google.common.base.Predicate<Expr>
    IS_EXPR_EQ_LITERAL_PREDICATE = new com.google.common.base.Predicate<Expr>() {
    @Override
    public boolean apply(Expr arg) {
      return arg instanceof BinaryPredicate
          && ((BinaryPredicate) arg).getOp() == Operator.EQ
          && IS_LITERAL.apply(((BinaryPredicate) arg).getChild(1));
    }
  };

  public static final com.google.common.base.Predicate<Expr>
      IS_NONDETERMINISTIC_BUILTIN_FN_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr
              && ((FunctionCallExpr) arg).isNondeterministicBuiltinFn();
        }
      };

  public static final com.google.common.base.Predicate<Expr> IS_UDF_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr
              && !((FunctionCallExpr) arg).getFnName().isBuiltin();
        }
      };

  /**
   * @return true if the expression is a literal.
   */
  public static final com.google.common.base.Predicate<Expr> IS_LITERAL =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        return arg instanceof LiteralExpr;
      }
    };

  /**
   * @return true if the expression is a null literal.
   */
  public static final com.google.common.base.Predicate<Expr> IS_NULL_LITERAL =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        return arg instanceof NullLiteral;
      }
    };

  /**
   * @return true if the expression is a literal value other than NULL.
   */
  public static final com.google.common.base.Predicate<Expr> IS_NON_NULL_LITERAL =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        return IS_LITERAL.apply(arg) && !IS_NULL_LITERAL.apply(arg);
      }
    };

  /**
   * @return true if the expression is a null literal, or a
   * cast of a null (as created by the ConstantFoldingRule.)
   */
  public static final com.google.common.base.Predicate<Expr> IS_NULL_VALUE =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        if (arg instanceof NullLiteral) return true;
        if (! (arg instanceof CastExpr)) return false;
        return IS_NULL_VALUE.apply(((CastExpr) arg).getChild(0));
      }
    };

  /**
   * @return true if the expression is a  literal, or a
   * cast of a null (as created by the ConstantFoldingRule.)
   */
  public static final com.google.common.base.Predicate<Expr> IS_LITERAL_VALUE =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        return IS_LITERAL.apply(arg) || IS_NULL_VALUE.apply(arg);
      }
    };

  public static final com.google.common.base.Predicate<Expr> IS_INT_LITERAL =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        return IS_LITERAL.apply(arg) && arg.getType().isIntegerType();
      }
    };

  public static final com.google.common.base.Predicate<Expr>
    IS_CONDITIONAL_BUILTIN_FN_PREDICATE =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        return arg instanceof FunctionCallExpr
            && ((FunctionCallExpr) arg).isConditionalBuiltinFn();
      }
    };

  public static final com.google.common.base.Predicate<Expr> IS_IS_NULL_PREDICATE =
    new com.google.common.base.Predicate<Expr>() {
      @Override
      public boolean apply(Expr arg) {
        return arg instanceof IsNullPredicate
            && !((IsNullPredicate) arg).isNotNull();
      }
    };

  public static final com.google.common.base.Predicate<Expr>
    IS_DISTINCT_FROM_OR_NOT_DISTINCT_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof BinaryPredicate
              && (((BinaryPredicate) arg).getOp() == Operator.DISTINCT_FROM
              || ((BinaryPredicate) arg).getOp() == Operator.NOT_DISTINCT);
        }
      };

  public static final com.google.common.base.Predicate<Expr> IS_CASE_EXPR_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof CaseExpr;
        }
      };

  public static final com.google.common.base.Predicate<Expr> IS_ALWAYS_TRUE_PREDICATE =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof Predicate
              && ((Predicate) arg).hasAlwaysTrueHint();
        }
      };

  // Returns true if an Expr is a builtin sleep function.
  public static final com.google.common.base.Predicate<Expr> IS_FN_SLEEP =
      new com.google.common.base.Predicate<Expr>() {
        @Override
        public boolean apply(Expr arg) {
          return arg instanceof FunctionCallExpr
              && ((FunctionCallExpr) arg).getFnName().isBuiltin()
              && ((FunctionCallExpr) arg).getFnName().getFunction() != null
              && ((FunctionCallExpr) arg).getFnName().getFunction().equals("sleep");
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
  private boolean isRewritten_ = false;


  // True if this has already been counted towards the number of statement expressions
  private boolean isCountedForNumStmtExprs_ = false;

  // For exprs of type Predicate, this keeps track of predicate hints
  protected List<PlanHint> predicateHints_;

  // Is codegen disabled for this expression ?
  private boolean isCodegenDisabled_ = false;

  protected Expr() {
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
    isRewritten_ = other.isRewritten_;
    isOnClauseConjunct_ = other.isOnClauseConjunct_;
    printSqlInParens_ = other.printSqlInParens_;
    selectivity_ = other.selectivity_;
    evalCost_ = other.evalCost_;
    numDistinctValues_ = other.numDistinctValues_;
    isConstant_ = other.isConstant_;
    fn_ = other.fn_;
    isCountedForNumStmtExprs_ = other.isCountedForNumStmtExprs_;
    children_ = Expr.cloneList(other.children_);
    if (other.predicateHints_ != null) {
      predicateHints_ = new ArrayList<>();
      predicateHints_.addAll(other.predicateHints_);
    }
    isCodegenDisabled_ = other.isCodegenDisabled_;
  }

  public boolean isAnalyzed() { return isAnalyzed_; }
  public boolean isRewritten() { return isRewritten_; }
  public void setRewritten(boolean isRewritten) { isRewritten_ = isRewritten; }
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
  public void disableCodegen() {
    isCodegenDisabled_ = true;
    for (Expr child : children_) {
      child.disableCodegen();
    }
  }

  /**
   * Perform semantic analysis of node and all of its children.
   * Throws exception if any errors found.
   * @see ParseNode#analyze(Analyzer)
   */
  // TODO: Analyze for expressions should return a possibly-rewritten
  // expression, leaving the StmtNode version to analyze statements
  // in-place.
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
      incrementNumStmtExprs(analyzer);
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

  protected void analyzeHints(Analyzer analyzer) throws AnalysisException {
    if (predicateHints_ != null && !predicateHints_.isEmpty()) {
      if (!(this instanceof Predicate)) {
        throw new AnalysisException("Expr hints are only supported for predicates");
      }
      for (PlanHint hint : predicateHints_) {
        if (hint.is("ALWAYS_TRUE")) {
          ((Predicate) this).setHasAlwaysTrueHint(true);
          // If the top level expr has always_true hint, its conjuncts must
          // also have the same hint (note that there's a TODO in the parser
          // grammar to allow hints on a per expr basis).
          List<Expr> conjuncts = getConjuncts();
          if (conjuncts.size() > 1) {
            for (Expr e : conjuncts) ((Predicate) e).setHasAlwaysTrueHint(true);
          }
          analyzer.setHasPlanHints();
        } else {
          analyzer.addWarning("Predicate hint not recognized: " + hint);
        }
      }
    }
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
   * Helper function to properly count the number of statement expressions.
   * If this expression has not been counted already and this is not a WITH clause,
   * increment the number of statement expressions. This function guarantees that an
   * expression will be counted at most once.
   */
  private void incrementNumStmtExprs(Analyzer analyzer) {
    // WITH clauses use a separate Analyzer with its own GlobalState. Skip counting
    // this expression towards that GlobalState. If the view defined by the WITH
    // clause is referenced, it will be counted during that analysis.
    if (analyzer.hasWithClause()) return;
    // If the expression is already counted, do not count it again. This is important
    // for expressions that can be cloned (e.g. when doing Expr::trySubstitute()).
    if (isCountedForNumStmtExprs_) return;
    analyzer.incrementNumStmtExprs();
    isCountedForNumStmtExprs_ = true;
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
    FunctionName fnName = new FunctionName(BuiltinsDb.NAME, name);
    Function searchDesc = new Function(fnName, argTypes, Type.INVALID, false);
    return analyzer.getCatalog().getFunction(searchDesc, mode);
  }

  /**
   * Generates the necessary casts for the children of this expr to call fn_.
   * child(0) is cast to the function's first argument, child(1) to the second etc.
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
   *
   * 'compatibility' defines the mode of wildcard type resolution;
   * if TypeCompatibility.isStrictDecimal() is true, it only considers casts that result
   * in no loss of information, if 'compatibility' is DEFAULT it does not guarantee
   * decimal cast without information loss.
   */
  protected void castForFunctionCall(boolean ignoreWildcardDecimals,
      TypeCompatibility compatibility) throws AnalysisException {
    Preconditions.checkState(fn_ != null);
    Type[] fnArgs = fn_.getArgs();
    Type resolvedWildcardType = getResolvedWildCardType(compatibility);
    if (resolvedWildcardType != null) {
      if (resolvedWildcardType.isNull()) {
        throw new SqlCastException(String.format(
            "Cannot resolve DECIMAL precision and scale from NULL type in %s function.",
            fn_.getFunctionName().getFunction()));
      }
      if (resolvedWildcardType.isInvalid() && !ignoreWildcardDecimals) {
        StringBuilder argTypes = new StringBuilder();
        for (int j = 0; j < children_.size(); ++j) {
          if (argTypes.length() > 0) argTypes.append(", ");
          Type childType = children_.get(j).type_;
          argTypes.append(childType.toSql());
        }
        throw new SqlCastException(String.format(
            "Cannot resolve DECIMAL types of the %s(%s) function arguments. You need " +
            "to wrap the arguments in a CAST.", fn_.getFunctionName().getFunction(),
            argTypes.toString()));
      }
    }
    for (int i = 0; i < children_.size(); ++i) {
      // For varargs, we must compare with the last type in fnArgs.argTypes.
      int ix = Math.min(fnArgs.length - 1, i);
      if (fnArgs[ix].isWildcardDecimal()) {
        if (children_.get(i).type_.isDecimal() && ignoreWildcardDecimals) continue;
        if (children_.get(i).type_.isDecimal() || !ignoreWildcardDecimals) {
          Preconditions.checkState(resolvedWildcardType != null);
          Preconditions.checkState(!resolvedWildcardType.isInvalid());
          if (!children_.get(i).type_.equals(resolvedWildcardType)) {
            castChild(resolvedWildcardType, i);
          }
        } else if (children_.get(i).type_.isNull()) {
          castChild(ScalarType.createDecimalType(), i);
        } else {
          Preconditions.checkState(children_.get(i).type_.isScalarType());
          // It is safe to assign an arbitrary decimal here only if the backend function
          // can handle it (in which case ignoreWildcardDecimals is true).
          Preconditions.checkState(ignoreWildcardDecimals);
          castChild(((ScalarType) children_.get(i).type_).getMinResolutionDecimal(), i);
        }
      } else if (!children_.get(i).type_.matchesType(fnArgs[ix])) {
        castChild(fnArgs[ix], i);
      }
    }
  }

  /**
   * Returns the max resolution type of all the wild card decimal types.
   * Returns null if there are no wild card types. If compatibility.isStrictDecimal() is
   * true, it will return an invalid type if it is not possible to come up with a decimal
   * type that is guaranteed to not lose information.
   */
  Type getResolvedWildCardType(TypeCompatibility compatibility) {
    Preconditions.checkState(compatibility.equals(TypeCompatibility.DEFAULT)
        || compatibility.equals(TypeCompatibility.STRICT_DECIMAL));
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
      if (result == null) {
        ScalarType decimalType = (ScalarType) childType;
        result = decimalType.getMinResolutionDecimal();
      } else {
        result = Type.getAssignmentCompatibleType(result, childType, compatibility);
      }
    }
    if (result != null && !result.isNull()) {
      result = ((ScalarType)result).getMinResolutionDecimal();
      Preconditions.checkState(result.isDecimal() || result.isInvalid());
      Preconditions.checkState(!result.isWildcardDecimal());
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
    List<NumericLiteral> literals = new ArrayList<>();
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
    if (getChildCount() == 1) return; // Do not attempt to convert for unary ops
    Preconditions.checkState(getChildCount() == 2);
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
  public final String toSql() {
    return toSql(DEFAULT);
  }

  /**
   * Some expression nodes are also statement-like and know about
   * before/after rewrite expressions.
   */
  @Override
  public String toSql(ToSqlOptions options) {
    return (printSqlInParens_) ? "(" + toSqlImpl(options) + ")" : toSqlImpl(options);
  }

  /**
   * Returns a SQL string representing this expr. Subclasses should override this method
   * instead of toSql() to ensure that parenthesis are properly added around the toSql().
   */
  protected abstract String toSqlImpl(ToSqlOptions options);

  protected String toSqlImpl() { return toSqlImpl(DEFAULT); };

  // For locations that don't need to handle tuple caching yet, keep the
  // old signature with a default ThriftSerializationCtx.
  public TExpr treeToThrift() {
    return treeToThrift(new ThriftSerializationCtx());
  }

  // Convert this expr, including all children, to its Thrift representation.
  public TExpr treeToThrift(ThriftSerializationCtx serialCtx) {
    if (type_.isNull()) {
      // Hack to ensure BE never sees TYPE_NULL. If an expr makes it this far without
      // being cast to a non-NULL type, the type doesn't matter and we can cast it
      // arbitrarily.
      Preconditions.checkState(IS_NULL_LITERAL.apply(this) ||
          this instanceof SlotRef);
      return NullLiteral.create(ScalarType.BOOLEAN).treeToThrift(serialCtx);
    }
    TExpr result = new TExpr();
    treeToThriftHelper(result, serialCtx);
    return result;
  }

  // Append a flattened version of this expr, including all children, to 'container'.
  protected void treeToThriftHelper(TExpr container, ThriftSerializationCtx serialCtx) {
    Preconditions.checkState(isAnalyzed_,
        "Must be analyzed before serializing to thrift. %s", this);
    Preconditions.checkState(!type_.isWildcardDecimal());
    // The BE should never see TYPE_NULL
    Preconditions.checkState(!type_.isNull(), "Expr has type null!");
    TExprNode msg = new TExprNode();
    msg.type = type_.toThrift();
    msg.is_constant = isConstant_;
    msg.num_children = children_.size();
    msg.setIs_codegen_disabled(isCodegenDisabled_);
    if (fn_ != null) {
      TFunction thriftFn = fn_.toThrift();
      thriftFn.setLast_modified_time(fn_.getLastModifiedTime());
      msg.setFn(thriftFn);
      if (fn_.hasVarArgs()) msg.setVararg_start_idx(fn_.getNumArgs() - 1);
    }
    toThrift(msg, serialCtx);
    container.addToNodes(msg);
    for (Expr child: children_) {
      child.treeToThriftHelper(container, serialCtx);
    }
  }

  // Convert this expr into msg (excluding children), which requires setting
  // msg.op as well as the expr-specific field.
  protected abstract void toThrift(TExprNode msg);

  // Exprs should override this signature of toThrift() if they need access to
  // the ThriftSerializationContext. That is necessary if the Expr is non-deterministic
  // or uses SlotIds/TupleIds. Everything else can simply keep using the old signature
  // for toThrift().
  protected void toThrift(TExprNode msg, ThriftSerializationCtx serialCtx) {
    toThrift(msg);
  }

  /**
   * Returns the product of the given exprs' number of distinct values or -1 if any of
   * the exprs have an invalid number of distinct values. Uses saturating arithmetic,
   * so that if the product would overflow, return Long.MAX_VALUE.
   */
  public static long getNumDistinctValues(List<Expr> exprs) {
    if (exprs == null || exprs.isEmpty()) return 0;
    long numDistinctValues = 1;
    for (Expr expr: exprs) {
      if (expr.getNumDistinctValues() == -1) {
        numDistinctValues = -1;
        break;
      }
      numDistinctValues = MathUtil.saturatingMultiply(
          numDistinctValues, expr.getNumDistinctValues());
    }
    return numDistinctValues;
  }

  public static List<TExpr> treesToThrift(List<? extends Expr> exprs,
      ThriftSerializationCtx serialCtx) {
    List<TExpr> result = new ArrayList<>();
    for (Expr expr: exprs) {
      result.add(expr.treeToThrift(serialCtx));
    }
    return result;
  }

  public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
    return treesToThrift(exprs, new ThriftSerializationCtx());
  }

  public boolean isAggregate() {
    return IS_AGGREGATE.apply(this);
  }

  public List<String> childrenToSql(ToSqlOptions options) {
    List<String> result = new ArrayList<>();
    for (Expr child: children_) {
      result.add(child.toSql(options));
    }
    return result;
  }

  public String debugString() {
    return (id_ != null ? "exprid=" + id_.toString() + " " : "") + debugString(children_);
  }

  public static String debugString(List<? extends Expr> exprs) {
    if (exprs == null || exprs.isEmpty()) return "";
    List<String> strings = new ArrayList<>();
    for (Expr expr: exprs) {
      strings.add(expr.debugString());
    }
    return Joiner.on(" ").join(strings);
  }

  public static String toSql(List<? extends Expr> exprs, ToSqlOptions options) {
    if (exprs == null || exprs.isEmpty()) return "";
    List<String> strings = new ArrayList<>();
    for (Expr expr: exprs) {
      strings.add(expr.toSql(options));
    }
    return Joiner.on(", ").join(strings);
  }

  /**
   * Returns true if this expr matches 'that'. Two exprs match if:
   * 1. The tree structures ignoring implicit casts are the same.
   * 2. For every pair of corresponding SlotRefs, slotRefCmp.matches() returns true.
   * 3. For every pair of corresponding non-SlotRef exprs, localEquals() returns true.
   */
  public boolean matches(Expr that, SlotRef.Comparator slotRefCmp) {
    if (that == null) return false;
    if (this instanceof CastExpr && ((CastExpr)this).isImplicit()) {
      return children_.get(0).matches(that, slotRefCmp);
    }
    if (that instanceof CastExpr && ((CastExpr)that).isImplicit()) {
      return matches(((CastExpr) that).children_.get(0), slotRefCmp);
    }
    if (this instanceof SlotRef && that instanceof SlotRef) {
      return slotRefCmp.matches((SlotRef)this, (SlotRef)that);
    }
    if (!localEquals(that)) return false;
    if (children_.size() != that.children_.size()) return false;
    for (int i = 0; i < children_.size(); ++i) {
      if (!children_.get(i).matches(that.children_.get(i), slotRefCmp)) return false;
    }
    return true;
  }

  /**
   * Local eq comparator. Returns true if this expr is equal to 'that' ignoring children.
   */
  protected boolean localEquals(Expr that) {
    return getClass() == that.getClass() &&
        (fn_ == null ? that.fn_ == null : fn_.equals(that.fn_));
  }

  /**
   * Returns true if two expressions are equal. The equality comparison works on analyzed
   * as well as unanalyzed exprs by ignoring implicit casts. If overridden by a subclass,
   * also provide an appropriate implementation for hashCode.
   */
  @Override
  public final boolean equals(Object obj) {
    return obj instanceof Expr && matches((Expr) obj, SlotRef.SLOTREF_EQ_CMP);
  }

  /**
   * Local hash code that ignores children.
   */
  protected int localHash() {
    return Objects.hash(getClass(), fn_);
  }

  /**
   * Returns a hash code based on the same keys used for equals. Any subclasses that
   * override equals must ensure hashCode is defined such that a.equals(b) implies
   * a.hashCode() == b.hashCode(), as required by
   * https://docs.oracle.com/javase/8/docs/api/java/lang/Object.html#hashCode--
   */
  @Override
  public int hashCode() {
    // CastExpr and SlotRef overload hashCode rather than mirroring 'matches'.
    return Objects.hash(localHash(), children_);
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
   * Return the intersection of l1 and l2.
   */
  public static <C extends Expr> List<C> intersect(List<C> l1, List<C> l2) {
    List<C> result = new ArrayList<>();
    for (C element: l1) {
      if (l2.contains(element)) result.add(element);
    }
    return result;
  }

  /**
   * Gather conjuncts from this expr and return them in a list.
   * A conjunct is an expr that returns a boolean, e.g., Predicates, function calls,
   * SlotRefs, etc. Hence, this method is placed here and not in Predicate.
   */
  public List<Expr> getConjuncts() {
    List<Expr> list = new ArrayList<>();
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
   * Returns true if this expression is trivially true. Currently we only check if
   * it is comprised of conjuncts that are all TRUE literals.
   */
  public boolean isTriviallyTrue() {
    for (Expr conjunct : getConjuncts()) {
      if (!Expr.IS_TRUE_LITERAL.apply(conjunct)) return false;
    }
    return true;
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
    if (preserveRootType && !type_.equals(result.getType())) {
      if (this instanceof CastExpr) {
        CastExpr thisCastExpr = (CastExpr) this;
        TypeCompatibility compatibility = thisCastExpr.getCompatibility();
        return result.castTo(type_, compatibility);
      }
      return result.castTo(type_);
    }
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

  public static List<Expr> trySubstituteList(Iterable<? extends Expr> exprs,
      ExprSubstitutionMap smap, Analyzer analyzer, boolean preserveRootTypes)
          throws AnalysisException {
    if (exprs == null) return null;
    List<Expr> result = new ArrayList<>();
    for (Expr e: exprs) {
      result.add(e.trySubstitute(smap, analyzer, preserveRootTypes));
    }
    return result;
  }

  public static List<Expr> substituteList(Iterable<? extends Expr> exprs,
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
    substituteImplOnChildren(smap, analyzer);
    resetAnalysisState();
    return this;
  }

  protected final void substituteImplOnChildren(ExprSubstitutionMap smap,
      Analyzer analyzer) {
    for (int i = 0; i < children_.size(); ++i) {
      children_.set(i, children_.get(i).substituteImpl(smap, analyzer));
    }
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

  public static List<Expr> resetList(List<Expr> l) {
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
  public static <C extends Expr> List<C> cloneList(List<C> l) {
    Preconditions.checkNotNull(l);
    List<C> result = new ArrayList<>(l.size());
    for (Expr element: l) {
      result.add((C) element.clone());
    }
    return result;
  }

  /**
   * Create a deep copy of 'ls'. The elements of the returned list are of the same
   * type as the input list.
   */
  public static <C extends Expr> List<List<C>> deepCopy(List<List<C>> ls) {
    Preconditions.checkNotNull(ls);
    List<List<C>> result = new ArrayList<>(ls.size());
    for (List<C> l : ls) {
      if (l == null) {
        result.add(null);
        continue;
      }
      List<C> l2 = new ArrayList<>(l.size());
      for (Expr element : l) {
        l2.add((C) element.clone());
      }
      result.add(l2);
    }
    return result;
  }

  /**
   * Create a clone of the expression including analysis state with a different
   * selectivity.
   */
  public Expr cloneAndOverrideSelectivity(double selectivity) {
    Preconditions.checkArgument(selectivity >= 0.0 && selectivity <= 1.0, selectivity);
    Expr e = clone();
    e.selectivity_ = selectivity;
    return e;
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
   * Return a new list without duplicate exprs (according to matches() using cmp).
   */
  public static <C extends Expr> List<C> removeDuplicates(List<C> l,
      SlotRef.Comparator cmp) {
    List<C> newList = new ArrayList<>();
    for (C expr: l) {
      boolean exists = false;
      for (C newExpr : newList) {
        if (newExpr.matches(expr, cmp)) {
          exists = true;
          break;
        }
      }
      if (!exists) newList.add(expr);
    }
    return newList;
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

  /**
   * Propagates constant expressions of the form <slot ref> = <constant> or,
   * for certain data types, <slot ref> range_operator <constant> to
   * other uses of slot ref in the given conjuncts; returns a BitSet with
   * bits set to true in all changed indices.  Only one round of substitution
   * is performed.  The candidates BitSet is used to determine which members of
   * conjuncts are considered for propagation. The keepConjuncts list is
   * populated in specific cases (e.g for date/time range predicate propagation)
   * with the original conjuncts that need to be preserved even after rewrite.
   */
  private static BitSet propagateConstants(List<Expr> conjuncts, BitSet candidates,
      List<Expr> keepConjuncts, Analyzer analyzer) {
    Preconditions.checkState(conjuncts.size() <= candidates.size());

    // first pass: gather the constant predicates into separate buckets
    // for range and equality predicates
    ConstantPredicateHandler handler = new ConstantPredicateHandler();
    handler.classifyPredicates(conjuncts, candidates);

    // second pass: propagate constants to the other predicates and keep track
    // of which other predicates have been changed
    BitSet changed = new BitSet(conjuncts.size());
    handler.propagateConstantPreds(conjuncts, changed, keepConjuncts, analyzer);
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
    List<Expr> tmpConjuncts = new ArrayList<>();
    List<Expr> keepConjuncts = new ArrayList<>();
    try {
      BitSet candidates = new BitSet(conjuncts.size());
      candidates.set(0, Math.min(conjuncts.size(), CONST_PROPAGATION_EXPR_LIMIT));
      int transfers = 0;
      tmpConjuncts.addAll(conjuncts);
      // Constant propagation may make other slots constant, so repeat the process
      // until there are no more changes.
      while (!candidates.isEmpty()) {
        // Use tmpConjuncts instead of conjuncts because propagateConstants can
        // change the content of the first input param. We do not want to
        // change the expr in conjucts before make sure the constant propagation
        // does not cause analysis failures.
        BitSet changed = propagateConstants(tmpConjuncts, candidates, keepConjuncts,
            analyzer);
        candidates.clear();
        int pruned = 0;
        for (int i = changed.nextSetBit(0); i >= 0; i = changed.nextSetBit(i+1)) {
          // When propagating constants, we may de-normalize expressions, so we
          // must normalize binary predicates.  Any additional rules will be
          // applied by the rewriter.
          int index = i - pruned;
          Preconditions.checkState(index >= 0);
          ExprRewriter rewriter = analyzer.getExprRewriter();
          Expr rewritten = rewriter.rewrite(tmpConjuncts.get(index), analyzer);
          // Re-analyze to add implicit casts and update cost
          rewritten.reset();
          rewritten.analyze(analyzer);
          if (!rewritten.isConstant()) {
            conjuncts.set(index, rewritten);
            tmpConjuncts.set(index, rewritten);
            if (++transfers < CONST_PROPAGATION_EXPR_LIMIT) candidates.set(index, true);
            continue;
          }
          // Remove constant boolean literal expressions.  N.B. - we may have
          // expressions determined to be constant which can not yet be discarded
          // because they can't be evaluated if expr rewriting is turned off.
          if (IS_NULL_LITERAL.apply(rewritten) ||
              IS_FALSE_LITERAL.apply(rewritten)) {
            conjuncts.clear();
            conjuncts.add(rewritten);
            return false;
          }
          if (IS_TRUE_LITERAL.apply(rewritten)) {
            pruned++;
            conjuncts.remove(index);
            tmpConjuncts.remove(index);
          }
        }
      }
    } catch (AnalysisException e) {
      LOG.warn("Not able to analyze after rewrite: " + e.toString() + " conjuncts: " +
          Expr.debugString(conjuncts));
    }
    // add the conjuncts that need to be preserved
    conjuncts.addAll(keepConjuncts);
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

  /**
   * Find all unique slot and/or tuple ids referenced by this expr tree.
   * @param tupleIds unique tuple IDs from this expr tree are appended here.
   * @param slotIds unique slot IDs from this expr tree are appended here.
   */
  public void getIds(List<TupleId> tupleIds, List<SlotId> slotIds) {
    Set<TupleId> tupleIdSet = new HashSet<>();
    Set<SlotId> slotIdSet = new HashSet<>();
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
   * Returns true if this expression tree references a slot in the tuple identified
   * by tid.
   */
  public boolean referencesTuple(TupleId tid) {
    // This is the default implementation. Expr subclasses that reference slots in
    // tuples, i.e. SlotRef, must override this.
    for (Expr child: children_) {
      if (child.referencesTuple(tid)) return true;
    }
    return false;
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
   * Return true if and only if all exprs in 'exprs' are constant.
   */
  public static boolean allConstant(List<Expr> exprs) {
    for (Expr p: exprs) {
      if (!p.isConstant()) return false;
    }
    return true;
  }

  /**
   * Return true if this expr is a scalar subquery.
   */
  public boolean isScalarSubquery() {
    Preconditions.checkState(isAnalyzed_);
    if (!(this instanceof Subquery)) return false;
    Subquery subq = (Subquery) this;
    SelectStmt stmt = (SelectStmt) subq.getStatement();
    return stmt.returnsAtMostOneRow() && getType().isScalarType();
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
   * Casts this expr to a specific target type. It checks the validity of the cast
   * according to 'compatibility' and calls uncheckedCastTo().
   * @param targetType
   *          type to be cast to
   * @param compatibility
   *          compatibility level that defines the relation between 'targetType' and the
   *          type of the expression
   * @return cast expression, or converted literal,
   *         should never return null
   * @throws AnalysisException
   *           when an invalid cast is asked for, for example,
   *           failure to convert a string literal to a date literal
   */
  public final Expr castTo(Type targetType, TypeCompatibility compatibility)
      throws AnalysisException {
    Type type = Type.getAssignmentCompatibleType(this.type_, targetType, compatibility);
    Preconditions.checkState(type.isValid(), "cast %s to %s", this.type_, targetType);
    // If the targetType is NULL_TYPE then ignore the cast because NULL_TYPE
    // is compatible with all types and no cast is necessary.
    if (targetType.isNull()) return this;
    // If decimal, cast to the target type.
    if (targetType.isDecimal()) return uncheckedCastTo(targetType, compatibility);
    // If they match, cast to the type both values can be assigned to (the definition of
    // getAssignmentCompatibleType), which implies no loss of precision. Note that
    // getAssignmentCompatibleType always returns a "real" (not wildcard) type.
    if (type.matchesType(targetType)) return uncheckedCastTo(type, compatibility);
    throw new SqlCastException("targetType=" + targetType + " type=" + type);
  }

  public final Expr castTo(Type targetType) throws AnalysisException {
    return castTo(targetType, TypeCompatibility.DEFAULT);
  }

  /**
   * Create an expression equivalent to 'this' but returning targetType; possibly by
   * inserting an implicit cast, or by returning an altogether new expression or by
   * returning 'this' with a modified return type'.
   *
   * @param targetType    type to be cast to
   * @param compatibility compatibility level used to calculate the cast
   * @return cast expression, or converted literal, should never return null
   * @throws AnalysisException when an invalid cast is asked for, for example, failure to
   *                           convert a string literal to a date literal
   */
  protected Expr uncheckedCastTo(Type targetType, TypeCompatibility compatibility)
      throws AnalysisException {
    return new CastExpr(targetType, this, compatibility);
  }

  protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
    return uncheckedCastTo(targetType, TypeCompatibility.DEFAULT);
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
  public Expr ignoreImplicitCast() { return this; }

  /**
   * Returns true if 'this' is an implicit cast expr.
   */
  public boolean isImplicitCast() { return false; }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this.getClass())
        .add("id", id_)
        .add("type", type_)
        .add("toSql", toSql(ToSqlOptions.SHOW_IMPLICIT_CASTS))
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
   * Returns the first child if this Expr is a CastExpr or builtin cast
   * function. Otherwise, returns 'this'.
   */
  public Expr unwrapExpr(boolean implicitOnly) {
    if ((this instanceof CastExpr
        && (!implicitOnly || ((CastExpr) this).isImplicit()))
        || (this instanceof FunctionCallExpr
            && ((FunctionCallExpr) this).isBuiltinCastFunction())) {
      return children_.get(0);
    }
    return this;
  }

  /**
   * Returns the source expression for this expression. Traverses the source
   * exprs of intermediate slot descriptors to resolve materialization points
   * (e.g., aggregations). Returns null if there are multiple source Exprs
   * mapped to the expression at any given point.
   */
  public Expr findSrcExpr() {
    // If the source expression is a constant expression, it won't have a scanSlotRef
    // and we can return this.
    if (isConstant()) {
      return this;
    }
    SlotRef slotRef = unwrapSlotRef(false);
    if (slotRef == null) return null;
    SlotDescriptor slotDesc = slotRef.getDesc();
    if (slotDesc.isScanSlot()) return slotRef;
    if (slotDesc.getSourceExprs().size() == 1) {
      return slotDesc.getSourceExprs().get(0).findSrcExpr();
    }
    // No known source expr, or there are several source exprs meaning the slot is
    // has no single source table.
    return null;
  }

  /**
   * Returns the descriptor of the scan slot that directly or indirectly produces
   * the values of 'this' SlotRef. Traverses the source exprs of intermediate slot
   * descriptors to resolve materialization points (e.g., aggregations).
   * Returns null if 'e' or any source expr of 'e' is not a SlotRef or cast SlotRef.
   */
  public SlotDescriptor findSrcScanSlot() {
    Expr sourceExpr = findSrcExpr();
    if (sourceExpr == null) {
      return null;
    }
    SlotRef slotRef = sourceExpr.unwrapSlotRef(false);
    if (slotRef == null) {
      return null;
    }
    return slotRef.getDesc();
  }

  /**
   * Pushes negation to the individual operands of a predicate
   * tree rooted at 'root'.
   */
  public static Expr pushNegationToOperands(Expr root) {
    Preconditions.checkNotNull(root);
    if (IS_NOT_PREDICATE.apply(root)) {
      try {
        // Make sure we call function 'negate' only on classes that support it,
        // otherwise we may recurse infinitely.
        root.getChild(0).getClass().getDeclaredMethod(NEGATE_FN);
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
    List<Subquery> subqueries = new ArrayList<>();
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
      return ((StringLiteral) e).getUnescapedValue().length();
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
  public static String listToSql(List<Expr> exprs, ToSqlOptions options) {
    com.google.common.base.Function<Expr, String> toSql =
        new com.google.common.base.Function<Expr, String>() {
        @Override
        public String apply(Expr arg) {
          return arg.toSql(options);
        }
    };
    return Joiner.on(",").join(Iterables.transform(exprs, toSql));
  }

  public static String getExplainString(
      List<? extends Expr> exprs, TExplainLevel detailLevel) {
    if (exprs == null) return "";
    ToSqlOptions toSqlOptions =
        detailLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal() ?
        ToSqlOptions.SHOW_IMPLICIT_CASTS :
        ToSqlOptions.DEFAULT;
    StringBuilder output = new StringBuilder();
    for (int i = 0; i < exprs.size(); ++i) {
      if (i > 0) output.append(", ");
      output.append(exprs.get(i).toSql(toSqlOptions));
    }
    return output.toString();
  }

  /**
   * Analyzes and evaluates expression to an integral value, returned as a long.
   * Throws if the expression cannot be evaluated or if the value evaluates to null.
   * The 'name' parameter is used in exception messages, e.g. "LIMIT expression
   * evaluates to NULL". If 'acceptDate' is true, treat Date expression as integral
   * expression as well.
   */
  public long evalToInteger(Analyzer analyzer, String name, boolean acceptDate)
      throws AnalysisException {
    // Check for slotrefs and subqueries before analysis so we can provide a more
    // helpful error message.
    if (contains(SlotRef.class) || contains(Subquery.class)) {
      throw new AnalysisException(name + " expression must be a constant expression: " +
          toSql());
    }
    analyze(analyzer);
    if (!isConstant()) {
      throw new AnalysisException(name + " expression must be a constant expression: " +
          toSql());
    }
    if (!getType().isIntegerType() && !(acceptDate && getType().isDate())) {
      throw new AnalysisException(name + " expression must be an integer type but is '" +
          getType() + "': " + toSql());
    }
    TColumnValue val = null;
    try {
      val = FeSupport.EvalExprWithoutRow(this, analyzer.getQueryCtx());
    } catch (InternalException e) {
      throw new AnalysisException("Failed to evaluate expr: " + toSql(), e);
    }

    try {
      return evalToInteger(val, acceptDate);
    } catch (AnalysisException e) {
      throw new AnalysisException(name + " expression evaluates to NULL: " + toSql());
    }
  }

  public static long evalToInteger(TColumnValue val, boolean acceptDate)
      throws AnalysisException {
    long value;
    if (val.isSetLong_val()) {
      value = val.getLong_val();
    } else if (val.isSetInt_val()) {
      value = val.getInt_val();
    } else if (val.isSetShort_val()) {
      value = val.getShort_val();
    } else if (val.isSetByte_val()) {
      value = val.getByte_val();
    } else if (acceptDate && val.isSetDate_val()) {
      value = val.getDate_val();
    } else {
      throw new AnalysisException("TColumnValue evaluates to NULL: " + val);
    }
    return value;
  }

  public long evalToInteger(Analyzer analyzer, String name) throws AnalysisException {
    return evalToInteger(analyzer, name, false);
  }

  /**
   * Analyzes and evaluates expression to a non-negative integral value, returned as a
   * long. Throws if the expression cannot be evaluated, if the value evaluates to null,
   * or if the result is negative. The 'name' parameter is used in exception messages,
   * e.g. "LIMIT expression evaluates to NULL".
   */
  public long evalToNonNegativeInteger(Analyzer analyzer, String name)
      throws AnalysisException {
    long value = evalToInteger(analyzer, name);
    if (value < 0) {
      throw new AnalysisException(name + " must be a non-negative integer: " +
          toSql() + " = " + value);
    }
    return value;
  }

  public void setPredicateHints(List<PlanHint> hints) {
    Preconditions.checkNotNull(hints);
    predicateHints_ = hints;
  }

  public List<PlanHint> getPredicateHints() { return predicateHints_; }

  // A wrapper method to create null literal. This can be overriden
  // in a derived class
  protected Expr createNullLiteral() {
    return new NullLiteral();
  }

  /**
   * Subclass that contains query statements, e.g SubQuery, should override this.
   */
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    boolean hasChanges = false;
    for (Expr child : children_) {
      hasChanges |= child.resolveTableMask(analyzer);
    }
    return hasChanges;
  }

  /**
   * A slot descriptor may be associated with more than 1 source expression.
   * This method returns the first source expr in that case or null if there
   * are no source exprs.
   */
  public Expr getSlotDescFirstSourceExpr() {
    SlotRef slotRef = unwrapSlotRef(false);
    if (slotRef == null) return null;
    SlotDescriptor slotDesc = slotRef.getDesc();
    if (slotDesc.getSourceExprs().size() >= 1) {
      return slotDesc.getSourceExprs().get(0);
    }
    return null;
  }

  /**
   * Returns the first non-const expression on the following path:
   *  - checking whether the expression itself is constant or not
   *  If there's an underlying slot ref:
   *  - checking the slot desc's source expressions constness
   */
  public Optional<Expr> getFirstNonConstSourceExpr() {
    SlotRef slotRef = unwrapSlotRef(false);

    if (slotRef == null) {
      Preconditions.checkState(isAnalyzed());
      return isConstant() ? Optional.empty() : Optional.of(this);
    }

    SlotDescriptor slotDesc = slotRef.getDesc();
    Preconditions.checkNotNull(slotDesc);

    if (slotDesc.getSourceExprs().isEmpty()) {
      return Optional.of(this);
    }

    Optional<Expr> nonConstSourceExpr = Optional.empty();
    for (Expr expr : slotDesc.getSourceExprs()) {
      Preconditions.checkState(expr.isAnalyzed());
      if (!expr.isConstant()) {
        nonConstSourceExpr = Optional.of(expr);
        break;
      }
    }
    return nonConstSourceExpr;
  }

  /**
   * Returns true if 'this' is a constant.
   */
  public boolean shouldConvertToCNF() {
    return isConstant();
  }
}
