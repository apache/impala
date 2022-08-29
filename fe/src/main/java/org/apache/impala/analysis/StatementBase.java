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

import java.util.Collections;
import java.util.List;

import java.util.Optional;
import org.apache.commons.lang.NotImplementedException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.catalog.TypeCompatibility;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.rewrite.ExprRewriter;

import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Base class for all Impala SQL statements.
 */
public abstract class StatementBase extends StmtNode {

  // True if this Stmt is the top level of an explain stmt.
  protected boolean isExplain_ = false;

  /////////////////////////////////////////
  // BEGIN: Members that need to be reset()

  // Analyzer that was used to analyze this statement.
  protected Analyzer analyzer_;

  // END: Members that need to be reset()
  /////////////////////////////////////////

  protected StatementBase() { }

  /**
   * C'tor for cloning.
   */
  protected StatementBase(StatementBase other) {
    analyzer_ = other.analyzer_;
    isExplain_ = other.isExplain_;
  }

  /**
   * Returns all table references in this statement and all its nested statements.
   * The TableRefs are collected depth-first in SQL-clause order.
   * Subclasses should override this method as necessary.
   */
  public void collectTableRefs(List<TableRef> tblRefs) { }

  /**
   * Analyzes the statement and throws an AnalysisException if analysis fails. A failure
   * could be due to a problem with the statement or because one or more tables/views
   * were missing from the catalog.
   * It is up to the analysis() implementation to ensure the maximum number of missing
   * tables/views get collected in the Analyzer before failing analyze().
   */
  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed()) return;
    if (isExplain_) analyzer.setIsExplain();
    analyzer_ = analyzer;
  }

  /**
   * Returns the output column labels of this statement, if applicable, or an empty list
   * if not applicable (not all statements produce an output result set).
   * Subclasses must override this as necessary.
   */
  public List<String> getColLabels() { return Collections.<String>emptyList();  }

  /**
   * Sets the column labels of this statement, if applicable. No-op of the statement does
   * not produce an output result set.
   */
  public void setColLabels(List<String> colLabels) {
    List<String> oldLabels = getColLabels();
    if (oldLabels == colLabels) return;
    oldLabels.clear();
    oldLabels.addAll(colLabels);
  }

  /**
   * Returns the unresolved result expressions of this statement, if applicable, or an
   * empty list if not applicable (not all statements produce an output result set).
   * Subclasses must override this as necessary.
   */
  public List<Expr> getResultExprs() { return Collections.<Expr>emptyList(); }

  /**
   * Casts the result expressions and derived members (e.g., destination column types for
   * CTAS) to the given types. No-op if this statement does not have result expressions.
   * Throws when casting fails. Subclasses may override this as necessary.
   */
  public void castResultExprs(List<Type> types) throws AnalysisException {
    List<Expr> resultExprs = getResultExprs();
    Preconditions.checkNotNull(resultExprs);
    Preconditions.checkState(resultExprs.size() == types.size());
    for (int i = 0; i < types.size(); ++i) {
      if (!resultExprs.get(i).getType().equals(types.get(i))) {
        resultExprs.set(i, resultExprs.get(i).castTo(types.get(i)));
      }
    }
  }

  /**
   * Uses the given 'rewriter' to transform all Exprs in this statement according
   * to the rules specified in the 'rewriter'. Replaces the original Exprs with the
   * transformed ones in-place. Subclasses that have Exprs to be rewritten must
   * override this method. Valid to call after analyze().
   */
  public void rewriteExprs(ExprRewriter rewriter) throws AnalysisException {
    throw new IllegalStateException(
        "rewriteExprs() not implemented for this stmt: " + getClass().getSimpleName());
  }

  public Analyzer getAnalyzer() { return analyzer_; }
  public boolean isAnalyzed() { return analyzer_ != null; }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  /**
   * If ToSqlOptions.REWRITTEN is passed, then this returns the rewritten SQL only if
   * the statement was rewritten. Otherwise, the original SQL will be returned instead.
   * It is the caller's responsibility to know if/when the statement was indeed rewritten.
   * @param options
   */
  @Override
  public String toSql(ToSqlOptions options) { return ""; }

  public void setIsExplain() { isExplain_ = true; }
  public boolean isExplain() { return isExplain_; }

  /**
   * Returns a deep copy of this node including its analysis state. Some members such as
   * tuple and slot descriptors are generally not deep copied to avoid potential
   * confusion of having multiple descriptor instances with the same id, although
   * they should be unique in the descriptor table.
   * TODO for 2.3: Consider also cloning table and slot descriptors for clarity,
   * or otherwise make changes to more provide clearly defined clone() semantics.
   */
  @Override
  public StatementBase clone() {
    throw new NotImplementedException(
        "Clone() not implemented for " + getClass().getSimpleName());
  }

  /**
   * Resets the internal analysis state of this node.
   * For easier maintenance, class members that need to be reset are grouped into
   * a 'section' clearly indicated by comments as follows:
   *
   * class SomeStmt extends StatementBase {
   *   ...
   *   /////////////////////////////////////////
   *   // BEGIN: Members that need to be reset()
   *
   *   <member declarations>
   *
   *   // END: Members that need to be reset()
   *   /////////////////////////////////////////
   *   ...
   * }
   *
   * In general, members that are set or modified during analyze() must be reset().
   * TODO: Introduce this same convention for Exprs, possibly by moving clone()/reset()
   * into the ParseNode interface for clarity.
   */
  public void reset() { analyzer_ = null; }

  /**
   * Checks that 'srcExpr' is type compatible with 'dstCol' and returns a type compatible
   * expression by applying a CAST() if needed. Throws an AnalysisException if the types
   * are incompatible. 'dstTableName' is only used when constructing an AnalysisException
   * message.
   *
   * 'widestTypeSrcExpr' is the first widest type expression of the source expressions.
   *
   * If compatibility is unsafe and the source expression is not constant, compatibility
   * ignores the unsafe option.
   */
  public static Expr checkTypeCompatibility(String dstTableName, Column dstCol,
      Expr srcExpr, Analyzer analyzer, Expr widestTypeSrcExpr) throws AnalysisException {
    // In 'ValueStmt', if all values in a column are CHARs but they have different
    // lengths, they are implicitly cast to VARCHAR to avoid padding (see IMPALA-10753 and
    // ValuesStmt.java). Since VARCHAR cannot normally be cast to CHAR because of possible
    // loss of precision, if the destination column is CHAR, we have to manually cast the
    // values back to CHAR. There is no danger of loss of precision here because we cast
    // the values to the CHAR type of the same length as the VARCHAR type. If that is
    // still not compatible with the destination column, we throw an AnalysisException.
    if (widestTypeSrcExpr != null &&  widestTypeSrcExpr.isImplicitCast()) {
      // TODO: Can we avoid casting CHAR -> VARCHAR -> CHAR for CHAR dst columns?
      Expr exprWithoutImplicitCast = widestTypeSrcExpr.ignoreImplicitCast();
      if (srcExpr.getType().isVarchar() && exprWithoutImplicitCast.getType().isChar()) {
        ScalarType varcharType = (ScalarType) srcExpr.getType();
        ScalarType charType = (ScalarType) exprWithoutImplicitCast.getType();
        Preconditions.checkState(varcharType.getLength() >= charType.getLength());
        Type newCharType = ScalarType.createCharType(varcharType.getLength());
        Expr newCharExpr = new CastExpr(newCharType, srcExpr);
        return checkTypeCompatibilityHelper(dstTableName, dstCol, newCharExpr,
            analyzer, null);
      }
    }

    return checkTypeCompatibilityHelper(dstTableName, dstCol, srcExpr, analyzer,
        widestTypeSrcExpr);
  }

  private static Expr checkTypeCompatibilityHelper(String dstTableName, Column dstCol,
      Expr srcExpr, Analyzer analyzer, Expr widestTypeSrcExpr) throws AnalysisException {
    Type dstColType = dstCol.getType();
    Type srcExprType = srcExpr.getType();

    if (widestTypeSrcExpr == null) widestTypeSrcExpr = srcExpr;
    // Trivially compatible, unless the type is complex.
    if (dstColType.equals(srcExprType) && !dstColType.isComplexType()) return srcExpr;

    TypeCompatibility permissiveCompatibility =
        analyzer.getPermissiveCompatibilityLevel();
    TypeCompatibility compatibilityLevel = analyzer.getRegularCompatibilityLevel();

    Type compatType =
        Type.getAssignmentCompatibleType(srcExprType, dstColType, compatibilityLevel);

    if (compatType.isInvalid() && permissiveCompatibility.isUnsafe()) {
      Optional<Expr> expr = srcExpr.getFirstNonConstSourceExpr();
      if (expr.isPresent()) {
        throw new AnalysisException(String.format(
            "Unsafe implicit cast is prohibited for non-const expression: %s ",
            expr.get().toSql()));
      }
      compatType = Type.getAssignmentCompatibleType(
          srcExprType, dstColType, permissiveCompatibility);
      compatibilityLevel = permissiveCompatibility;
    }

    if (!compatType.isValid()) {
      throw new AnalysisException(String.format(
          "Target table '%s' is incompatible with source expressions.\nExpression '%s' " +
              "(type: %s) is not compatible with column '%s' (type: %s)",
          dstTableName, srcExpr.toSql(), srcExprType.toSql(), dstCol.getName(),
          dstColType.toSql()));
    }

    if (!compatType.equals(dstColType) && !compatType.isNull()
        && !permissiveCompatibility.isUnsafe()) {
      throw new AnalysisException(String.format(
          "Possible loss of precision for target table '%s'.\nExpression '%s' (type: "
              + "%s) would need to be cast to %s for column '%s'",
          dstTableName, widestTypeSrcExpr.toSql(), srcExprType.toSql(),
          dstColType.toSql(), dstCol.getName()));
    }
    return srcExpr.castTo(compatType, compatibilityLevel);
  }
}
