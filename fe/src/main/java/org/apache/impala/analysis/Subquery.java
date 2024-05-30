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
import java.util.List;
import java.util.Objects;

import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.StructType;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TExprNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Class representing a subquery. A Subquery consists of a QueryStmt and has
 * its own Analyzer context.
 */
public class Subquery extends Expr {
  // The QueryStmt of the subquery.
  protected QueryStmt stmt_;
  // A subquery has its own analysis context
  protected Analyzer analyzer_;

  public Analyzer getAnalyzer() { return analyzer_; }
  public QueryStmt getStatement() { return stmt_; }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return "(" + stmt_.toSql(options) + ")";
  }

  /**
   * C'tor that initializes a Subquery from a QueryStmt.
   */
  public Subquery(QueryStmt queryStmt) {
    super();
    Preconditions.checkNotNull(queryStmt);
    stmt_ = queryStmt;
  }

  /**
   * Copy c'tor.
   */
  public Subquery(Subquery other) {
    super(other);
    stmt_ = other.stmt_.clone();
    analyzer_ = other.analyzer_;
  }

  /**
   * Analyzes the subquery in a child analyzer.
   */
  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    if (!(stmt_ instanceof SelectStmt)) {
      throw new AnalysisException("A subquery must contain a single select block: " +
        toSql());
    }
    // The subquery is analyzed with its own analyzer.
    analyzer_ = new Analyzer(analyzer);
    analyzer_.setIsSubquery();
    stmt_.analyze(analyzer_);
    // Check whether the stmt_ contains an illegal mix of un/correlated table refs.
    stmt_.getCorrelatedTupleIds();

    // Set the subquery type based on the types of the exprs in the
    // result list of the associated SelectStmt.
    List<Expr> stmtResultExprs = stmt_.getResultExprs();
    if (stmtResultExprs.size() == 1) {
      type_ = stmtResultExprs.get(0).getType();
      if (type_.isComplexType()) {
        throw new AnalysisException("A subquery can't return complex types. " + toSql());
      }
    } else {
      type_ = createStructTypeFromExprList();
    }

    // If the subquery can return many rows, do the cardinality check at runtime.
    if (!((SelectStmt)stmt_).returnsAtMostOneRow()) stmt_.setIsRuntimeScalar(true);

    Preconditions.checkNotNull(type_);
  }

  @Override
  protected float computeEvalCost() { return UNKNOWN_COST; }

  @Override
  protected boolean isConstantImpl() { return false; }

  /**
   * Check if the subquery's SelectStmt returns a single column of scalar type.
   */
  public boolean returnsScalarColumn() {
    List<Expr> stmtResultExprs = stmt_.getResultExprs();
    if (stmtResultExprs.size() == 1 && stmtResultExprs.get(0).getType().isScalarType()) {
      return true;
    }
    return false;
  }

  /**
   * Create a StrucType from the result expr list of a subquery's SelectStmt.
   */
  private StructType createStructTypeFromExprList() {
    List<Expr> stmtResultExprs = stmt_.getResultExprs();
    List<StructField> structFields = new ArrayList<>();
    // Check if we have unique labels
    List<String> labels = stmt_.getColLabels();
    boolean hasUniqueLabels = true;
    if (Sets.newHashSet(labels).size() != labels.size()) hasUniqueLabels = false;

    // Construct a StructField from each expr in the select list
    for (int i = 0; i < stmtResultExprs.size(); ++i) {
      Expr expr = stmtResultExprs.get(i);
      String fieldName = null;
      // Check if the label meets the Metastore's requirements.
      if (MetastoreShim.validateName(labels.get(i))) {
        fieldName = labels.get(i);
        // Make sure the field names are unique.
        if (!hasUniqueLabels) {
          fieldName = "_" + Integer.toString(i) + "_" + fieldName;
        }
      } else {
        // Use the expr ordinal to construct a StructField.
        fieldName = "_" + Integer.toString(i);
      }
      Preconditions.checkNotNull(fieldName);
      structFields.add(new StructField(fieldName, expr.getType(), null));
    }
    Preconditions.checkState(structFields.size() != 0);
    return new StructType(structFields);
  }

  /**
   * Returns true if the toSql() of the Subqueries is identical. May return false for
   * equivalent statements even due to minor syntactic differences like parenthesis.
   * TODO: Switch to a less restrictive implementation.
   */
  @Override
  protected boolean localEquals(Expr that) {
    return super.localEquals(that) &&
        stmt_.toSql().equals(((Subquery)that).stmt_.toSql());
  }

  @Override
  protected int localHash() {
    return Objects.hash(super.localHash(), stmt_.toSql());
  }

  @Override
  public Subquery clone() { return new Subquery(this); }

  @Override
  protected void toThrift(TExprNode msg) {}

  @Override
  public boolean resolveTableMask(Analyzer analyzer) throws AnalysisException {
    return stmt_.resolveTableMask(analyzer);
  }

  @Override
  protected void resetAnalysisState() {
    super.resetAnalysisState();
    stmt_.reset();
    analyzer_ = null;
  }
}
