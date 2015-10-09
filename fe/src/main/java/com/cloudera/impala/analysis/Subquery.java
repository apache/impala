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

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.ArrayType;
import com.cloudera.impala.catalog.StructField;
import com.cloudera.impala.catalog.StructType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TExprNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Class representing a subquery. A Subquery consists of a QueryStmt and has
 * its own Analyzer context.
 */
public class Subquery extends Expr {
  private final static Logger LOG = LoggerFactory.getLogger(Subquery.class);

  // The QueryStmt of the subquery.
  protected QueryStmt stmt_;
  // A subquery has its own analysis context
  protected Analyzer analyzer_;

  public Analyzer getAnalyzer() { return analyzer_; }
  public QueryStmt getStatement() { return stmt_; }
  @Override
  public String toSqlImpl() { return "(" + stmt_.toSql() + ")"; }

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
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    super.analyze(analyzer);
    if (!(stmt_ instanceof SelectStmt)) {
      throw new AnalysisException("A subquery must contain a single select block: " +
        toSql());
    }
    // The subquery is analyzed with its own analyzer.
    analyzer_ = new Analyzer(analyzer);
    analyzer_.setIsSubquery();
    stmt_.analyze(analyzer_);
    // Check whether the stmt_ contains an illegal mix of un/correlated table refs.
    stmt_.getCorrelatedTupleIds(analyzer_);

    // Set the subquery type based on the types of the exprs in the
    // result list of the associated SelectStmt.
    ArrayList<Expr> stmtResultExprs = stmt_.getResultExprs();
    if (stmtResultExprs.size() == 1) {
      type_ = stmtResultExprs.get(0).getType();
      Preconditions.checkState(!type_.isComplexType());
    } else {
      type_ = createStructTypeFromExprList();
    }

    // If the subquery returns many rows, set its type to ArrayType.
    if (!((SelectStmt)stmt_).returnsSingleRow()) type_ = new ArrayType(type_);

    Preconditions.checkNotNull(type_);
    isAnalyzed_ = true;
  }

  @Override
  public boolean isConstant() { return false; }

  /**
   * Check if the subquery's SelectStmt returns a single column of scalar type.
   */
  public boolean returnsScalarColumn() {
    ArrayList<Expr> stmtResultExprs = stmt_.getResultExprs();
    if (stmtResultExprs.size() == 1 && stmtResultExprs.get(0).getType().isScalarType()) {
      return true;
    }
    return false;
  }

  /**
   * Create a StrucType from the result expr list of a subquery's SelectStmt.
   */
  private StructType createStructTypeFromExprList() {
    ArrayList<Expr> stmtResultExprs = stmt_.getResultExprs();
    ArrayList<StructField> structFields = Lists.newArrayList();
    // Check if we have unique labels
    ArrayList<String> labels = stmt_.getColLabels();
    boolean hasUniqueLabels = true;
    if (Sets.newHashSet(labels).size() != labels.size()) hasUniqueLabels = false;

    // Construct a StructField from each expr in the select list
    for (int i = 0; i < stmtResultExprs.size(); ++i) {
      Expr expr = stmtResultExprs.get(i);
      String fieldName = null;
      // Check if the label meets the Metastore's requirements.
      if (MetaStoreUtils.validateName(labels.get(i))) {
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

  @Override
  public Subquery clone() { return new Subquery(this); }

  @Override
  protected void toThrift(TExprNode msg) {}
}
