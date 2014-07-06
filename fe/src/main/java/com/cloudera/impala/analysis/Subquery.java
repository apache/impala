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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.thrift.TExprNode;
import com.google.common.base.Preconditions;

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

  public QueryStmt getStatement() { return stmt_; }
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
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);
    if (!(stmt_ instanceof SelectStmt)) {
      throw new AnalysisException("A subquery must contain a single select block: " +
        toSql());
    }
    // The subquery is analyzed with its own analyzer.
    analyzer_ = new Analyzer(analyzer, analyzer.getUser());
    analyzer_.setIsSubquery();
    stmt_.analyze(analyzer_);
    ArrayList<Expr> stmtResultExprs = stmt_.getResultExprs();
    // If the subquery returns a single column, set the type of this expr to be
    // the type of the return expr in the subquery's select clause.
    // TODO: When complex types arrive this can also be a struct, so this check
    // may no longer be valid.
    if (stmtResultExprs.size() == 1) type_ = stmtResultExprs.get(0).getType();
    isAnalyzed_ = true;
  }

  @Override
  public Subquery clone() { return new Subquery(this); }

  @Override
  protected void toThrift(TExprNode msg) {}
}
