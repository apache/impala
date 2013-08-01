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

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.opcode.FunctionOperator;
import com.cloudera.impala.thrift.TCaseExpr;
import com.cloudera.impala.thrift.TExprNode;
import com.cloudera.impala.thrift.TExprNodeType;
import com.google.common.base.Preconditions;

/**
 * CaseExpr represents the SQL expression
 * CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr).
 * If a case expr is given then it is the first child.
 * If an else expr is given then it is the last child.
 *
 */
public class CaseExpr extends Expr {
  private boolean hasCaseExpr;
  private boolean hasElseExpr;

  public CaseExpr(Expr caseExpr, List<CaseWhenClause> whenClauses, Expr elseExpr) {
    super();
    if (caseExpr != null) {
      children.add(caseExpr);
      hasCaseExpr = true;
    }
    for (CaseWhenClause whenClause: whenClauses) {
      Preconditions.checkNotNull(whenClause.getWhenExpr());
      children.add(whenClause.getWhenExpr());
      Preconditions.checkNotNull(whenClause.getThenExpr());
      children.add(whenClause.getThenExpr());
    }
    if (elseExpr != null) {
      children.add(elseExpr);
      hasElseExpr = true;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    CaseExpr expr = (CaseExpr) obj;
    return hasCaseExpr == expr.hasCaseExpr && hasElseExpr == expr.hasElseExpr;
  }

  @Override
  public String toSqlImpl() {
    StringBuilder output = new StringBuilder("CASE");
    int childIdx = 0;
    if (hasCaseExpr) {
      output.append(" " + children.get(childIdx++).toSql());
    }
    while (childIdx + 2 <= children.size()) {
      output.append(" WHEN " + children.get(childIdx++).toSql());
      output.append(" THEN " + children.get(childIdx++).toSql());
    }
    if (hasElseExpr) {
      output.append(" ELSE " + children.get(children.size() - 1).toSql());
    }
    output.append(" END");
    return output.toString();
  }

  @Override
  protected void toThrift(TExprNode msg) {
    msg.node_type = TExprNodeType.CASE_EXPR;
    msg.case_expr = new TCaseExpr(hasCaseExpr, hasElseExpr);
    msg.setOpcode(opcode);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    // Keep track of maximum compatible type of case expr and all when exprs.
    PrimitiveType whenType = null;
    // Keep track of maximum compatible type of else expr and all then exprs.
    PrimitiveType returnType = null;
    // Remember last of these exprs for error reporting.
    Expr lastCompatibleThenExpr = null;
    Expr lastCompatibleWhenExpr = null;
    int loopEnd = children.size();
    if (hasElseExpr) {
      --loopEnd;
    }
    int loopStart;
    Expr caseExpr = null;
    // Set loop start, and initialize returnType as type of castExpr.
    if (hasCaseExpr) {
      loopStart = 1;
      caseExpr = children.get(0);
      caseExpr.analyze(analyzer);
      whenType = caseExpr.getType();
      lastCompatibleWhenExpr = children.get(0);
    } else {
      whenType = PrimitiveType.BOOLEAN;
      loopStart = 0;
    }

    // Go through when/then exprs and determine compatible types.
    for (int i = loopStart; i < loopEnd; i += 2) {
      Expr whenExpr = children.get(i);
      if (hasCaseExpr) {
        // Determine maximum compatible type of the case expr,
        // and all when exprs seen so far. We will add casts to them at the very end.
        whenType = analyzer.getCompatibleType(whenType,
            lastCompatibleWhenExpr, whenExpr);
        lastCompatibleWhenExpr = whenExpr;
      } else {
        // If no case expr was given, then the when exprs should always return
        // boolean or be castable to boolean.
        if (!PrimitiveType.isImplicitlyCastable(whenExpr.getType(),
            PrimitiveType.BOOLEAN)) {
          throw new AnalysisException("When expr '" + whenExpr.toSql() + "'" +
              " is not of type boolean and not castable to type boolean.");
        }
        // Add a cast if necessary.
        if (whenExpr.getType() != PrimitiveType.BOOLEAN) {
          castChild(PrimitiveType.BOOLEAN, i);
        }
      }
      // Determine maximum compatible type of the then exprs seen so far.
      // We will add casts to them at the very end.
      Expr thenExpr = children.get(i + 1);
      returnType = analyzer.getCompatibleType(returnType,
          lastCompatibleThenExpr, thenExpr);
      lastCompatibleThenExpr = thenExpr;
    }
    if (hasElseExpr) {
      Expr elseExpr = children.get(children.size() - 1);
      returnType = analyzer.getCompatibleType(returnType,
          lastCompatibleThenExpr, elseExpr);
    }

    // Add casts to case expr to compatible type.
    if (hasCaseExpr) {
      // Cast case expr.
      if (children.get(0).type != whenType) {
        castChild(whenType, 0);
      }
      // Add casts to when exprs to compatible type.
      for (int i = loopStart; i < loopEnd; i += 2) {
        if (children.get(i).type != whenType) {
          castChild(whenType, i);
        }
      }
    }
    // Cast then exprs to compatible type.
    for (int i = loopStart + 1; i < children.size(); i += 2) {
      if (children.get(i).type != returnType) {
        castChild(returnType, i);
      }
    }
    // Cast else expr to compatible type.
    if (hasElseExpr) {
      if (children.get(children.size() - 1).type != returnType) {
        castChild(returnType, children.size() - 1);
      }
    }

    // Set opcode based on whenType.
    OpcodeRegistry.Signature match = OpcodeRegistry.instance().getFunctionInfo(
        FunctionOperator.CASE, true, whenType);
    if (match == null) {
      throw new AnalysisException("Could not find match in function registry " +
          "for CASE and arg type: " + whenType);
    }
    opcode = match.opcode;
    type = returnType;
    isAnalyzed = true;
  }
}
