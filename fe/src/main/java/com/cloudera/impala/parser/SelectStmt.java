package com.cloudera.impala.parser;

import java.lang.String;
import java.util.ArrayList;

class SelectStmt {
  SelectStmt(ArrayList<SelectListItem> select_list,
             ArrayList<TableRef> tableRefList,
             Predicate wherePredicate, ArrayList<Expr> groupingExprs,
             Predicate havingPredicate, ArrayList<OrderByExpr> orderByExprs,
             long limit) {
    super();
  }
}
