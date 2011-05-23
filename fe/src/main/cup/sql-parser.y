package com.cloudera.impala.parser;
//import "java.io.IOException", "java.lang.StringBuffer";
import java.util.ArrayList;

parser code {:
  private java_cup.runtime.Symbol errorToken;

  // override to save error token
  public void syntax_error(java_cup.runtime.Symbol token) {
    errorToken = token;
    done_parsing();
  }

  // override to keep it from calling report_fatal_error()
  public void unrecovered_syntax_error(java_cup.runtime.Symbol cur_token)
    throws java.lang.Exception {
    throw new java.lang.Exception("Syntax error");
  }
  
  // Returns error string, consisting of the original
  // stmt with a '^' under the offending token. Assumes
  // that parse() has been called and threw an exception
  public String getErrorMsg(String stmt) {
    if (errorToken == null || stmt == null) return null;
    String[] lines = stmt.split("\n");
    StringBuffer result = new StringBuffer();
    result.append("Syntax error at:\n");

    // print lines up to and including the one with the error
    for (int i = 0; i < errorToken.left; ++i) {
      result.append(lines[i]);
      result.append('\n');
    }
    // print error indicator
    for (int i = 0; i < errorToken.right - 1; ++i) {
      result.append(' ');
    }
    result.append("^\n");
    // print remaining lines
    for (int i = errorToken.left; i < lines.length; ++i) {
      result.append(lines[i]);
      result.append('\n');
    }

    return result.toString();
  }
:};

terminal KW_AND, KW_AS, KW_ASC, KW_AVG, KW_BIGINT, KW_BOOLEAN, KW_BY,
  KW_CASE, KW_CAST, KW_COUNT, KW_DATE, KW_DATETIME, KW_DESC, KW_DISTINCT,
  KW_DIV, KW_DOUBLE, KW_ELSE, KW_END, KW_FALSE, KW_FLOAT, KW_FROM, KW_FULL, KW_GROUP,
  KW_HAVING, KW_IS, KW_INNER, KW_JOIN, KW_INT, KW_LEFT, KW_LIKE, KW_LIMIT, KW_MIN,
  KW_MAX, KW_NOT, KW_NULL, KW_ON, KW_OR, KW_ORDER, KW_OUTER, KW_REGEXP,
  KW_RLIKE, KW_RIGHT, KW_SELECT, KW_SEMI, KW_SMALLINT, KW_STRING, KW_SUM,
  KW_TINYINT, KW_TRUE, KW_USING, KW_WHEN, KW_WHERE, KW_THEN, KW_TIMESTAMP;
terminal COMMA, DOT, STAR, LPAREN, RPAREN, DIVIDE, MOD, PLUS, MINUS, UNARY_MINUS;
terminal BITAND, BITOR, BITXOR, BITNOT;
terminal EQUAL, NOT, LESSTHAN, GREATERTHAN;
terminal String IDENT;
terminal String NUMERIC_OVERFLOW;
terminal Boolean BOOL_LITERAL;
terminal Number NUMERIC_LITERAL;
terminal String STRING_LITERAL;

nonterminal SelectStmt select_stmt;
nonterminal ArrayList<SelectListItem> select_clause;
nonterminal ArrayList<SelectListItem> select_list;
nonterminal SelectListItem select_list_item;
nonterminal SelectListItem star_expr ;
nonterminal Expr expr, arithmetic_expr;
nonterminal ArrayList<Expr> expr_list;
nonterminal String alias_clause;
nonterminal ArrayList<String> ident_list;
nonterminal TableName table_name;
nonterminal Predicate where_clause;
nonterminal Predicate predicate, comparison_predicate, compound_predicate,
  like_predicate, literal_predicate;
nonterminal ArrayList<Expr> group_by_clause;
nonterminal Predicate having_clause;
nonterminal ArrayList<OrderByExpr> order_by_expr_list, order_by_clause;
nonterminal OrderByExpr order_by_expr;
nonterminal Number limit_clause;
nonterminal Expr cast_expr, case_else_clause, literal, aggregate_expr;
nonterminal CaseExpr case_expr;
nonterminal ArrayList<CaseWhenClause> case_when_clause_list;
nonterminal AggregateParamsList aggregate_param_list;
nonterminal AggregateExpr.Operator aggregate_operator;
nonterminal SlotRef column_ref;
nonterminal ArrayList<TableRef> from_clause, table_ref_list;
nonterminal TableRef table_ref;
nonterminal JoinOperator join_operator;
nonterminal opt_inner, opt_outer;
nonterminal PrimitiveType primitive_type;

precedence left KW_OR;
precedence left KW_AND;
precedence left KW_NOT;
precedence left KW_LIKE, KW_RLIKE, KW_REGEXP;
precedence left EQUAL, LESSTHAN, GREATERTHAN;
precedence left PLUS, MINUS;
precedence left STAR, DIVIDE, MOD, KW_DIV;
precedence left BITAND, BITOR, BITXOR, BITNOT;
precedence left UNARY_MINUS;
precedence left RPAREN;

start with select_stmt;

select_stmt ::=
    select_clause:selectList
    from_clause:tableRefList
    where_clause:wherePredicate
    group_by_clause:groupingExprs
    having_clause:havingPredicate
    order_by_clause:orderByClause
    limit_clause:limitClause
  {:
    RESULT = new SelectStmt(selectList, tableRefList, wherePredicate,
                            groupingExprs, havingPredicate, orderByClause,
                            (limitClause == null ? -1 : limitClause.longValue()));
  :}
  ;

select_clause ::=
  KW_SELECT select_list:l
  {: RESULT = l; :}
  ;

select_list ::=
  select_list_item:item
  {:
    ArrayList<SelectListItem> list = new ArrayList<SelectListItem>();
    list.add(item);
    RESULT = list;
  :}
  | select_list:list COMMA select_list_item:item
  {:
    list.add(item);
    RESULT = list;
  :}
  ;

select_list_item ::=
  expr:expr alias_clause:alias
  {: RESULT = new SelectListItem(expr, alias); :}
  | expr:expr
  {: RESULT = new SelectListItem(expr, null); :}
  | star_expr:expr
  {: RESULT = expr; :}
  ;
  
alias_clause ::=
  KW_AS IDENT:ident
  {: RESULT = ident; :}
  | IDENT:ident
  {: RESULT = ident; :}
  ;

star_expr ::=
  STAR
  // table_name DOT STAR doesn't work because of a reduce-reduce conflict 
  // on IDENT [DOT]
  {:
    RESULT = SelectListItem.createStarItem(null);
  :}
  | IDENT:tbl DOT STAR
  {: RESULT = SelectListItem.createStarItem(new TableName(null, tbl)); :}
  | IDENT:db DOT IDENT:tbl DOT STAR
  {: RESULT = SelectListItem.createStarItem(new TableName(db, tbl)); :}
  ;

table_name ::=
  IDENT:tbl
  {: RESULT = new TableName(null, tbl); :}
  | IDENT:db DOT IDENT:tbl
  {: RESULT = new TableName(db, tbl); :}
  ;

from_clause ::=
  KW_FROM table_ref_list:l
  {: RESULT = l; :}
  ;

table_ref_list ::=
  table_ref:t
  {:
    ArrayList<TableRef> list = new ArrayList<TableRef>();
    list.add(t);
    RESULT = list;
  :}
  | table_ref_list:list COMMA table_ref:t
  {:
    list.add(t);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op table_ref:t
  {:
    t.setJoinOperator((JoinOperator) op);
    list.add(t);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op table_ref:t
    KW_ON predicate:p
  {:
    t.setJoinOperator((JoinOperator) op);
    t.setOnClause(p);
    list.add(t);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op table_ref:t
    KW_USING LPAREN ident_list:colNames RPAREN
  {:
    t.setJoinOperator((JoinOperator) op);
    t.setUsingClause(colNames);
    list.add(t);
    RESULT = list;
  :}
  ;

table_ref ::=
  table_name:name IDENT:alias
  {: RESULT = new TableRef(name, alias); :}
  | table_name:name
  {: RESULT = new TableRef(name, null); :}
  ;

join_operator ::=
  opt_inner KW_JOIN
  {: RESULT = JoinOperator.INNER_JOIN; :}
  | KW_LEFT opt_outer KW_JOIN
  {: RESULT = JoinOperator.LEFT_OUTER_JOIN; :}
  | KW_RIGHT opt_outer KW_JOIN
  {: RESULT = JoinOperator.RIGHT_OUTER_JOIN; :}
  | KW_FULL opt_outer KW_JOIN
  {: RESULT = JoinOperator.FULL_OUTER_JOIN; :}
  | KW_LEFT KW_SEMI KW_JOIN
  {: RESULT = JoinOperator.LEFT_SEMI_JOIN; :}
  ;

opt_inner ::=
  KW_INNER
  |
  ;

opt_outer ::=
  KW_OUTER
  |
  ;

ident_list ::=
  IDENT:ident
  {:
    ArrayList<String> list = new ArrayList<String>();
    list.add(ident);
    RESULT = list;
  :}
  | ident_list:list COMMA IDENT:ident
  {:
    list.add(ident);
    RESULT = list;
  :}
  ;

expr_list ::=
  expr:e
  {:
    ArrayList<Expr> list = new ArrayList<Expr>();
    list.add(e);
    RESULT = list;
  :}
  | expr_list:list COMMA expr:e
  {:
    list.add(e);
    RESULT = list;
  :}
  ;

where_clause ::=
  KW_WHERE predicate:p
  {: RESULT = p; :}
  | /* empty */
  {: RESULT = null; :}
  ;

group_by_clause ::=
  KW_GROUP KW_BY expr_list:l
  {: RESULT = l; :}
  | /* empty */
  {: RESULT = null; :}
  ;

having_clause ::=
  KW_HAVING predicate:p
  {: RESULT = p; :}
  | /* empty */
  {: RESULT = null; :}
  ;

order_by_clause ::=
  KW_ORDER KW_BY order_by_expr_list:l
  {: RESULT = l; :}
  | /* empty */
  {: RESULT = null; :}
  ;

order_by_expr_list ::=
  order_by_expr:e
  {:
    ArrayList<OrderByExpr> list = new ArrayList<OrderByExpr>();
    list.add(e);
    RESULT = list;
  :}
  | order_by_expr_list:list COMMA order_by_expr:e
  {:
    list.add(e);
    RESULT = list;
  :}
  ;

order_by_expr ::=
  expr:e
  {: RESULT = new OrderByExpr(e, true); :}
  | expr:e KW_ASC
  {: RESULT = new OrderByExpr(e, true); :}
  | expr:e KW_DESC
  {: RESULT = new OrderByExpr(e, false); :}
  ;

limit_clause ::=
  KW_LIMIT NUMERIC_LITERAL:l
  {: RESULT = l; :}
  | /* empty */
  {: RESULT = null; :}
  ;

cast_expr ::=
  KW_CAST LPAREN expr:e KW_AS primitive_type:targetType RPAREN
  {: RESULT = new CastExpr((PrimitiveType) targetType, e); :}
  ;

case_expr ::=
  KW_CASE expr:caseExpr
    case_when_clause_list:whenClauseList
    case_else_clause:elseExpr
    KW_END
  {: RESULT = new CaseExpr(caseExpr, whenClauseList, elseExpr); :}
  | KW_CASE
    case_when_clause_list:whenClauseList
    case_else_clause:elseExpr
    KW_END
  {: RESULT = new CaseExpr(null, whenClauseList, elseExpr); :}
  ;

case_when_clause_list ::=
  KW_WHEN expr:whenExpr KW_THEN expr:thenExpr
  {:
    ArrayList<CaseWhenClause> list = new ArrayList<CaseWhenClause>();
    list.add(new CaseWhenClause(whenExpr, thenExpr));
    RESULT = list;
  :}
  | case_when_clause_list:list KW_WHEN expr:whenExpr KW_THEN expr:thenExpr
  {:
    list.add(new CaseWhenClause(whenExpr, thenExpr));
    RESULT = list;
  :}
  ;

case_else_clause ::=
  KW_ELSE expr:e
  {: RESULT = e; :}
  | /* emtpy */
  {: RESULT = null; :}
  ;

expr ::=
  literal:l
  {: RESULT = l; :}
  | IDENT:functionName LPAREN expr_list:exprs RPAREN
  {: RESULT = new FunctionCallExpr(functionName, exprs); :}
  | cast_expr:c
  {: RESULT = c; :}
  | case_expr:c
  {: RESULT = c; :}
  | aggregate_expr:a
  {: RESULT = a; :}
  | column_ref:c
  {: RESULT = c; :}
  | arithmetic_expr:e
  {: RESULT = e; :}
  | LPAREN expr:e RPAREN
  {: RESULT = e; :}
  ;

arithmetic_expr ::=
  expr:e1 STAR expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, e1, e2); :}
  | expr:e1 DIVIDE expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.DIVIDE, e1, e2); :}
  | expr:e1 MOD expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.MOD, e1, e2); :}
  | expr:e1 KW_DIV expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.INT_DIVIDE, e1, e2); :}
  | expr:e1 PLUS expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.PLUS, e1, e2); :}
  | expr:e1 MINUS expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.MINUS, e1, e2); :}
  | MINUS expr:e
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY,
                               LiteralExpr.createNumericLiteral(-1), e);
  :} %prec UNARY_MINUS
  | expr:e1 BITAND expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.BITAND, e1, e2); :}
  | expr:e1 BITOR expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.BITOR, e1, e2); :}
  | expr:e1 BITXOR expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.BITXOR, e1, e2); :}
  | BITNOT expr:e
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.BITNOT, e, null); :}
  ;

literal ::=
  NUMERIC_LITERAL:l
  {: RESULT = LiteralExpr.createNumericLiteral(l); :}
  | NUMERIC_OVERFLOW:l
  {: RESULT = LiteralExpr.createStringLiteral(l); :}
  | STRING_LITERAL:l
  {: RESULT = LiteralExpr.createStringLiteral(l); :}
  | BOOL_LITERAL:l
  {: RESULT = LiteralExpr.createBoolLiteral(l); :}
  ;

aggregate_expr ::=
  aggregate_operator:op LPAREN aggregate_param_list:params RPAREN
  {:
    RESULT = new AggregateExpr((AggregateExpr.Operator) op,
        params.isStar(), params.isDistinct(), params.exprs());
  :}
  ;

aggregate_operator ::=
  KW_COUNT
  {: RESULT = AggregateExpr.Operator.COUNT; :}
  | KW_MIN
  {: RESULT = AggregateExpr.Operator.MIN; :}
  | KW_MAX
  {: RESULT = AggregateExpr.Operator.MAX; :}
  | KW_SUM
  {: RESULT = AggregateExpr.Operator.SUM; :}
  | KW_AVG
  {: RESULT = AggregateExpr.Operator.AVG; :}
  ;

aggregate_param_list ::=
  STAR
  {: RESULT = AggregateParamsList.createStarParam(); :}
  | expr_list:exprs
  {: RESULT = new AggregateParamsList(false, exprs); :}
  | KW_DISTINCT:distinct expr_list:exprs
  {: RESULT = new AggregateParamsList(true, exprs); :}
  ;

predicate ::=
  expr:e KW_IS KW_NULL
  {: RESULT = new IsNullPredicate(e, false); :}
  | expr:e KW_IS KW_NOT KW_NULL
  {: RESULT = new IsNullPredicate(e, true); :}
  | comparison_predicate:p
  {: RESULT = p; :}
  | compound_predicate:p
  {: RESULT = p; :}
  | like_predicate:p
  {: RESULT = p; :}
  | literal_predicate:p
  {: RESULT = p; :}
  | LPAREN predicate:p RPAREN
  {: RESULT = p; :}
  ;

comparison_predicate ::=
  expr:e1 EQUAL expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.EQ, e1, e2); :}
  | expr:e1 NOT EQUAL expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.NE, e1, e2); :}
  | expr:e1 LESSTHAN GREATERTHAN expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.NE, e1, e2); :}
  | expr:e1 LESSTHAN EQUAL expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.LE, e1, e2); :}
  | expr:e1 GREATERTHAN EQUAL expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.GE, e1, e2); :}
  | expr:e1 LESSTHAN expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.LT, e1, e2); :}
  | expr:e1 GREATERTHAN expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.GT, e1, e2); :}
  ;

like_predicate ::=
  expr:e1 KW_LIKE expr:e2
  {: RESULT = new LikePredicate(LikePredicate.Operator.LIKE, e1, e2); :}
  | expr:e1 KW_RLIKE expr:e2
  {: RESULT = new LikePredicate(LikePredicate.Operator.RLIKE, e1, e2); :}
  | expr:e1 KW_REGEXP expr:e2
  {: RESULT = new LikePredicate(LikePredicate.Operator.REGEXP, e1, e2); :}
  ;

compound_predicate ::=
  predicate:p1 KW_AND predicate:p2
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.AND, p1, p2); :}
  | predicate:p1 KW_OR predicate:p2
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.OR, p1, p2); :}
  | KW_NOT predicate:p
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.NOT, p, null); :}
  ;

literal_predicate ::=
  KW_TRUE
  {: RESULT = LiteralPredicate.True(); :}
  | KW_FALSE
  {: RESULT = LiteralPredicate.False(); :}
  ;

column_ref ::=
  IDENT:col
  {:
    RESULT = new SlotRef(null, col);
  :}
  | IDENT:tbl DOT IDENT:col
  {:
    RESULT = new SlotRef(tbl, col);
  :}
  ;

primitive_type ::=
  KW_TINYINT
  {: RESULT = PrimitiveType.TINYINT; :}
  | KW_SMALLINT
  {: RESULT = PrimitiveType.SMALLINT; :}
  | KW_INT
  {: RESULT = PrimitiveType.INT; :}
  | KW_BIGINT
  {: RESULT = PrimitiveType.BIGINT; :}
  | KW_BOOLEAN
  {: RESULT = PrimitiveType.BOOLEAN; :}
  | KW_FLOAT
  {: RESULT = PrimitiveType.FLOAT; :}
  | KW_DOUBLE
  {: RESULT = PrimitiveType.DOUBLE; :}
  | KW_DATE
  {: RESULT = PrimitiveType.DATE; :}
  | KW_DATETIME
  {: RESULT = PrimitiveType.DATETIME; :}
  | KW_TIMESTAMP
  {: RESULT = PrimitiveType.TIMESTAMP; :}
  | KW_STRING
  {: RESULT = PrimitiveType.STRING; :}
  ;
