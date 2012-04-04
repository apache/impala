// Copyright (c) 2011 Cloudera, Inc. All rights reserved.  

package com.cloudera.impala.analysis;

import com.cloudera.impala.catalog.PrimitiveType;
import java.util.ArrayList;
import java.util.List;
import java_cup.runtime.Symbol;

parser code {:
  private Symbol errorToken;
  
  // list of expected tokens ids from current parsing state 
  // for generating syntax error message
  private final List<Integer> expectedTokenIds = new ArrayList<Integer>();
  
  // to avoid reporting trivial tokens as expected tokens in error messages
  private boolean reportExpectedToken(Integer tokenId) {
    if (SqlScanner.isKeyword(tokenId) || 
        tokenId.intValue() == SqlParserSymbols.COMMA ||
        tokenId.intValue() == SqlParserSymbols.IDENT) {
      return true;
    } else {
      return false;
    }
  }  
  
  private String getErrorTypeMessage(int lastTokenId) {
    String msg = null;
    switch(lastTokenId) {
      case SqlParserSymbols.UNMATCHED_STRING_LITERAL: 
        msg = "Unmatched string literal";
        break;    
      case SqlParserSymbols.NUMERIC_OVERFLOW:
        msg = "Numeric overflow";
        break;      
      default:
        msg = "Syntax error";
        break;
    }    
    return msg;
  }  
  
  // override to save error token
  public void syntax_error(java_cup.runtime.Symbol token) {
    errorToken = token;
    
    // derive expected tokens from current parsing state
    expectedTokenIds.clear();    
    int state = ((Symbol)stack.peek()).parse_state;    
    // get row of actions table corresponding to current parsing state    
    // the row consists of pairs of <tokenId, actionId> 
    // a pair is stored as row[i] (tokenId) and row[i+1] (actionId)
    // the last pair is a special error action
    short[] row = action_tab[state];
    short tokenId;
    // the expected tokens are all the symbols with a 
    // corresponding action from the current parsing state 
    for (int i = 0; i < row.length-2; ++i) {
      // get tokenId and skip actionId
      tokenId = row[i++];
      expectedTokenIds.add(Integer.valueOf(tokenId));
    }  
  }
  
  // override to keep it from calling report_fatal_error()
  public void unrecovered_syntax_error(Symbol cur_token)
    throws Exception {
    throw new Exception(getErrorTypeMessage(cur_token.sym));
  }
  
  // Returns error string, consisting of the original
  // stmt with a '^' under the offending token. Assumes
  // that parse() has been called and threw an exception  
  public String getErrorMsg(String stmt) {
    if (errorToken == null || stmt == null) return null;
    String[] lines = stmt.split("\n");
    StringBuffer result = new StringBuffer();
    result.append(getErrorTypeMessage(errorToken.sym) + " at:\n");

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
    
    // only report encountered and expected tokens for syntax errors
    if (errorToken.sym == SqlParserSymbols.UNMATCHED_STRING_LITERAL ||
        errorToken.sym == SqlParserSymbols.NUMERIC_OVERFLOW) {
      return result.toString();                    
    }
    
    // append last encountered token
    result.append("Encountered: ");
    String lastToken = 
      SqlScanner.tokenIdMap.get(Integer.valueOf(errorToken.sym));
    if (lastToken != null) {
      result.append(lastToken);
    } else {
      result.append("Unknown last token with id: " + errorToken.sym);
    }
    
    // append expected tokens
    result.append('\n');
    result.append("Expected: ");
    String expectedToken = null;
    Integer tokenId = null;
    for (int i = 0; i < expectedTokenIds.size(); ++i) {
      tokenId = expectedTokenIds.get(i);
      if (reportExpectedToken(tokenId)) {
       expectedToken = SqlScanner.tokenIdMap.get(tokenId);        
         result.append(expectedToken + ", ");        
      }
    }
    // remove trailing ", "
    result.delete(result.length()-2, result.length());    
    result.append('\n');
    
    return result.toString();
  }
:};

terminal KW_AND, KW_AS, KW_ASC, KW_AVG, KW_BIGINT, KW_BOOLEAN, KW_BY,
  KW_CASE, KW_CAST, KW_COUNT, KW_DATE, KW_DATETIME, KW_DESC, KW_DISTINCT,
  KW_DIV, KW_DOUBLE, KW_ELSE, KW_END, KW_FALSE, KW_FLOAT, KW_FROM, KW_FULL, KW_GROUP,
  KW_HAVING, KW_IS, KW_INNER, KW_JOIN, KW_INT, KW_LEFT, KW_LIKE, KW_LIMIT, KW_MIN,
  KW_MAX, KW_NOT, KW_NULL, KW_ON, KW_OR, KW_ORDER, KW_OUTER, KW_REGEXP,
  KW_RLIKE, KW_RIGHT, KW_SELECT, KW_SEMI, KW_SMALLINT, KW_STRING, KW_SUM,
  KW_TINYINT, KW_TRUE, KW_USING, KW_WHEN, KW_WHERE, KW_THEN, KW_TIMESTAMP,
  KW_INSERT, KW_INTO, KW_OVERWRITE, KW_TABLE, KW_PARTITION;
terminal COMMA, DOT, STAR, LPAREN, RPAREN, DIVIDE, MOD, ADD, SUBTRACT;
terminal BITAND, BITOR, BITXOR, BITNOT;
terminal EQUAL, NOT, LESSTHAN, GREATERTHAN;
terminal String IDENT;
terminal String NUMERIC_OVERFLOW;
terminal Boolean BOOL_LITERAL;
terminal Long INTEGER_LITERAL;
terminal Double FLOATINGPOINT_LITERAL;
terminal String STRING_LITERAL;
terminal String UNMATCHED_STRING_LITERAL;

nonterminal ParseNodeBase insert_or_select_stmt;
nonterminal SelectStmt select_stmt;
nonterminal SelectList select_clause;
nonterminal SelectList select_list;
nonterminal SelectListItem select_list_item;
nonterminal SelectListItem star_expr ;
nonterminal Expr expr, arithmetic_expr;
nonterminal ArrayList<Expr> expr_list;
nonterminal ArrayList<Expr> func_arg_list;
nonterminal String alias_clause;
nonterminal ArrayList<String> ident_list;
nonterminal TableName table_name;
nonterminal Predicate where_clause;
nonterminal Predicate predicate, comparison_predicate, compound_predicate,
  like_predicate;
nonterminal LiteralPredicate literal_predicate;
nonterminal ArrayList<Expr> group_by_clause;
nonterminal Predicate having_clause;
nonterminal ArrayList<OrderByElement> order_by_elements, order_by_clause;
nonterminal OrderByElement order_by_element;
nonterminal Number limit_clause;
nonterminal Expr cast_expr, case_else_clause, literal, aggregate_expr;
nonterminal CaseExpr case_expr;
nonterminal ArrayList<CaseWhenClause> case_when_clause_list;
nonterminal AggregateParamsList aggregate_param_list;
nonterminal AggregateExpr.Operator aggregate_operator;
nonterminal SlotRef column_ref;
nonterminal ArrayList<TableRef> from_clause, table_ref_list;
nonterminal TableRef table_ref;
nonterminal BaseTableRef base_table_ref;
nonterminal InlineViewRef inline_view_ref;
nonterminal JoinOperator join_operator;
nonterminal opt_inner, opt_outer;
nonterminal PrimitiveType primitive_type;
nonterminal Expr subtract_chain_expr;
nonterminal BinaryPredicate.Operator binary_comparison_operator;
nonterminal InsertStmt insert_stmt;
nonterminal ArrayList<PartitionKeyValue> partition_clause;
nonterminal ArrayList<PartitionKeyValue> partition_key_value_list;
nonterminal PartitionKeyValue partition_key_value;
nonterminal Expr expr_or_predicate;

precedence left KW_OR;
precedence left KW_AND;
precedence left KW_NOT;
precedence left KW_LIKE, KW_RLIKE, KW_REGEXP;
precedence left EQUAL, LESSTHAN, GREATERTHAN;
precedence left ADD, SUBTRACT;
precedence left STAR, DIVIDE, MOD, KW_DIV;
precedence left BITAND, BITOR, BITXOR, BITNOT;
precedence left RPAREN;

start with insert_or_select_stmt;

insert_or_select_stmt ::= 
    select_stmt:select
    {: RESULT = select; :}
    | insert_stmt:insert
    {: RESULT = insert; :}
    ;

insert_stmt ::=
    KW_INSERT KW_OVERWRITE KW_TABLE table_name:table partition_clause:list select_stmt:select
    {: RESULT = new InsertStmt(table, true, list, select); :}
    | KW_INSERT KW_INTO KW_TABLE table_name:table partition_clause:list select_stmt:select
    {: RESULT = new InsertStmt(table, false, list, select); :}
    ;
    
partition_clause ::=
    KW_PARTITION LPAREN partition_key_value_list:list RPAREN
    {: RESULT = list; :}
    | 
    {: RESULT = null; :}
    ;
    
partition_key_value_list ::=
  partition_key_value:item
  {:
    ArrayList<PartitionKeyValue> list = new ArrayList<PartitionKeyValue>();
    list.add(item);
    RESULT = list;
  :}
  | partition_key_value_list:list COMMA partition_key_value:item
  {:
    list.add(item);
    RESULT = list;
  :}
  ;

partition_key_value ::=
    // Dynamic partition key values.
    IDENT:column
    {: RESULT = new PartitionKeyValue(column, null); :}
    // Static partition key values.
    | IDENT:column EQUAL literal:value
    {: RESULT = new PartitionKeyValue(column, (LiteralExpr)value); :}
    // Static partition key value with NULL.
    | IDENT:column EQUAL KW_NULL
    {: RESULT = new PartitionKeyValue(column, new NullLiteral()); :}
    ;

select_stmt ::=
    select_clause:selectList
  {: RESULT = new SelectStmt(selectList, null, null, null, null, null, -1); :}
  |
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
  | KW_SELECT KW_DISTINCT select_list:l
  {:
    l.setIsDistinct(true);
    RESULT = l;
  :}  
  ;

select_list ::=
  select_list_item:item
  {:
    SelectList list = new SelectList();
    list.getItems().add(item);
    RESULT = list;
  :}
  | select_list:list COMMA select_list_item:item
  {:
    list.getItems().add(item);
    RESULT = list;
  :}    
  ;

select_list_item ::=
  expr:expr alias_clause:alias
  {: RESULT = new SelectListItem(expr, alias); :}
  | expr:expr
  {: RESULT = new SelectListItem(expr, null); :}
  // allow predicates in the select list
  | predicate:p alias_clause:alias
  {: RESULT = new SelectListItem(p, alias); :}
  | predicate:p
  {: RESULT = new SelectListItem(p, null); :}  
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
    t.setJoinOp((JoinOperator) op);
    list.add(t);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op table_ref:t
    KW_ON predicate:p
  {:
    t.setJoinOp((JoinOperator) op);
    t.setOnClause(p);
    list.add(t);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op table_ref:t
    KW_USING LPAREN ident_list:colNames RPAREN
  {:
    t.setJoinOp((JoinOperator) op);
    t.setUsingClause(colNames);
    list.add(t);
    RESULT = list;
  :}
  ;

table_ref ::=
  base_table_ref:b
  {: RESULT = b; :}
  | inline_view_ref:s
  {: RESULT = s; :}
  ;
  
inline_view_ref ::=
  LPAREN select_stmt:select RPAREN IDENT:alias
  {: RESULT = new InlineViewRef(alias, select); :}
  ;
  
base_table_ref ::=
  table_name:name IDENT:alias
  {: RESULT = new BaseTableRef(name, alias); :}
  | table_name:name
  {: RESULT = new BaseTableRef(name, null); :}
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
  KW_ORDER KW_BY order_by_elements:l
  {: RESULT = l; :}
  | /* empty */
  {: RESULT = null; :}
  ;

order_by_elements ::=
  order_by_element:e
  {:
    ArrayList<OrderByElement> list = new ArrayList<OrderByElement>();
    list.add(e);
    RESULT = list;
  :}
  | order_by_elements:list COMMA order_by_element:e
  {:
    list.add(e);
    RESULT = list;
  :}
  ;

order_by_element ::=
  expr:e
  {: RESULT = new OrderByElement(e, true); :}
  | expr:e KW_ASC
  {: RESULT = new OrderByElement(e, true); :}
  | expr:e KW_DESC
  {: RESULT = new OrderByElement(e, false); :}
  ;

limit_clause ::=
  KW_LIMIT INTEGER_LITERAL:l
  {: RESULT = l; :}
  | /* empty */
  {: RESULT = null; :}
  ;

cast_expr ::=
  KW_CAST LPAREN expr:e KW_AS primitive_type:targetType RPAREN
  {: RESULT = new CastExpr((PrimitiveType) targetType, e, false); :}
  ;

case_expr ::=
  KW_CASE expr_or_predicate:caseExpr
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
  KW_WHEN expr_or_predicate:whenExpr KW_THEN expr_or_predicate:thenExpr
  {:
    ArrayList<CaseWhenClause> list = new ArrayList<CaseWhenClause>();
    list.add(new CaseWhenClause(whenExpr, thenExpr));
    RESULT = list;
  :}
  | case_when_clause_list:list KW_WHEN expr_or_predicate:whenExpr 
    KW_THEN expr_or_predicate:thenExpr
  {:
    list.add(new CaseWhenClause(whenExpr, thenExpr));
    RESULT = list;
  :}
  ;
  
case_else_clause ::=
  KW_ELSE expr_or_predicate:e
  {: RESULT = e; :}
  | /* emtpy */
  {: RESULT = null; :}
  ;

expr_or_predicate ::=
  expr:e
  {: RESULT = e; :}
  | predicate:p
  {: RESULT = p; :}
  ;

subtract_chain_expr ::=    
  SUBTRACT expr:e
  {:      
    // integrate signs into literals 
    if (e.isLiteral() && e.getType().isNumericType()) {
      ((LiteralExpr)e).swapSign();
      RESULT = e;
    } else {
      RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY, new IntLiteral((long)-1), e);
    }
  :}
  ;

expr ::=
  subtract_chain_expr:e
  {: RESULT = e; :}  
  | literal:l
  {: RESULT = l; :}
  | IDENT:functionName LPAREN RPAREN
  {: RESULT = new FunctionCallExpr(functionName, new ArrayList<Expr>()); :}
  | IDENT:functionName LPAREN func_arg_list:exprs RPAREN
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

func_arg_list ::=
  // Function arguments can be exprs as well as predicates.
  expr_or_predicate:item
  {:
    ArrayList<Expr> list = new ArrayList<Expr>();
    list.add(item);
    RESULT = list;
  :}
  | func_arg_list:list COMMA expr_or_predicate:item
  {:
    list.add(item);
    RESULT = list;
  :}
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
  | expr:e1 ADD expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.ADD, e1, e2); :}
  | expr:e1 SUBTRACT expr:e2
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, e1, e2); :}     
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
  INTEGER_LITERAL:l
  {: RESULT = new IntLiteral(l); :}
  | FLOATINGPOINT_LITERAL:l
  {: RESULT = new FloatLiteral(l); :}
  | STRING_LITERAL:l
  {: RESULT = new StringLiteral(l); :}
  | BOOL_LITERAL:l
  {: RESULT = new BoolLiteral(l); :}
  | UNMATCHED_STRING_LITERAL:l expr:e
  {: 
    // we have an unmatched string literal.
    // to correctly report the root cause of this syntax error
    // we must force parsing to fail at this point,
    // and generate an unmatched string literal symbol 
    // to be passed as the last seen token in the
    // error handling routine (otherwise some other token could be reported)
    Symbol errorToken = parser.getSymbolFactory().newSymbol("literal",
        SqlParserSymbols.UNMATCHED_STRING_LITERAL,
        ((Symbol) CUP$SqlParser$stack.peek()),
        ((Symbol) CUP$SqlParser$stack.peek()), RESULT);
    // call syntax error to gather information about expected tokens, etc.
    // syntax_error does not throw an exception
    parser.syntax_error(errorToken);
    // unrecovered_syntax_error throws an exception and will terminate parsing
    parser.unrecovered_syntax_error(errorToken);
    RESULT = null;
  :}
  | NUMERIC_OVERFLOW:l
  {: 
    // similar to the unmatched string literal case
    // we must terminate parsing at this point
    // and generate a corresponding symbol to be reported
    Symbol errorToken = parser.getSymbolFactory().newSymbol("literal",
        SqlParserSymbols.NUMERIC_OVERFLOW,
        ((Symbol) CUP$SqlParser$stack.peek()),
        ((Symbol) CUP$SqlParser$stack.peek()), RESULT);
    // call syntax error to gather information about expected tokens, etc.
    // syntax_error does not throw an exception
    parser.syntax_error(errorToken);
    // unrecovered_syntax_error throws an exception and will terminate parsing
    parser.unrecovered_syntax_error(errorToken);    
    RESULT = null; 
  :}
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

binary_comparison_operator ::=
  EQUAL:op
  {: RESULT = BinaryPredicate.Operator.EQ; :}
  | NOT EQUAL:op
  {: RESULT = BinaryPredicate.Operator.NE; :}
  | LESSTHAN GREATERTHAN:op
  {: RESULT = BinaryPredicate.Operator.NE; :}
  | LESSTHAN EQUAL:op
  {: RESULT = BinaryPredicate.Operator.LE; :}
  | GREATERTHAN EQUAL:op
  {: RESULT = BinaryPredicate.Operator.GE; :}
  | LESSTHAN:op
  {: RESULT = BinaryPredicate.Operator.LT; :}
  | GREATERTHAN:op
  {: RESULT = BinaryPredicate.Operator.GT; :}
  ;

comparison_predicate ::=
  expr:e1 binary_comparison_operator:op expr:e2
  {: RESULT = new BinaryPredicate(op, e1, e2); :}  
  // A bool/null literal should be both an expr (to act as a BoolLiteral)
  // and a predicate (to act as a LiteralPredicate).
  // Implementing this directly will lead to shift-reduce conflicts.
  // We decided that a bool literal shall be literal predicate. 
  // This means we must list all combinations with bool literals in the ops below,
  // transforming the literal predicate to a literal expr.
  // We could have chosen the other way (bool literal as a literal expr), but
  // this would have required more and uglier code, 
  // e.g., a special-case rule for dealing with "where true/false".
  | expr:e1 binary_comparison_operator:op literal_predicate:l
  {: 
    Expr e2 = (l.isNull()) ? new NullLiteral() : new BoolLiteral(l.getValue());
    RESULT = new BinaryPredicate(op, e1, e2);  
  :}
  | literal_predicate:l binary_comparison_operator:op expr:e2
  {: 
    Expr e1 = (l.isNull()) ? new NullLiteral() : new BoolLiteral(l.getValue());
    RESULT = new BinaryPredicate(op, e1, e2);
  :}
  | literal_predicate:l1 binary_comparison_operator:op literal_predicate:l2
  {:     
    Expr e1 = (l1.isNull()) ? new NullLiteral() : new BoolLiteral(l1.getValue());
    Expr e2 = (l2.isNull()) ? new NullLiteral() : new BoolLiteral(l2.getValue());
    RESULT = new BinaryPredicate(op, e1, e2);
  :}
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
  | NOT predicate:p
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.NOT, p, null); :}
  ;

literal_predicate ::=
  KW_TRUE
  {: RESULT = LiteralPredicate.True(); :}
  | KW_FALSE
  {: RESULT = LiteralPredicate.False(); :}
  | KW_NULL
  {: RESULT = LiteralPredicate.Null(); :}
  ;

column_ref ::=
  IDENT:col
  {:
    RESULT = new SlotRef(null, col);
  :}
  // table_name:tblName DOT IDENT:col causes reduce/reduce conflicts
  | IDENT:tbl DOT IDENT:col
  {:
    RESULT = new SlotRef(new TableName(null, tbl), col);
  :}
  | IDENT:db DOT IDENT:tbl DOT IDENT:col
  {:
    RESULT = new SlotRef(new TableName(db, tbl), col);
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
