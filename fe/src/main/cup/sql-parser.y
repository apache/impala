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

import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.analysis.UnionStmt.Qualifier;
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
  @Override
  public void unrecovered_syntax_error(Symbol cur_token)
      throws Exception {
    throw new Exception(getErrorTypeMessage(cur_token.sym));
  }

  /**
   * Manually throw a parse error on a given symbol for special circumstances.
   *
   * @symbolName
   *   name of symbol on which to fail parsing
   * @symbolId
   *   id of symbol from SqlParserSymbols on which to fail parsing
   */
  public void parseError(String symbolName, int symbolId) throws Exception {
    Symbol errorToken = getSymbolFactory().newSymbol(symbolName, symbolId,
        ((Symbol) stack.peek()), ((Symbol) stack.peek()), null);
    // Call syntax error to gather information about expected tokens, etc.
    // syntax_error does not throw an exception
    syntax_error(errorToken);
    // Unrecovered_syntax_error throws an exception and will terminate parsing
    unrecovered_syntax_error(errorToken);
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

terminal KW_AND, KW_ALL, KW_AS, KW_ASC, KW_AVG, KW_BETWEEN, KW_BIGINT, KW_BOOLEAN, KW_BY,
  KW_CASE, KW_CAST, KW_CREATE, KW_COMMENT, KW_COUNT, KW_DATABASE, KW_DATABASES, KW_DATE,
  KW_DATETIME, KW_DESC, KW_DESCRIBE, KW_DISTINCT, KW_DISTINCTPC, KW_DISTINCTPCSA, KW_DIV,
  KW_DELIMITED, KW_DOUBLE, KW_DROP, KW_ELSE, KW_END, KW_EXISTS, KW_EXTERNAL, KW_FALSE,
  KW_FIELDS, KW_FLOAT, KW_FORMAT, KW_FROM, KW_FULL, KW_GROUP, KW_HAVING, KW_IF, KW_IS,
  KW_IN, KW_INNER, KW_JOIN, KW_INT, KW_LEFT, KW_LIKE, KW_LIMIT, KW_LINES, KW_LOCATION,
  KW_MIN, KW_MAX, KW_NOT, KW_NULL, KW_ON, KW_OR, KW_ORDER, KW_OUTER, KW_PARQUETFILE,
  KW_PARTITIONED, KW_RCFILE, KW_REGEXP, KW_RLIKE, KW_RIGHT, KW_ROW, KW_SCHEMA, KW_SCHEMAS,
  KW_SELECT, KW_SEQUENCEFILE, KW_SHOW, KW_SEMI, KW_SMALLINT, KW_STORED, KW_STRING, KW_SUM,
  KW_TABLES, KW_TERMINATED, KW_TINYINT, KW_TRUE, KW_UNION, KW_USE, KW_USING, KW_WHEN,
  KW_WHERE, KW_TEXTFILE, KW_THEN, KW_TIMESTAMP, KW_INSERT, KW_INTO, KW_OVERWRITE,
  KW_TABLE, KW_PARTITION, KW_INTERVAL;
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

nonterminal ParseNodeBase stmt;
// Single select statement.
nonterminal SelectStmt select_stmt;
// Select or union statement.
nonterminal QueryStmt query_stmt;
// Single select_stmt or parenthesized query_stmt.
nonterminal QueryStmt union_operand;
// List of select or union blocks connected by UNION operators or a single select block.
nonterminal List<UnionOperand> union_operand_list;
// USE stmt
nonterminal UseStmt use_stmt;
nonterminal ShowTablesStmt show_tables_stmt;
nonterminal ShowDbsStmt show_dbs_stmt;
nonterminal String show_pattern;
nonterminal DescribeStmt describe_stmt;
// List of select blocks connected by UNION operators, with order by or limit.
nonterminal QueryStmt union_with_order_by_or_limit;
nonterminal SelectList select_clause;
nonterminal SelectList select_list;
nonterminal SelectListItem select_list_item;
nonterminal SelectListItem star_expr ;
nonterminal Expr expr, arithmetic_expr, timestamp_arithmetic_expr;
nonterminal ArrayList<Expr> expr_list;
nonterminal ArrayList<Expr> func_arg_list;
nonterminal String alias_clause;
nonterminal ArrayList<String> ident_list;
nonterminal TableName table_name;
nonterminal Predicate where_clause;
nonterminal Predicate predicate, between_predicate, comparison_predicate,
  compound_predicate, in_predicate, like_predicate;
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
nonterminal Expr sign_chain_expr;
nonterminal BinaryPredicate.Operator binary_comparison_operator;
nonterminal InsertStmt insert_stmt;
nonterminal ArrayList<PartitionKeyValue> partition_clause;
nonterminal ArrayList<PartitionKeyValue> partition_key_value_list;
nonterminal PartitionKeyValue partition_key_value;
nonterminal Expr expr_or_predicate;
nonterminal Qualifier union_op;

nonterminal DropDbStmt drop_db_stmt;
nonterminal DropTableStmt drop_tbl_stmt;
nonterminal CreateDbStmt create_db_stmt;
nonterminal CreateTableLikeStmt create_tbl_like_stmt;
nonterminal CreateTableStmt create_tbl_stmt;
nonterminal ColumnDef column_def;
nonterminal ArrayList<ColumnDef> column_def_list;
nonterminal ArrayList<ColumnDef> partition_column_defs;
// Options for CREATE DATABASE/TABLE
nonterminal String comment_val;
nonterminal Boolean external_val;
nonterminal FileFormat file_format_val;
nonterminal Boolean if_exists_val;
nonterminal Boolean if_not_exists_val;
nonterminal String location_val;
nonterminal RowFormat row_format_val;
nonterminal String terminator_val;
// Used to simplify commands that accept either KW_DATABASE(S) or KW_SCHEMA(S)
nonterminal String db_or_schema_kw;
nonterminal String dbs_or_schemas_kw;

precedence left KW_OR;
precedence left KW_AND;
precedence left KW_NOT;
precedence left KW_LIKE, KW_RLIKE, KW_REGEXP;
precedence left EQUAL, LESSTHAN, GREATERTHAN;
precedence left ADD, SUBTRACT;
precedence left STAR, DIVIDE, MOD, KW_DIV;
precedence left BITAND, BITOR, BITXOR, BITNOT;
precedence left KW_ORDER, KW_BY, KW_LIMIT;
precedence left RPAREN;
precedence left KW_IN;
// Support chaining of timestamp arithmetic exprs.
precedence left KW_INTERVAL;

start with stmt;

stmt ::=
  query_stmt:query
  {: RESULT = query; :}
  | insert_stmt:insert
  {: RESULT = insert; :}
  | use_stmt:use
  {: RESULT = use; :}
  | show_tables_stmt:show_tables
  {: RESULT = show_tables; :}
  | show_dbs_stmt:show_dbs
  {: RESULT = show_dbs; :}
  | describe_stmt:describe
  {: RESULT = describe; :}
  | create_tbl_like_stmt:create_tbl_like
  {: RESULT = create_tbl_like; :}
  | create_tbl_stmt:create_tbl
  {: RESULT = create_tbl; :}
  | create_db_stmt:create_db
  {: RESULT = create_db; :}
  | drop_db_stmt:drop_db
  {: RESULT = drop_db; :}
  | drop_tbl_stmt:drop_tbl
  {: RESULT = drop_tbl; :}
  ;

insert_stmt ::=
  KW_INSERT KW_OVERWRITE KW_TABLE table_name:table
  partition_clause:list query_stmt:query
  {: RESULT = new InsertStmt(table, true, list, query); :}
  | KW_INSERT KW_INTO KW_TABLE table_name:table
  partition_clause:list query_stmt:query
  {: RESULT = new InsertStmt(table, false, list, query); :}
  ;

create_db_stmt ::=
  KW_CREATE db_or_schema_kw if_not_exists_val:if_not_exists IDENT:db_name
  comment_val:comment location_val:location
  {: RESULT = new CreateDbStmt(db_name, comment, location, if_not_exists); :}
  ;

create_tbl_like_stmt ::=
  KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
  table_name:table KW_LIKE table_name:other_table location_val:location
  {:
    RESULT = new CreateTableLikeStmt(table, other_table, external, location,
        if_not_exists);
  :} 
  ;

create_tbl_stmt ::=
  KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
  table_name:table LPAREN column_def_list:col_defs RPAREN
  partition_column_defs:partition_col_defs comment_val:comment
  row_format_val:row_format file_format_val:file_format location_val:location
  {:
    RESULT = new CreateTableStmt(table, col_defs, partition_col_defs, external, comment,
        row_format, file_format, location, if_not_exists);
  :}
  ;

comment_val ::=
  KW_COMMENT STRING_LITERAL:comment
  {: RESULT = comment; :}
  | /* empty */
  {: RESULT = null; :}
  ;

location_val ::=
  KW_LOCATION STRING_LITERAL:location
  {: RESULT = location; :}
  | /* empty */
  {: RESULT = null; :}
  ;

external_val ::=
  KW_EXTERNAL
  {: RESULT = true; :}
  |
  {: RESULT = false; :}
  ;

if_not_exists_val ::=
  KW_IF KW_NOT KW_EXISTS
  {: RESULT = true; :}
  |
  {: RESULT = false; :}
  ;

row_format_val ::=
  KW_ROW KW_FORMAT KW_DELIMITED KW_FIELDS terminator_val:field_terminator
  {: RESULT = new RowFormat(field_terminator, null); :}
  |
  KW_ROW KW_FORMAT KW_DELIMITED KW_LINES terminator_val:line_terminator
  {: RESULT = new RowFormat(null, line_terminator); :}
  |
  KW_ROW KW_FORMAT KW_DELIMITED KW_FIELDS terminator_val:field_terminator
  KW_LINES terminator_val:line_terminator
  {: RESULT = new RowFormat(field_terminator, line_terminator); :}
  |
  KW_ROW KW_FORMAT KW_DELIMITED
  {: RESULT = RowFormat.DEFAULT_ROW_FORMAT; :}
  |/* empty */
  {: RESULT = RowFormat.DEFAULT_ROW_FORMAT; :}
  ;

terminator_val ::=
  KW_TERMINATED KW_BY STRING_LITERAL:terminator
  {: RESULT = terminator; :}
  | /* empty */
  {: RESULT = null; :}
  ;

file_format_val ::=
  KW_STORED KW_AS KW_PARQUETFILE
  {: RESULT = FileFormat.PARQUETFILE; :}
  | KW_STORED KW_AS KW_TEXTFILE
  {: RESULT = FileFormat.TEXTFILE; :}
  | KW_STORED KW_AS KW_SEQUENCEFILE
  {: RESULT = FileFormat.SEQUENCEFILE; :}
  | KW_STORED KW_AS KW_RCFILE
  {: RESULT = FileFormat.RCFILE; :}
  | /* empty - default to TEXTFILE */
  {: RESULT = FileFormat.TEXTFILE; :}
  ;

partition_column_defs ::=
  KW_PARTITIONED KW_BY LPAREN column_def_list:col_defs RPAREN
  {: RESULT = col_defs; :}
  | /* Empty - not a partitioned table */
  {: RESULT = new ArrayList<ColumnDef>(); :}
  ;

column_def_list ::=
  column_def:col_def
  {:
    ArrayList<ColumnDef> list = new ArrayList<ColumnDef>();
    list.add(col_def);
    RESULT = list;
  :}
  | column_def_list:list COMMA column_def:col_def
  {:
    list.add(col_def);
    RESULT = list;
  :}
  ;

column_def ::=
  IDENT:col_name primitive_type:targetType comment_val:comment
  {: RESULT = new ColumnDef(col_name, targetType, comment); :}
  ;

drop_db_stmt ::=
  KW_DROP db_or_schema_kw if_exists_val:if_exists IDENT:db_name
  {: RESULT = new DropDbStmt(db_name, if_exists); :}
  ;

drop_tbl_stmt ::=
  | KW_DROP KW_TABLE if_exists_val:if_exists table_name:table
  {: RESULT = new DropTableStmt(table, if_exists); :}
  ;

db_or_schema_kw ::=
  KW_DATABASE
  | KW_SCHEMA
  ;

dbs_or_schemas_kw ::=
  KW_DATABASES
  | KW_SCHEMAS
  ;

if_exists_val ::=
  KW_IF KW_EXISTS
  {: RESULT = true; :}
  |
  {: RESULT = false; :}
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


// Our parsing of UNION is slightly different from MySQL's:
// http://dev.mysql.com/doc/refman/5.5/en/union.html
//
// Imo, MySQL's parsing of union is not very clear.
// For example, MySQL cannot parse this query:
// select 3 order by 1 limit 1 union all select 1;
//
// On the other hand, MySQL does parse this query, but associates
// the order by and limit with the union, not the select:
// select 3 as g union all select 1 order by 1 limit 2;
//
// MySQL also allows some combinations of select blocks
// with and without parenthesis, but also disallows others.
//
// Our parsing:
// Select blocks may or may not be in parenthesis,
// even if the union has order by and limit.
// ORDER BY and LIMIT bind to the preceding select statement by default.
query_stmt ::=
  union_operand_list:operands
  {:
    QueryStmt queryStmt = null;
    if (operands.size() == 1) {
      queryStmt = operands.get(0).getQueryStmt();
    } else {
      queryStmt = new UnionStmt(operands, null, -1);
    }
    RESULT = queryStmt;
  :}
  | union_with_order_by_or_limit:union
  {: RESULT = union; :}
  ;

// We must have a non-empty order by or limit for them to bind to the union.
// We cannot reuse the existing order_by_clause or
// limit_clause because they would introduce conflicts with EOF,
// which, unfortunately, cannot be accessed in the parser as a nonterminal
// making this issue unresolvable.
// We rely on the left precedence of KW_ORDER, KW_BY, and KW_LIMIT,
// to resolve the ambiguity with select_stmt in favor of select_stmt
// (i.e., ORDER BY and LIMIT bind to the select_stmt by default, and not the union).
// There must be at least two union operands for ORDER BY or LIMIT to bind to a union,
// and we manually throw a parse error if we reach this production
// with only a single operand.
union_with_order_by_or_limit ::=
    union_operand_list:operands
    KW_ORDER KW_BY order_by_elements:orderByClause
  {:
    if (operands.size() == 1) {
      parser.parseError("order", SqlParserSymbols.KW_ORDER);
    }
    RESULT = new UnionStmt(operands, orderByClause, -1);
  :}
  |
    union_operand_list:operands
    KW_LIMIT INTEGER_LITERAL:limitClause
  {:
    if (operands.size() == 1) {
      parser.parseError("limit", SqlParserSymbols.KW_LIMIT);
    }
    RESULT = new UnionStmt(operands, null, limitClause.longValue());
  :}
  |
    union_operand_list:operands
    KW_ORDER KW_BY order_by_elements:orderByClause
    KW_LIMIT INTEGER_LITERAL:limitClause
  {:
    if (operands.size() == 1) {
      parser.parseError("order", SqlParserSymbols.KW_ORDER);
    }
    RESULT = new UnionStmt(operands, orderByClause, limitClause.longValue());
  :}
  ;

union_operand ::=
  select_stmt:select
  {:
    RESULT = select;
  :}
  | LPAREN query_stmt:query RPAREN
  {:
    RESULT = query;
  :}
  ;

union_operand_list ::=
  union_operand:operand
  {:
    List<UnionOperand> operands = new ArrayList<UnionOperand>();
    operands.add(new UnionOperand(operand, null));
    RESULT = operands;
  :}
  | union_operand_list:operands union_op:op union_operand:operand
  {:
    operands.add(new UnionOperand(operand, op));
    RESULT = operands;
  :}
  ;

union_op ::=
  KW_UNION
  {: RESULT = Qualifier.DISTINCT; :}
  | KW_UNION KW_DISTINCT
  {: RESULT = Qualifier.DISTINCT; :}
  | KW_UNION KW_ALL
  {: RESULT = Qualifier.ALL; :}
  ;

use_stmt ::=
  KW_USE IDENT:db
  {: RESULT = new UseStmt(db); :}
  ;

show_tables_stmt ::=
  KW_SHOW KW_TABLES
  {: RESULT = new ShowTablesStmt(); :}
  | KW_SHOW KW_TABLES show_pattern:showPattern
  {: RESULT = new ShowTablesStmt(showPattern); :}
  | KW_SHOW KW_TABLES KW_IN IDENT:db
  {: RESULT = new ShowTablesStmt(db, null); :}
  | KW_SHOW KW_TABLES KW_IN IDENT:db show_pattern:showPattern
  {: RESULT = new ShowTablesStmt(db, showPattern); :}
  ;

show_dbs_stmt ::=
  KW_SHOW dbs_or_schemas_kw
  {: RESULT = new ShowDbsStmt(); :}
  | KW_SHOW dbs_or_schemas_kw show_pattern:showPattern
  {: RESULT = new ShowDbsStmt(showPattern); :}
  ;

show_pattern ::=
  STRING_LITERAL:showPattern
  {: RESULT = showPattern; :}
  | KW_LIKE STRING_LITERAL:showPattern
  {: RESULT = showPattern; :}
  ;

describe_stmt ::=
  KW_DESCRIBE table_name:table
  {: RESULT = new DescribeStmt(table); :}
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
  | KW_SELECT KW_ALL select_list:l
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
  LPAREN query_stmt:query RPAREN alias_clause:alias
  {: RESULT = new InlineViewRef(alias, query); :}
  ;

base_table_ref ::=
  table_name:name alias_clause:alias
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

sign_chain_expr ::=
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
  | ADD expr:e
  {: RESULT = e; :}
  ;

expr ::=
  sign_chain_expr:e
  {: RESULT = e; :}
  | literal:l
  {: RESULT = l; :}
  | IDENT:functionName LPAREN RPAREN
  {: RESULT = new FunctionCallExpr(functionName, new ArrayList<Expr>()); :}
  | IDENT:functionName LPAREN func_arg_list:exprs RPAREN
  {: RESULT = new FunctionCallExpr(functionName, exprs); :}
  /* Since "IF" is a keyword, need to special case this function */
  | KW_IF LPAREN func_arg_list:exprs RPAREN
  {: RESULT = new FunctionCallExpr("if", exprs); :}
  | cast_expr:c
  {: RESULT = c; :}
  | case_expr:c
  {: RESULT = c; :}
  | aggregate_expr:a
  {: RESULT = a; :}
  | column_ref:c
  {: RESULT = c; :}
  | timestamp_arithmetic_expr:e
  {: RESULT = e; :}
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

// We use IDENT for the temporal unit to avoid making DAY, YEAR, etc. keywords.
// This way we do not need to change existing uses of IDENT.
// We chose not to make DATE_ADD and DATE_SUB keywords for the same reason.
timestamp_arithmetic_expr ::=
  KW_INTERVAL expr:v IDENT:u ADD expr:t
  {: RESULT = new TimestampArithmeticExpr(ArithmeticExpr.Operator.ADD, t, v, u, true); :}
  | expr:t ADD KW_INTERVAL expr:v IDENT:u
  {:
    RESULT = new TimestampArithmeticExpr(ArithmeticExpr.Operator.ADD, t, v, u, false);
  :}
  // Set precedence to KW_INTERVAL (which is higher than ADD) for chaining.
  %prec KW_INTERVAL
  | expr:t SUBTRACT KW_INTERVAL expr:v IDENT:u
  {:
    RESULT =
        new TimestampArithmeticExpr(ArithmeticExpr.Operator.SUBTRACT, t, v, u, false);
  :}
  // Set precedence to KW_INTERVAL (which is higher than ADD) for chaining.
  %prec KW_INTERVAL
  // Timestamp arithmetic expr that looks like a function call.
  // We use func_arg_list instead of expr to avoid a shift/reduce conflict with
  // func_arg_list on COMMA, and report an error if the list contains more than one expr.
  | IDENT:functionName LPAREN func_arg_list:l COMMA KW_INTERVAL expr:v IDENT:u RPAREN
  {:
    if (l.size() > 1) {
      // Report parsing failure on keyword interval.
      parser.parseError("interval", SqlParserSymbols.KW_INTERVAL);
    }
    RESULT = new TimestampArithmeticExpr(functionName, l.get(0), v, u);
  :}
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
    parser.parseError("literal", SqlParserSymbols.UNMATCHED_STRING_LITERAL);
  :}
  | NUMERIC_OVERFLOW:l
  {:
    // similar to the unmatched string literal case
    // we must terminate parsing at this point
    // and generate a corresponding symbol to be reported
    parser.parseError("literal", SqlParserSymbols.NUMERIC_OVERFLOW);
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
  | KW_DISTINCTPC
  {: RESULT = AggregateExpr.Operator.DISTINCT_PC; :}
  | KW_DISTINCTPCSA
  {: RESULT = AggregateExpr.Operator.DISTINCT_PCSA; :}
  | KW_SUM
  {: RESULT = AggregateExpr.Operator.SUM; :}
  | KW_AVG
  {: RESULT = AggregateExpr.Operator.AVG; :}
  ;

aggregate_param_list ::=
  STAR
  {: RESULT = AggregateParamsList.createStarParam(); :}
  | KW_ALL STAR
  {: RESULT = AggregateParamsList.createStarParam(); :}
  | expr_list:exprs
  {: RESULT = new AggregateParamsList(false, exprs); :}
  | KW_ALL expr_list:exprs
  {: RESULT = new AggregateParamsList(false, exprs); :}
  | KW_DISTINCT:distinct expr_list:exprs
  {: RESULT = new AggregateParamsList(true, exprs); :}
  ;

predicate ::=
  expr:e KW_IS KW_NULL
  {: RESULT = new IsNullPredicate(e, false); :}
  | expr:e KW_IS KW_NOT KW_NULL
  {: RESULT = new IsNullPredicate(e, true); :}
  | between_predicate:p
  {: RESULT = p; :}
  | comparison_predicate:p
  {: RESULT = p; :}
  | compound_predicate:p
  {: RESULT = p; :}
  | in_predicate:p
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
  | expr:e1 KW_NOT KW_LIKE expr:e2
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.NOT,
    new LikePredicate(LikePredicate.Operator.LIKE, e1, e2), null); :}
  | expr:e1 KW_NOT KW_RLIKE expr:e2
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.NOT,
    new LikePredicate(LikePredicate.Operator.RLIKE, e1, e2), null); :}
  | expr:e1 KW_NOT KW_REGEXP expr:e2
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.NOT,
    new LikePredicate(LikePredicate.Operator.REGEXP, e1, e2), null); :}
  ;

between_predicate ::=
  expr:e1 KW_BETWEEN expr:e2 KW_AND expr:e3
  {: RESULT = new BetweenPredicate(e1, e2, e3, false); :}
  | expr:e1 KW_NOT KW_BETWEEN expr:e2 KW_AND expr:e3
  {: RESULT = new BetweenPredicate(e1, e2, e3, true); :}
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

// Using expr_or_predicate here results in an unresolvable shift/reduce conflict.
// Instead, we must list expr and predicate explicitly.
in_predicate ::=
  expr:e KW_IN LPAREN func_arg_list:l RPAREN
  {: RESULT = new InPredicate(e, l, false); :}
  | predicate:p KW_IN LPAREN func_arg_list:l RPAREN
  {: RESULT = new InPredicate(p, l, false); :}
  | expr:e KW_NOT KW_IN LPAREN func_arg_list:l RPAREN
  {: RESULT = new InPredicate(e, l, true); :}
  | predicate:p KW_NOT KW_IN LPAREN func_arg_list:l RPAREN
  {: RESULT = new InPredicate(p, l, true); :}
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
