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

import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.ArrayType;
import com.cloudera.impala.catalog.MapType;
import com.cloudera.impala.catalog.StructType;
import com.cloudera.impala.catalog.StructField;
import com.cloudera.impala.catalog.RowFormat;
import com.cloudera.impala.catalog.View;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.analysis.ColumnDef;
import com.cloudera.impala.analysis.UnionStmt.UnionOperand;
import com.cloudera.impala.analysis.UnionStmt.Qualifier;
import com.cloudera.impala.thrift.TFunctionCategory;
import com.cloudera.impala.thrift.TDescribeTableOutputStyle;
import com.cloudera.impala.thrift.THdfsFileFormat;
import com.cloudera.impala.thrift.TPrivilegeLevel;
import com.cloudera.impala.thrift.TTablePropertyType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java_cup.runtime.Symbol;
import com.google.common.collect.Lists;

parser code {:
  private Symbol errorToken_;

  // Set if the errorToken_ to be printed in the error message has a different name, e.g.
  // when parsing identifiers instead of defined keywords. This is necessary to avoid
  // conflicting keywords.
  private String expectedTokenName_;

  // list of expected tokens ids from current parsing state
  // for generating syntax error message
  private final List<Integer> expectedTokenIds_ = new ArrayList<Integer>();

  // to avoid reporting trivial tokens as expected tokens in error messages
  private boolean reportExpectedToken(Integer tokenId, int numExpectedTokens) {
    if (SqlScanner.isKeyword(tokenId) ||
        tokenId.intValue() == SqlParserSymbols.COMMA ||
        tokenId.intValue() == SqlParserSymbols.IDENT) {
      return true;
    } else {
      // if this is the only valid token, always report it
      return numExpectedTokens == 1;
    }
  }

  private String getErrorTypeMessage(int lastTokenId) {
    String msg = null;
    switch (lastTokenId) {
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
    errorToken_ = token;

    // derive expected tokens from current parsing state
    expectedTokenIds_.clear();
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
      expectedTokenIds_.add(Integer.valueOf(tokenId));
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
    parseError(symbolName, symbolId, null);
  }

  /**
   * Same as parseError() above but allows the error token to have a different
   * name printed as the expected token.
   */
  public void parseError(String symbolName, int symbolId, String expectedTokenName)
      throws Exception {
    expectedTokenName_ = expectedTokenName;
    Symbol errorToken = getSymbolFactory().newSymbol(symbolName, symbolId,
        ((Symbol) stack.peek()), ((Symbol) stack.peek()), null);
    // Call syntax error to gather information about expected tokens, etc.
    // syntax_error does not throw an exception
    syntax_error(errorToken);
    // Unrecovered_syntax_error throws an exception and will terminate parsing
    unrecovered_syntax_error(errorToken);
  }

  // Returns error string, consisting of a shortened offending line
  // with a '^' under the offending token. Assumes
  // that parse() has been called and threw an exception
  public String getErrorMsg(String stmt) {
    if (errorToken_ == null || stmt == null) return null;
    String[] lines = stmt.split("\n");
    StringBuffer result = new StringBuffer();
    result.append(getErrorTypeMessage(errorToken_.sym) + " in line ");
    result.append(errorToken_.left);
    result.append(":\n");

    // errorToken_.left is the line number of error.
    // errorToken_.right is the column number of the error.
    String errorLine = lines[errorToken_.left - 1];
    // If the error is that additional tokens are expected past the end,
    // errorToken_.right will be past the end of the string.
    int lastCharIndex = Math.min(errorLine.length(), errorToken_.right);
    int maxPrintLength = 60;
    int errorLoc = 0;
    if (errorLine.length() <= maxPrintLength) {
      // The line is short. Print the entire line.
      result.append(errorLine);
      result.append('\n');
      errorLoc = errorToken_.right;
    } else {
      // The line is too long. Print maxPrintLength/2 characters before the error and
      // after the error.
      int contextLength = maxPrintLength / 2 - 3;
      String leftSubStr;
      if (errorToken_.right > maxPrintLength / 2) {
        leftSubStr = "..." + errorLine.substring(errorToken_.right - contextLength,
            lastCharIndex);
      } else {
        leftSubStr = errorLine.substring(0, errorToken_.right);
      }
      errorLoc = leftSubStr.length();
      result.append(leftSubStr);
      if (errorLine.length() - errorToken_.right > maxPrintLength / 2) {
        result.append(errorLine.substring(errorToken_.right,
           errorToken_.right + contextLength) + "...");
      } else {
        result.append(errorLine.substring(lastCharIndex));
      }
      result.append("\n");
    }

    // print error indicator
    for (int i = 0; i < errorLoc - 1; ++i) {
      result.append(' ');
    }
    result.append("^\n");

    // only report encountered and expected tokens for syntax errors
    if (errorToken_.sym == SqlParserSymbols.UNMATCHED_STRING_LITERAL ||
        errorToken_.sym == SqlParserSymbols.NUMERIC_OVERFLOW) {
      return result.toString();
    }

    // append last encountered token
    result.append("Encountered: ");
    String lastToken =
      SqlScanner.tokenIdMap.get(Integer.valueOf(errorToken_.sym));
    if (lastToken != null) {
      result.append(lastToken);
    } else {
      result.append("Unknown last token with id: " + errorToken_.sym);
    }

    // append expected tokens
    result.append('\n');
    result.append("Expected: ");
    if (expectedTokenName_ == null) {
      String expectedToken = null;
      Integer tokenId = null;
      for (int i = 0; i < expectedTokenIds_.size(); ++i) {
        tokenId = expectedTokenIds_.get(i);
        if (reportExpectedToken(tokenId, expectedTokenIds_.size())) {
          expectedToken = SqlScanner.tokenIdMap.get(tokenId);
          result.append(expectedToken + ", ");
        }
      }
      // remove trailing ", "
      result.delete(result.length()-2, result.length());
    } else {
      result.append(expectedTokenName_);
    }
    result.append('\n');

    return result.toString();
  }
:};

// List of keywords. Please keep them sorted alphabetically.
terminal
  KW_ADD, KW_AGGREGATE, KW_ALL, KW_ALTER, KW_ANALYTIC, KW_AND, KW_ANTI, KW_API_VERSION,
  KW_ARRAY, KW_AS, KW_ASC, KW_AVRO, KW_BETWEEN, KW_BIGINT, KW_BINARY, KW_BOOLEAN, KW_BY,
  KW_CACHED, KW_CASCADE, KW_CASE, KW_CAST, KW_CHANGE, KW_CHAR, KW_CLASS, KW_CLOSE_FN, KW_COLUMN,
  KW_COLUMNS, KW_COMMENT, KW_COMPUTE, KW_CREATE, KW_CROSS, KW_CURRENT, KW_DATA,
  KW_DATABASE, KW_DATABASES, KW_DATE, KW_DATETIME, KW_DECIMAL, KW_DELIMITED, KW_DESC,
  KW_DESCRIBE, KW_DISTINCT, KW_DIV, KW_DOUBLE, KW_DROP, KW_ELSE, KW_END, KW_ESCAPED,
  KW_EXISTS, KW_EXPLAIN, KW_EXTERNAL, KW_FALSE, KW_FIELDS,
  KW_FILEFORMAT, KW_FILES, KW_FINALIZE_FN,
  KW_FIRST, KW_FLOAT, KW_FOLLOWING, KW_FOR, KW_FORMAT, KW_FORMATTED, KW_FROM, KW_FULL,
  KW_FUNCTION, KW_FUNCTIONS, KW_GRANT, KW_GROUP, KW_HAVING, KW_IF, KW_IN, KW_INCREMENTAL,
  KW_INIT_FN, KW_INNER, KW_INPATH, KW_INSERT, KW_INT, KW_INTERMEDIATE, KW_INTERVAL,
  KW_INTO, KW_INVALIDATE, KW_IS, KW_JOIN, KW_LAST, KW_LEFT, KW_LIKE, KW_LIMIT, KW_LINES,
  KW_LOAD, KW_LOCATION, KW_MAP, KW_MERGE_FN, KW_METADATA, KW_NOT, KW_NULL, KW_NULLS,
  KW_OFFSET, KW_ON, KW_OR, KW_ORDER, KW_OUTER, KW_OVER, KW_OVERWRITE, KW_PARQUET,
  KW_PARQUETFILE, KW_PARTITION, KW_PARTITIONED, KW_PARTITIONS, KW_PRECEDING,
  KW_PREPARE_FN, KW_PRODUCED, KW_PURGE, KW_RANGE, KW_RCFILE, KW_RECOVER, KW_REFRESH,
  KW_REGEXP, KW_RENAME, KW_REPLACE, KW_REPLICATION, KW_RESTRICT, KW_RETURNS,
  KW_REVOKE, KW_RIGHT, KW_RLIKE, KW_ROLE,
  KW_ROLES, KW_ROW, KW_ROWS, KW_SCHEMA, KW_SCHEMAS, KW_SELECT, KW_SEMI, KW_SEQUENCEFILE,
  KW_SERDEPROPERTIES, KW_SERIALIZE_FN, KW_SET, KW_SHOW, KW_SMALLINT, KW_STORED,
  KW_STRAIGHT_JOIN, KW_STRING, KW_STRUCT, KW_SYMBOL, KW_TABLE, KW_TABLES,
  KW_TBLPROPERTIES, KW_TERMINATED, KW_TEXTFILE, KW_THEN, KW_TIMESTAMP,
  KW_TINYINT, KW_TRUNCATE, KW_STATS, KW_TO, KW_TRUE, KW_UNBOUNDED, KW_UNCACHED,
  KW_UNION, KW_UPDATE_FN, KW_USE, KW_USING,
  KW_VALUES, KW_VARCHAR, KW_VIEW, KW_WHEN, KW_WHERE, KW_WITH;

terminal COLON, SEMICOLON, COMMA, DOT, DOTDOTDOT, STAR, LPAREN, RPAREN, LBRACKET,
  RBRACKET, DIVIDE, MOD, ADD, SUBTRACT;
terminal BITAND, BITOR, BITXOR, BITNOT;
terminal EQUAL, NOT, NOTEQUAL, LESSTHAN, GREATERTHAN;
terminal FACTORIAL; // Placeholder terminal for postfix factorial operator
terminal String IDENT;
terminal String EMPTY_IDENT;
terminal String NUMERIC_OVERFLOW;
terminal String COMMENTED_PLAN_HINTS;
terminal BigDecimal INTEGER_LITERAL;
terminal BigDecimal DECIMAL_LITERAL;
terminal String STRING_LITERAL;
terminal String UNMATCHED_STRING_LITERAL;
terminal String UNEXPECTED_CHAR;

nonterminal StatementBase stmt;
// Single select statement.
nonterminal SelectStmt select_stmt;
// Single values statement.
nonterminal ValuesStmt values_stmt;
// Select or union statement.
nonterminal QueryStmt query_stmt;
nonterminal QueryStmt opt_query_stmt;
// Single select_stmt or parenthesized query_stmt.
nonterminal QueryStmt union_operand;
// List of select or union blocks connected by UNION operators or a single select block.
nonterminal List<UnionOperand> union_operand_list;
// List of union operands consisting of constant selects.
nonterminal List<UnionOperand> values_operand_list;
// USE stmt
nonterminal UseStmt use_stmt;
nonterminal SetStmt set_stmt;
nonterminal ShowTablesStmt show_tables_stmt;
nonterminal ShowDbsStmt show_dbs_stmt;
nonterminal ShowPartitionsStmt show_partitions_stmt;
nonterminal ShowStatsStmt show_stats_stmt;
nonterminal String show_pattern;
nonterminal ShowFilesStmt show_files_stmt;
nonterminal DescribeStmt describe_stmt;
nonterminal ShowCreateTableStmt show_create_tbl_stmt;
nonterminal TDescribeTableOutputStyle describe_output_style;
nonterminal LoadDataStmt load_stmt;
nonterminal TruncateStmt truncate_stmt;
nonterminal ResetMetadataStmt reset_metadata_stmt;
// List of select blocks connected by UNION operators, with order by or limit.
nonterminal QueryStmt union_with_order_by_or_limit;
nonterminal SelectList select_clause;
nonterminal SelectList select_list;
nonterminal SelectListItem select_list_item;
nonterminal SelectListItem star_expr;
nonterminal Expr expr, non_pred_expr, arithmetic_expr, timestamp_arithmetic_expr;
nonterminal ArrayList<Expr> expr_list;
nonterminal String alias_clause;
nonterminal ArrayList<String> ident_list;
nonterminal ArrayList<String> opt_ident_list;
nonterminal TableName table_name;
nonterminal FunctionName function_name;
nonterminal Expr where_clause;
nonterminal Predicate predicate, between_predicate, comparison_predicate,
  compound_predicate, in_predicate, like_predicate, exists_predicate;
nonterminal ArrayList<Expr> group_by_clause, opt_partition_by_clause;
nonterminal Expr having_clause;
nonterminal ArrayList<OrderByElement> order_by_elements, opt_order_by_clause;
nonterminal OrderByElement order_by_element;
nonterminal Boolean opt_order_param;
nonterminal Boolean opt_nulls_order_param;
nonterminal Expr opt_offset_param;
nonterminal LimitElement opt_limit_offset_clause;
nonterminal Expr opt_limit_clause, opt_offset_clause;
nonterminal Expr cast_expr, case_else_clause, analytic_expr;
nonterminal Expr function_call_expr;
nonterminal AnalyticWindow opt_window_clause;
nonterminal AnalyticWindow.Type window_type;
nonterminal AnalyticWindow.Boundary window_boundary;
nonterminal LiteralExpr literal;
nonterminal CaseExpr case_expr;
nonterminal ArrayList<CaseWhenClause> case_when_clause_list;
nonterminal FunctionParams function_params;
nonterminal ArrayList<String> dotted_path;
nonterminal SlotRef slot_ref;
nonterminal ArrayList<TableRef> from_clause, table_ref_list;
nonterminal WithClause opt_with_clause;
nonterminal ArrayList<View> with_view_def_list;
nonterminal View with_view_def;
nonterminal TableRef table_ref;
nonterminal Subquery subquery;
nonterminal JoinOperator join_operator;
nonterminal opt_inner, opt_outer;
nonterminal ArrayList<String> opt_plan_hints;
nonterminal TypeDef type_def;
nonterminal Type type;
nonterminal Expr sign_chain_expr;
nonterminal InsertStmt insert_stmt;
nonterminal StatementBase explain_stmt;
// Optional partition spec
nonterminal PartitionSpec opt_partition_spec;
// Required partition spec
nonterminal PartitionSpec partition_spec;
nonterminal ArrayList<PartitionKeyValue> partition_clause;
nonterminal ArrayList<PartitionKeyValue> static_partition_key_value_list;
nonterminal ArrayList<PartitionKeyValue> partition_key_value_list;
nonterminal PartitionKeyValue partition_key_value;
nonterminal PartitionKeyValue static_partition_key_value;
nonterminal Qualifier union_op;

nonterminal AlterTableStmt alter_tbl_stmt;
nonterminal StatementBase alter_view_stmt;
nonterminal ComputeStatsStmt compute_stats_stmt;
nonterminal DropDbStmt drop_db_stmt;
nonterminal DropStatsStmt drop_stats_stmt;
nonterminal DropTableOrViewStmt drop_tbl_or_view_stmt;
nonterminal CreateDbStmt create_db_stmt;
nonterminal CreateTableAsSelectStmt create_tbl_as_select_stmt;
nonterminal CreateTableLikeStmt create_tbl_like_stmt;
nonterminal CreateTableLikeFileStmt create_tbl_like_file_stmt;
nonterminal CreateTableStmt create_unpartitioned_tbl_stmt, create_partitioned_tbl_stmt;
nonterminal CreateViewStmt create_view_stmt;
nonterminal CreateDataSrcStmt create_data_src_stmt;
nonterminal DropDataSrcStmt drop_data_src_stmt;
nonterminal ShowDataSrcsStmt show_data_srcs_stmt;
nonterminal StructField struct_field_def;
nonterminal ColumnDef column_def, view_column_def;
nonterminal ArrayList<ColumnDef> column_def_list, view_column_def_list;
nonterminal ArrayList<ColumnDef> partition_column_defs, view_column_defs;
nonterminal ArrayList<StructField> struct_field_def_list;
// Options for DDL commands - CREATE/DROP/ALTER
nonterminal HdfsCachingOp cache_op_val;
nonterminal BigDecimal opt_cache_op_replication;
nonterminal String comment_val;
nonterminal Boolean external_val;
nonterminal Boolean purge_val;
nonterminal String opt_init_string_val;
nonterminal THdfsFileFormat file_format_val;
nonterminal THdfsFileFormat file_format_create_table_val;
nonterminal Boolean if_exists_val;
nonterminal Boolean if_not_exists_val;
nonterminal Boolean replace_existing_cols_val;
nonterminal HdfsUri location_val;
nonterminal RowFormat row_format_val;
nonterminal String field_terminator_val;
nonterminal String line_terminator_val;
nonterminal String escaped_by_val;
nonterminal String terminator_val;
nonterminal TTablePropertyType table_property_type;
nonterminal HashMap serde_properties;
nonterminal HashMap tbl_properties;
nonterminal HashMap properties_map;
// Used to simplify commands that accept either KW_DATABASE(S) or KW_SCHEMA(S)
nonterminal String db_or_schema_kw;
nonterminal String dbs_or_schemas_kw;
// Used to simplify commands where KW_COLUMN is optional
nonterminal String opt_kw_column;
// Used to simplify commands where KW_TABLE is optional
nonterminal String opt_kw_table;
nonterminal Boolean overwrite_val;
nonterminal Boolean cascade_val;

// For GRANT/REVOKE/AUTH DDL statements
nonterminal ShowRolesStmt show_roles_stmt;
nonterminal ShowGrantRoleStmt show_grant_role_stmt;
nonterminal CreateDropRoleStmt create_drop_role_stmt;
nonterminal GrantRevokeRoleStmt grant_role_stmt;
nonterminal GrantRevokeRoleStmt revoke_role_stmt;
nonterminal GrantRevokePrivStmt grant_privilege_stmt;
nonterminal GrantRevokePrivStmt revoke_privilege_stmt;
nonterminal PrivilegeSpec privilege_spec;
nonterminal TPrivilegeLevel privilege;
nonterminal Boolean opt_with_grantopt;
nonterminal Boolean opt_grantopt_for;
nonterminal Boolean opt_kw_role;

// To avoid creating common keywords such as 'SERVER' or 'SOURCES' we treat them as
// identifiers rather than keywords. Throws a parse exception if the identifier does not
// match the expected string.
nonterminal Boolean source_ident;
nonterminal Boolean sources_ident;
nonterminal Boolean server_ident;
nonterminal Boolean uri_ident;
nonterminal Boolean option_ident;

// For Create/Drop/Show function ddl
nonterminal FunctionArgs function_def_args;
nonterminal FunctionArgs function_def_arg_list;
// Accepts space separated key='v' arguments.
nonterminal HashMap function_def_args_map;
nonterminal CreateFunctionStmtBase.OptArg function_def_arg_key;
nonterminal Boolean opt_is_aggregate_fn;
nonterminal Boolean opt_is_varargs;
nonterminal TypeDef opt_aggregate_fn_intermediate_type_def;
nonterminal CreateUdfStmt create_udf_stmt;
nonterminal CreateUdaStmt create_uda_stmt;
nonterminal ShowFunctionsStmt show_functions_stmt;
nonterminal DropFunctionStmt drop_function_stmt;
nonterminal TFunctionCategory opt_function_category;

precedence left KW_OR;
precedence left KW_AND;
precedence right KW_NOT, NOT;
precedence left KW_BETWEEN, KW_IN, KW_IS, KW_EXISTS;
precedence left KW_LIKE, KW_RLIKE, KW_REGEXP;
precedence left EQUAL, NOTEQUAL, LESSTHAN, GREATERTHAN;
precedence left ADD, SUBTRACT;
precedence left STAR, DIVIDE, MOD, KW_DIV;
precedence left BITAND, BITOR, BITXOR, BITNOT;
precedence left FACTORIAL;
precedence left KW_ORDER, KW_BY, KW_LIMIT;
precedence left LPAREN, RPAREN;
// Support chaining of timestamp arithmetic exprs.
precedence left KW_INTERVAL;

// These tokens need to be at the end for function_def_args_map to accept
// no keys. Otherwise, the grammar has shift/reduce conflicts.
precedence left KW_COMMENT;
precedence left KW_SYMBOL;
precedence left KW_PREPARE_FN;
precedence left KW_CLOSE_FN;
precedence left KW_UPDATE_FN;
precedence left KW_FINALIZE_FN;
precedence left KW_INIT_FN;
precedence left KW_MERGE_FN;
precedence left KW_SERIALIZE_FN;

precedence left KW_OVER;

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
  | show_partitions_stmt:show_partitions
  {: RESULT = show_partitions; :}
  | show_stats_stmt:show_stats
  {: RESULT = show_stats; :}
  | show_functions_stmt:show_functions
  {: RESULT = show_functions; :}
  | show_data_srcs_stmt:show_data_srcs
  {: RESULT = show_data_srcs; :}
  | show_create_tbl_stmt:show_create_tbl
  {: RESULT = show_create_tbl; :}
  | show_files_stmt:show_files
  {: RESULT = show_files; :}
  | describe_stmt:describe
  {: RESULT = describe; :}
  | alter_tbl_stmt:alter_tbl
  {: RESULT = alter_tbl; :}
  | alter_view_stmt:alter_view
  {: RESULT = alter_view; :}
  | compute_stats_stmt:compute_stats
  {: RESULT = compute_stats; :}
  | drop_stats_stmt:drop_stats
  {: RESULT = drop_stats; :}
  | create_tbl_as_select_stmt:create_tbl_as_select
  {: RESULT = create_tbl_as_select; :}
  | create_tbl_like_stmt:create_tbl_like
  {: RESULT = create_tbl_like; :}
  | create_tbl_like_file_stmt:create_tbl_like_file
  {: RESULT = create_tbl_like_file; :}
  | create_unpartitioned_tbl_stmt:create_tbl
  {: RESULT = create_tbl; :}
  | create_partitioned_tbl_stmt:create_tbl
  {: RESULT = create_tbl; :}
  | create_view_stmt:create_view
  {: RESULT = create_view; :}
  | create_data_src_stmt:create_data_src
  {: RESULT = create_data_src; :}
  | create_db_stmt:create_db
  {: RESULT = create_db; :}
  | create_udf_stmt:create_udf
  {: RESULT = create_udf; :}
  | create_uda_stmt:create_uda
  {: RESULT = create_uda; :}
  | drop_db_stmt:drop_db
  {: RESULT = drop_db; :}
  | drop_tbl_or_view_stmt:drop_tbl
  {: RESULT = drop_tbl; :}
  | drop_function_stmt:drop_function
  {: RESULT = drop_function; :}
  | drop_data_src_stmt:drop_data_src
  {: RESULT = drop_data_src; :}
  | explain_stmt:explain
  {: RESULT = explain; :}
  | load_stmt: load
  {: RESULT = load; :}
  | truncate_stmt: truncate
  {: RESULT = truncate; :}
  | reset_metadata_stmt: reset_metadata
  {: RESULT = reset_metadata; :}
  | set_stmt:set
  {: RESULT = set; :}
  | show_roles_stmt:show_roles
  {: RESULT = show_roles; :}
  | show_grant_role_stmt:show_grant_role
  {: RESULT = show_grant_role; :}
  | create_drop_role_stmt:create_drop_role
  {: RESULT = create_drop_role; :}
  | grant_role_stmt:grant_role
  {: RESULT = grant_role; :}
  | revoke_role_stmt:revoke_role
  {: RESULT = revoke_role; :}
  | grant_privilege_stmt:grant_privilege
  {: RESULT = grant_privilege; :}
  | revoke_privilege_stmt:revoke_privilege
  {: RESULT = revoke_privilege; :}
  | stmt:s SEMICOLON
  {: RESULT = s; :}
  ;

load_stmt ::=
  KW_LOAD KW_DATA KW_INPATH STRING_LITERAL:path overwrite_val:overwrite KW_INTO KW_TABLE
  table_name:table opt_partition_spec:partition
  {: RESULT = new LoadDataStmt(table, new HdfsUri(path), overwrite, partition); :}
  ;

truncate_stmt ::=
  KW_TRUNCATE KW_TABLE table_name:tbl_name
  {: RESULT = new TruncateStmt(tbl_name); :}
  | KW_TRUNCATE table_name:tbl_name
  {: RESULT = new TruncateStmt(tbl_name); :}
  ;

overwrite_val ::=
  KW_OVERWRITE
  {: RESULT = Boolean.TRUE; :}
  | /* empty */
  {: RESULT = Boolean.FALSE; :}
  ;

reset_metadata_stmt ::=
  KW_INVALIDATE KW_METADATA
  {: RESULT = new ResetMetadataStmt(null, false); :}
  | KW_INVALIDATE KW_METADATA table_name:table
  {: RESULT = new ResetMetadataStmt(table, false); :}
  | KW_REFRESH table_name:table
  {: RESULT = new ResetMetadataStmt(table, true); :}
  ;

explain_stmt ::=
  KW_EXPLAIN query_stmt:query
  {:
     query.setIsExplain();
     RESULT = query;
  :}
  | KW_EXPLAIN insert_stmt:insert
  {:
     insert.setIsExplain();
     RESULT = insert;
  :}
  | KW_EXPLAIN create_tbl_as_select_stmt:ctas_stmt
  {:
     ctas_stmt.setIsExplain();
     RESULT = ctas_stmt;
  :}
  ;

// Insert statements have two optional clauses: the column permutation (INSERT into
// tbl(col1,...) etc) and the PARTITION clause. If the column permutation is present, the
// query statement clause is optional as well.
insert_stmt ::=
  opt_with_clause:w KW_INSERT KW_OVERWRITE opt_kw_table table_name:table LPAREN
  opt_ident_list:col_perm RPAREN partition_clause:list opt_plan_hints:hints
  opt_query_stmt:query
  {: RESULT = new InsertStmt(w, table, true, list, hints, query, col_perm); :}
  | opt_with_clause:w KW_INSERT KW_OVERWRITE opt_kw_table table_name:table
  partition_clause:list opt_plan_hints:hints query_stmt:query
  {: RESULT = new InsertStmt(w, table, true, list, hints, query, null); :}
  | opt_with_clause:w KW_INSERT KW_INTO opt_kw_table table_name:table LPAREN
  opt_ident_list:col_perm RPAREN partition_clause:list opt_plan_hints:hints
  opt_query_stmt:query
  {: RESULT = new InsertStmt(w, table, false, list, hints, query, col_perm); :}
  | opt_with_clause:w KW_INSERT KW_INTO opt_kw_table table_name:table
  partition_clause:list opt_plan_hints:hints query_stmt:query
  {: RESULT = new InsertStmt(w, table, false, list, hints, query, null); :}
  ;

opt_query_stmt ::=
  query_stmt:query
  {: RESULT = query; :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_ident_list ::=
  ident_list:ident
  {: RESULT = ident; :}
  | /* empty */
  {: RESULT = Lists.newArrayList(); :}
  ;

opt_kw_table ::=
  KW_TABLE
  | /* empty */
  ;

show_roles_stmt ::=
  KW_SHOW KW_ROLES
  {: RESULT = new ShowRolesStmt(false, null); :}
  | KW_SHOW KW_ROLE KW_GRANT KW_GROUP IDENT:group
  {: RESULT = new ShowRolesStmt(false, group); :}
  | KW_SHOW KW_CURRENT KW_ROLES
  {: RESULT = new ShowRolesStmt(true, null); :}
  ;

show_grant_role_stmt ::=
  KW_SHOW KW_GRANT KW_ROLE IDENT:role
  {: RESULT = new ShowGrantRoleStmt(role, null); :}
  | KW_SHOW KW_GRANT KW_ROLE IDENT:role KW_ON server_ident:server_kw
  {:
    RESULT = new ShowGrantRoleStmt(role,
        PrivilegeSpec.createServerScopedPriv(TPrivilegeLevel.ALL));
  :}
  | KW_SHOW KW_GRANT KW_ROLE IDENT:role KW_ON KW_DATABASE IDENT:db_name
  {:
    RESULT = new ShowGrantRoleStmt(role,
        PrivilegeSpec.createDbScopedPriv(TPrivilegeLevel.ALL, db_name));
  :}
  | KW_SHOW KW_GRANT KW_ROLE IDENT:role KW_ON KW_TABLE table_name:tbl_name
  {:
    RESULT = new ShowGrantRoleStmt(role,
        PrivilegeSpec.createTableScopedPriv(TPrivilegeLevel.ALL, tbl_name));
  :}
  | KW_SHOW KW_GRANT KW_ROLE IDENT:role KW_ON uri_ident:uri_kw STRING_LITERAL:uri
  {:
    RESULT = new ShowGrantRoleStmt(role,
        PrivilegeSpec.createUriScopedPriv(TPrivilegeLevel.ALL, new HdfsUri(uri)));
  :}
  ;

create_drop_role_stmt ::=
  KW_CREATE KW_ROLE IDENT:role
  {: RESULT = new CreateDropRoleStmt(role, false); :}
  | KW_DROP KW_ROLE IDENT:role
  {: RESULT = new CreateDropRoleStmt(role, true); :}
  ;

grant_role_stmt ::=
  KW_GRANT KW_ROLE IDENT:role KW_TO KW_GROUP IDENT:group
  {: RESULT = new GrantRevokeRoleStmt(role, group, true); :}
  ;

revoke_role_stmt ::=
  KW_REVOKE KW_ROLE IDENT:role KW_FROM KW_GROUP IDENT:group
  {: RESULT = new GrantRevokeRoleStmt(role, group, false); :}
  ;

grant_privilege_stmt ::=
  KW_GRANT privilege_spec:priv KW_TO opt_kw_role:opt_role IDENT:role
  opt_with_grantopt:grant_opt
  {: RESULT = new GrantRevokePrivStmt(role, priv, true, grant_opt); :}
  ;

revoke_privilege_stmt ::=
  KW_REVOKE opt_grantopt_for:grant_opt privilege_spec:priv KW_FROM
  opt_kw_role:opt_role IDENT:role
  {: RESULT = new GrantRevokePrivStmt(role, priv, false, grant_opt); :}
  ;

privilege_spec ::=
  privilege:priv KW_ON server_ident:server_kw
  {: RESULT = PrivilegeSpec.createServerScopedPriv(priv); :}
  | privilege:priv KW_ON KW_DATABASE IDENT:db_name
  {: RESULT = PrivilegeSpec.createDbScopedPriv(priv, db_name); :}
  | privilege:priv KW_ON KW_TABLE table_name:tbl_name
  {: RESULT = PrivilegeSpec.createTableScopedPriv(priv, tbl_name); :}
  | privilege:priv LPAREN opt_ident_list:cols RPAREN KW_ON KW_TABLE table_name:tbl_name
  {: RESULT = PrivilegeSpec.createColumnScopedPriv(priv, tbl_name, cols); :}
  | privilege:priv KW_ON uri_ident:uri_kw STRING_LITERAL:uri
  {: RESULT = PrivilegeSpec.createUriScopedPriv(priv, new HdfsUri(uri)); :}
  ;

privilege ::=
  KW_SELECT
  {: RESULT = TPrivilegeLevel.SELECT; :}
  | KW_INSERT
  {: RESULT = TPrivilegeLevel.INSERT; :}
  | KW_ALL
  {: RESULT = TPrivilegeLevel.ALL; :}
  ;

opt_grantopt_for ::=
  KW_GRANT option_ident:option KW_FOR
  {: RESULT = true; :}
  | /* empty */
  {: RESULT = false; :}
  ;

opt_with_grantopt ::=
  KW_WITH KW_GRANT option_ident:option
  {: RESULT = true; :}
  | /* empty */
  {: RESULT = false; :}
  ;

opt_kw_role ::=
  KW_ROLE
  | /* empty */
  ;

alter_tbl_stmt ::=
  KW_ALTER KW_TABLE table_name:table replace_existing_cols_val:replace KW_COLUMNS
  LPAREN column_def_list:col_defs RPAREN
  {: RESULT = new AlterTableAddReplaceColsStmt(table, col_defs, replace); :}
  | KW_ALTER KW_TABLE table_name:table KW_ADD if_not_exists_val:if_not_exists
    partition_spec:partition location_val:location cache_op_val:cache_op
  {:
    RESULT = new AlterTableAddPartitionStmt(table, partition,
        location, if_not_exists, cache_op);
  :}
  | KW_ALTER KW_TABLE table_name:table KW_DROP opt_kw_column IDENT:col_name
  {: RESULT = new AlterTableDropColStmt(table, col_name); :}
  | KW_ALTER KW_TABLE table_name:table KW_CHANGE opt_kw_column IDENT:col_name
    column_def:col_def
  {: RESULT = new AlterTableChangeColStmt(table, col_name, col_def); :}
  | KW_ALTER KW_TABLE table_name:table KW_DROP if_exists_val:if_exists
    partition_spec:partition purge_val:purge
  {: RESULT = new AlterTableDropPartitionStmt(table, partition, if_exists, purge); :}
  | KW_ALTER KW_TABLE table_name:table opt_partition_spec:partition KW_SET KW_FILEFORMAT
    file_format_val:file_format
  {: RESULT = new AlterTableSetFileFormatStmt(table, partition, file_format); :}
  | KW_ALTER KW_TABLE table_name:table opt_partition_spec:partition KW_SET
    KW_LOCATION STRING_LITERAL:location
  {: RESULT = new AlterTableSetLocationStmt(table, partition, new HdfsUri(location)); :}
  | KW_ALTER KW_TABLE table_name:table KW_RENAME KW_TO table_name:new_table
  {: RESULT = new AlterTableOrViewRenameStmt(table, new_table, true); :}
  | KW_ALTER KW_TABLE table_name:table opt_partition_spec:partition KW_SET
    table_property_type:target LPAREN properties_map:properties RPAREN
  {: RESULT = new AlterTableSetTblProperties(table, partition, target, properties); :}
  | KW_ALTER KW_TABLE table_name:table opt_partition_spec:partition KW_SET
    cache_op_val:cache_op
  {:
    // Ensure a parser error is thrown for ALTER statements if no cache op is specified.
    if (cache_op == null) {
      parser.parseError("set", SqlParserSymbols.KW_SET);
    }
    RESULT = new AlterTableSetCachedStmt(table, partition, cache_op);
  :}
  | KW_ALTER KW_TABLE table_name:table KW_RECOVER KW_PARTITIONS
  {: RESULT = new AlterTableRecoverPartitionsStmt(table); :}
  ;

table_property_type ::=
  KW_TBLPROPERTIES
  {: RESULT = TTablePropertyType.TBL_PROPERTY; :}
  | KW_SERDEPROPERTIES
  {: RESULT = TTablePropertyType.SERDE_PROPERTY; :}
  ;

opt_kw_column ::=
  KW_COLUMN
  | /* empty */
  ;

replace_existing_cols_val ::=
  KW_REPLACE
  {: RESULT = true; :}
  | KW_ADD
  {: RESULT = false; :}
  ;

create_db_stmt ::=
  KW_CREATE db_or_schema_kw if_not_exists_val:if_not_exists IDENT:db_name
  comment_val:comment location_val:location
  {: RESULT = new CreateDbStmt(db_name, comment, location, if_not_exists); :}
  ;

create_tbl_like_stmt ::=
  KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
  table_name:table KW_LIKE table_name:other_table comment_val:comment
  KW_STORED KW_AS file_format_val:file_format location_val:location
  {:
    RESULT = new CreateTableLikeStmt(table, other_table, external, comment,
        file_format, location, if_not_exists);
  :}
  | KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
    table_name:table KW_LIKE table_name:other_table comment_val:comment
    location_val:location
  {:
    RESULT = new CreateTableLikeStmt(table, other_table, external, comment,
        null, location, if_not_exists);
  :}
  ;

create_tbl_like_file_stmt ::=
  KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
  table_name:table KW_LIKE file_format_val:schema_file_format
  STRING_LITERAL:schema_location partition_column_defs:partition_col_defs
  comment_val:comment row_format_val:row_format serde_properties:serde_props
  file_format_create_table_val:file_format location_val:location cache_op_val:cache_op
  tbl_properties:tbl_props
  {:
    RESULT = new CreateTableLikeFileStmt(table, schema_file_format,
        new HdfsUri(schema_location), partition_col_defs, external, comment, row_format,
        file_format, location, cache_op, if_not_exists, tbl_props, serde_props);
  :}
  ;

create_tbl_as_select_stmt ::=
  KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
  table_name:table comment_val:comment row_format_val:row_format
  serde_properties:serde_props file_format_create_table_val:file_format
  location_val:location cache_op_val:cache_op tbl_properties:tbl_props
  KW_AS query_stmt:query
  {:
    // Initialize with empty List of columns and partition columns. The
    // columns will be added from the query statement during analysis
    CreateTableStmt create_stmt = new CreateTableStmt(table, new ArrayList<ColumnDef>(),
        new ArrayList<ColumnDef>(), external, comment, row_format,
        file_format, location, cache_op, if_not_exists, tbl_props, serde_props);
    RESULT = new CreateTableAsSelectStmt(create_stmt, query);
  :}
  ;

// Create unpartitioned tables with and without column definitions.
// We cannot coalesce this production with create_partitioned_tbl_stmt because
// that results in an unresolvable reduce/reduce conflict due to the many
// optional clauses (not clear which rule to reduce on 'empty').
// TODO: Clean up by consolidating everything after the column defs and
// partition clause into a CreateTableParams.
create_unpartitioned_tbl_stmt ::=
  KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
  table_name:table LPAREN column_def_list:col_defs RPAREN comment_val:comment
  row_format_val:row_format serde_properties:serde_props
  file_format_create_table_val:file_format location_val:location cache_op_val:cache_op
  tbl_properties:tbl_props
  {:
    RESULT = new CreateTableStmt(table, col_defs, new ArrayList<ColumnDef>(), external,
        comment, row_format, file_format, location, cache_op, if_not_exists, tbl_props,
        serde_props);
  :}
  | KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
    table_name:table comment_val:comment row_format_val:row_format
    serde_properties:serde_props file_format_create_table_val:file_format
    location_val:location cache_op_val:cache_op tbl_properties:tbl_props
  {:
    RESULT = new CreateTableStmt(table, new ArrayList<ColumnDef>(),
        new ArrayList<ColumnDef>(), external, comment, row_format, file_format,
        location, cache_op, if_not_exists, tbl_props, serde_props);
  :}
  | KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
    table_name:table LPAREN column_def_list:col_defs RPAREN
    KW_PRODUCED KW_BY KW_DATA source_ident:is_source_id IDENT:data_src_name
    opt_init_string_val:init_string comment_val:comment
  {:
    // Need external_val in the grammar to avoid shift/reduce conflict with other
    // CREATE TABLE statements.
    if (external) parser.parseError("external", SqlParserSymbols.KW_EXTERNAL);
    RESULT = new CreateTableDataSrcStmt(table, col_defs, data_src_name, init_string,
        comment, if_not_exists);
  :}
  ;

// Create partitioned tables with and without column definitions.
// TODO: Clean up by consolidating everything after the column defs and
// partition clause into a CreateTableParams.
create_partitioned_tbl_stmt ::=
  KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
  table_name:table LPAREN column_def_list:col_defs RPAREN KW_PARTITIONED KW_BY
  LPAREN column_def_list:partition_col_defs RPAREN comment_val:comment
  row_format_val:row_format serde_properties:serde_props
  file_format_create_table_val:file_format location_val:location cache_op_val:cache_op
  tbl_properties:tbl_props
  {:
    RESULT = new CreateTableStmt(table, col_defs, partition_col_defs, external, comment,
        row_format, file_format, location, cache_op, if_not_exists, tbl_props,
        serde_props);
  :}
  | KW_CREATE external_val:external KW_TABLE if_not_exists_val:if_not_exists
    table_name:table KW_PARTITIONED KW_BY
    LPAREN column_def_list:partition_col_defs RPAREN
    comment_val:comment row_format_val:row_format serde_properties:serde_props
    file_format_create_table_val:file_format location_val:location cache_op_val:cache_op
    tbl_properties:tbl_props
  {:
    RESULT = new CreateTableStmt(table, new ArrayList<ColumnDef>(), partition_col_defs,
        external, comment, row_format, file_format, location, cache_op, if_not_exists,
        tbl_props, serde_props);
  :}
  ;

create_udf_stmt ::=
  KW_CREATE KW_FUNCTION if_not_exists_val:if_not_exists
  function_name:fn_name function_def_args:fn_args
  KW_RETURNS type_def:return_type
  KW_LOCATION STRING_LITERAL:binary_path
  function_def_args_map:arg_map
  {:
    RESULT = new CreateUdfStmt(fn_name, fn_args, return_type, new HdfsUri(binary_path),
        if_not_exists, arg_map);
  :}
  ;

create_uda_stmt ::=
  KW_CREATE KW_AGGREGATE KW_FUNCTION if_not_exists_val:if_not_exists
  function_name:fn_name function_def_args:fn_args
  KW_RETURNS type_def:return_type
  opt_aggregate_fn_intermediate_type_def:intermediate_type
  KW_LOCATION STRING_LITERAL:binary_path
  function_def_args_map:arg_map
  {:
    RESULT = new CreateUdaStmt(fn_name, fn_args, return_type, intermediate_type,
        new HdfsUri(binary_path), if_not_exists, arg_map);
  :}
  ;

cache_op_val ::=
  KW_CACHED KW_IN STRING_LITERAL:pool_name opt_cache_op_replication:replication
  {: RESULT = new HdfsCachingOp(pool_name, replication); :}
  | KW_UNCACHED
  {: RESULT = new HdfsCachingOp(); :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_cache_op_replication ::=
  KW_WITH KW_REPLICATION EQUAL INTEGER_LITERAL:replication
  {: RESULT = replication; :}
  | /* empty */
  {: RESULT = null; :}
  ;

comment_val ::=
  KW_COMMENT STRING_LITERAL:comment
  {: RESULT = comment; :}
  | /* empty */
  {: RESULT = null; :}
  ;

location_val ::=
  KW_LOCATION STRING_LITERAL:location
  {: RESULT = new HdfsUri(location); :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_init_string_val ::=
  LPAREN STRING_LITERAL:init_string RPAREN
  {: RESULT = init_string; :}
  | /* empty */
  {: RESULT = null; :}
  ;

external_val ::=
  KW_EXTERNAL
  {: RESULT = true; :}
  |
  {: RESULT = false; :}
  ;

purge_val ::=
  KW_PURGE
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
  KW_ROW KW_FORMAT KW_DELIMITED field_terminator_val:field_terminator
  escaped_by_val:escaped_by line_terminator_val:line_terminator
  {: RESULT = new RowFormat(field_terminator, line_terminator, escaped_by); :}
  |/* empty */
  {: RESULT = RowFormat.DEFAULT_ROW_FORMAT; :}
  ;

escaped_by_val ::=
  KW_ESCAPED KW_BY STRING_LITERAL:escaped_by
  {: RESULT = escaped_by; :}
  | /* empty */
  {: RESULT = null; :}
  ;

line_terminator_val ::=
  KW_LINES terminator_val:line_terminator
  {: RESULT = line_terminator; :}
  | /* empty */
  {: RESULT = null; :}
  ;

field_terminator_val ::=
  KW_FIELDS terminator_val:field_terminator
  {: RESULT = field_terminator; :}
  | /* empty */
  {: RESULT = null; :}
  ;

terminator_val ::=
  KW_TERMINATED KW_BY STRING_LITERAL:terminator
  {: RESULT = terminator; :}
  ;

file_format_create_table_val ::=
  KW_STORED KW_AS file_format_val:file_format
  {: RESULT = file_format; :}
  | /* empty - default to TEXT */
  {: RESULT = THdfsFileFormat.TEXT; :}
  ;

file_format_val ::=
  KW_PARQUET
  {: RESULT = THdfsFileFormat.PARQUET; :}
  | KW_PARQUETFILE
  {: RESULT = THdfsFileFormat.PARQUET; :}
  | KW_TEXTFILE
  {: RESULT = THdfsFileFormat.TEXT; :}
  | KW_SEQUENCEFILE
  {: RESULT = THdfsFileFormat.SEQUENCE_FILE; :}
  | KW_RCFILE
  {: RESULT = THdfsFileFormat.RC_FILE; :}
  | KW_AVRO
  {: RESULT = THdfsFileFormat.AVRO; :}
  ;

tbl_properties ::=
  KW_TBLPROPERTIES LPAREN properties_map:map RPAREN
  {: RESULT = map; :}
  | /* empty */
  {: RESULT = null; :}
  ;

serde_properties ::=
  KW_WITH KW_SERDEPROPERTIES LPAREN properties_map:map RPAREN
  {: RESULT = map; :}
  | /* empty */
  {: RESULT = null; :}
  ;

properties_map ::=
  STRING_LITERAL:key EQUAL STRING_LITERAL:value
  {:
    HashMap<String, String> properties = new HashMap<String, String>();
    properties.put(key, value);
    RESULT = properties;
  :}
  | properties_map:properties COMMA STRING_LITERAL:key EQUAL STRING_LITERAL:value
  {:
    properties.put(key, value);
    RESULT = properties;
  :}
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
  IDENT:col_name type_def:type comment_val:comment
  {: RESULT = new ColumnDef(col_name, type, comment); :}
  ;

create_view_stmt ::=
  KW_CREATE KW_VIEW if_not_exists_val:if_not_exists table_name:view_name
  view_column_defs:col_defs comment_val:comment KW_AS query_stmt:view_def
  {:
    RESULT = new CreateViewStmt(if_not_exists, view_name, col_defs, comment, view_def);
  :}
  ;

create_data_src_stmt ::=
  KW_CREATE KW_DATA source_ident:is_source_id
  if_not_exists_val:if_not_exists IDENT:data_src_name
  KW_LOCATION STRING_LITERAL:location
  KW_CLASS STRING_LITERAL:class_name
  KW_API_VERSION STRING_LITERAL:api_version
  {:
    RESULT = new CreateDataSrcStmt(data_src_name, new HdfsUri(location), class_name,
        api_version, if_not_exists);
  :}
  ;

source_ident ::=
  IDENT:ident
  {:
    if (!ident.toUpperCase().equals("SOURCE")) {
      parser.parseError("identifier", SqlParserSymbols.IDENT, "SOURCE");
    }
    RESULT = true;
  :}
  ;

sources_ident ::=
  IDENT:ident
  {:
    if (!ident.toUpperCase().equals("SOURCES")) {
      parser.parseError("identifier", SqlParserSymbols.IDENT, "SOURCES");
    }
    RESULT = true;
  :}
  ;

uri_ident ::=
  IDENT:ident
  {:
    if (!ident.toUpperCase().equals("URI")) {
      parser.parseError("identifier", SqlParserSymbols.IDENT, "URI");
    }
    RESULT = true;
  :}
  ;

server_ident ::=
  IDENT:ident
  {:
    if (!ident.toUpperCase().equals("SERVER")) {
      parser.parseError("identifier", SqlParserSymbols.IDENT, "SERVER");
    }
    RESULT = true;
  :}
  ;

option_ident ::=
  IDENT:ident
  {:
    if (!ident.toUpperCase().equals("OPTION")) {
      parser.parseError("identifier", SqlParserSymbols.IDENT, "OPTION");
    }
    RESULT = true;
  :}
  ;

view_column_defs ::=
  LPAREN view_column_def_list:view_col_defs RPAREN
  {: RESULT = view_col_defs; :}
  | /* empty */
  {: RESULT = null; :}
  ;

view_column_def_list ::=
  view_column_def:col_def
  {:
    ArrayList<ColumnDef> list = new ArrayList<ColumnDef>();
    list.add(col_def);
    RESULT = list;
  :}
  | view_column_def_list:list COMMA view_column_def:col_def
  {:
    list.add(col_def);
    RESULT = list;
  :}
  ;

view_column_def ::=
  IDENT:col_name comment_val:comment
  {: RESULT = new ColumnDef(col_name, null, comment); :}
  ;

alter_view_stmt ::=
  KW_ALTER KW_VIEW table_name:table KW_AS query_stmt:view_def
  {: RESULT = new AlterViewStmt(table, view_def); :}
  | KW_ALTER KW_VIEW table_name:before_table KW_RENAME KW_TO table_name:new_table
  {: RESULT = new AlterTableOrViewRenameStmt(before_table, new_table, false); :}
  ;

cascade_val ::=
  KW_CASCADE
  {: RESULT = true; :}
  | KW_RESTRICT
  {: RESULT = false; :}
  |
  {: RESULT = false; :}
  ;

compute_stats_stmt ::=
  KW_COMPUTE KW_STATS table_name:table
  {: RESULT = new ComputeStatsStmt(table); :}
  | KW_COMPUTE KW_INCREMENTAL KW_STATS table_name:table
  {: RESULT = new ComputeStatsStmt(table, true, null); :}
  | KW_COMPUTE KW_INCREMENTAL KW_STATS table_name:table partition_spec:spec
  {: RESULT = new ComputeStatsStmt(table, true, spec); :}
  ;

drop_stats_stmt ::=
  KW_DROP KW_STATS table_name:table
  {: RESULT = new DropStatsStmt(table); :}
  | KW_DROP KW_INCREMENTAL KW_STATS table_name:table partition_spec:spec
  {: RESULT = new DropStatsStmt(table, spec); :}
  ;

drop_db_stmt ::=
  KW_DROP db_or_schema_kw if_exists_val:if_exists IDENT:db_name cascade_val:cascade
  {: RESULT = new DropDbStmt(db_name, if_exists, cascade); :}
  ;

drop_tbl_or_view_stmt ::=
  KW_DROP KW_TABLE if_exists_val:if_exists table_name:table purge_val:purge
  {: RESULT = new DropTableOrViewStmt(table, if_exists, true, purge); :}
  | KW_DROP KW_VIEW if_exists_val:if_exists table_name:table
  {: RESULT = new DropTableOrViewStmt(table, if_exists, false, false); :}
  ;

drop_function_stmt ::=
  KW_DROP opt_is_aggregate_fn:is_aggregate KW_FUNCTION
      if_exists_val:if_exists function_name:fn_name
  function_def_args:fn_args
  {: RESULT = new DropFunctionStmt(fn_name, fn_args, if_exists); :}
  ;

drop_data_src_stmt ::=
  KW_DROP KW_DATA source_ident:is_source_id if_exists_val:if_exists IDENT:data_src_name
  {: RESULT = new DropDataSrcStmt(data_src_name, if_exists); :}
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

// A partition spec is a set of static partition key/value pairs. This is a bit
// different than a partition clause in an INSERT statement because that allows
// for dynamic and static partition key/values.
partition_spec ::=
  KW_PARTITION LPAREN static_partition_key_value_list:list RPAREN
  {: RESULT = new PartitionSpec(list); :}
  ;

opt_partition_spec ::=
  partition_spec:partition_spec
  {: RESULT = partition_spec; :}
  | /* Empty */
  {: RESULT = null; :}
  ;

static_partition_key_value_list ::=
  static_partition_key_value:item
  {:
    ArrayList<PartitionKeyValue> list = new ArrayList<PartitionKeyValue>();
    list.add(item);
    RESULT = list;
  :}
  | static_partition_key_value_list:list COMMA static_partition_key_value:item
  {:
    list.add(item);
    RESULT = list;
  :}
  ;

partition_key_value ::=
  // Dynamic partition key values.
  IDENT:column
  {: RESULT = new PartitionKeyValue(column, null); :}
  | static_partition_key_value:partition
  {: RESULT = partition; :}
  ;

static_partition_key_value ::=
  // Static partition key values.
  IDENT:column EQUAL expr:e
  {: RESULT = new PartitionKeyValue(column, e); :}
  ;

function_def_args ::=
  LPAREN RPAREN
  {: RESULT = new FunctionArgs(); :}
  | LPAREN function_def_arg_list:args opt_is_varargs:var_args RPAREN
  {:
    args.setHasVarArgs(var_args);
    RESULT = args;
  :}
  ;

function_def_arg_list ::=
  type_def:type_def
  {:
    FunctionArgs args = new FunctionArgs();
    args.getArgTypeDefs().add(type_def);
    RESULT = args;
  :}
  | function_def_arg_list:args COMMA type_def:type_def
  {:
    args.getArgTypeDefs().add(type_def);
    RESULT = args;
  :}
  ;

opt_is_aggregate_fn ::=
  KW_AGGREGATE
  {: RESULT = true; :}
  |
  {: RESULT = false; :}
  ;

opt_is_varargs ::=
  DOTDOTDOT
  {: RESULT = true; :}
  |
  {: RESULT = false; :}
  ;

opt_aggregate_fn_intermediate_type_def ::=
  KW_INTERMEDIATE type_def:type_def
  {: RESULT = type_def; :}
  |
  {: RESULT = null; :}
  ;

function_def_args_map ::=
  function_def_arg_key:key EQUAL STRING_LITERAL:value
  {:
    HashMap<CreateFunctionStmtBase.OptArg, String> args =
        new HashMap<CreateFunctionStmtBase.OptArg, String>();
    args.put(key, value);
    RESULT = args;
  :}
  | function_def_args_map:args function_def_arg_key:key EQUAL STRING_LITERAL:value
  {:
    if (args.containsKey(key)) throw new Exception("Duplicate argument key: " + key);
    args.put(key, value);
    RESULT = args;
  :}
  |
  {: RESULT = new HashMap<CreateFunctionStmtBase.OptArg, String>(); :}
  ;

// Any keys added here must also be added to the end of the precedence list.
function_def_arg_key ::=
  KW_COMMENT
  {: RESULT = CreateFunctionStmtBase.OptArg.COMMENT; :}
  | KW_SYMBOL
  {: RESULT = CreateFunctionStmtBase.OptArg.SYMBOL; :}
  | KW_PREPARE_FN
  {: RESULT = CreateFunctionStmtBase.OptArg.PREPARE_FN; :}
  | KW_CLOSE_FN
  {: RESULT = CreateFunctionStmtBase.OptArg.CLOSE_FN; :}
  | KW_UPDATE_FN
  {: RESULT = CreateFunctionStmtBase.OptArg.UPDATE_FN; :}
  | KW_INIT_FN
  {: RESULT = CreateFunctionStmtBase.OptArg.INIT_FN; :}
  | KW_SERIALIZE_FN
  {: RESULT = CreateFunctionStmtBase.OptArg.SERIALIZE_FN; :}
  | KW_MERGE_FN
  {: RESULT = CreateFunctionStmtBase.OptArg.MERGE_FN; :}
  | KW_FINALIZE_FN
  {: RESULT = CreateFunctionStmtBase.OptArg.FINALIZE_FN; :}
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
  opt_with_clause:w union_operand_list:operands
  {:
    QueryStmt queryStmt = null;
    if (operands.size() == 1) {
      queryStmt = operands.get(0).getQueryStmt();
    } else {
      queryStmt = new UnionStmt(operands, null, null);
    }
    queryStmt.setWithClause(w);
    RESULT = queryStmt;
  :}
  | opt_with_clause:w union_with_order_by_or_limit:union
  {:
    union.setWithClause(w);
    RESULT = union;
  :}
  ;

opt_with_clause ::=
  KW_WITH with_view_def_list:list
  {: RESULT = new WithClause(list); :}
  | /* empty */
  {: RESULT = null; :}
  ;

with_view_def ::=
  IDENT:alias KW_AS LPAREN query_stmt:query RPAREN
  {: RESULT = new View(alias, query, null); :}
  | STRING_LITERAL:alias KW_AS LPAREN query_stmt:query RPAREN
  {: RESULT = new View(alias, query, null); :}
  | IDENT:alias LPAREN ident_list:col_names RPAREN KW_AS LPAREN query_stmt:query RPAREN
  {: RESULT = new View(alias, query, col_names); :}
  | STRING_LITERAL:alias LPAREN ident_list:col_names RPAREN
    KW_AS LPAREN query_stmt:query RPAREN
  {: RESULT = new View(alias, query, col_names); :}
  ;

with_view_def_list ::=
  with_view_def:v
  {:
    ArrayList<View> list = new ArrayList<View>();
    list.add(v);
    RESULT = list;
  :}
  | with_view_def_list:list COMMA with_view_def:v
  {:
    list.add(v);
    RESULT = list;
  :}
  ;

// We must have a non-empty order by or limit for them to bind to the union.
// We cannot reuse the existing opt_order_by_clause or
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
    opt_offset_param:offsetExpr
  {:
    if (operands.size() == 1) {
      parser.parseError("order", SqlParserSymbols.KW_ORDER);
    }
    RESULT = new UnionStmt(operands, orderByClause, new LimitElement(null, offsetExpr));
  :}
  |
    union_operand_list:operands
    KW_LIMIT expr:limitExpr
  {:
    if (operands.size() == 1) {
      parser.parseError("limit", SqlParserSymbols.KW_LIMIT);
    }
    RESULT = new UnionStmt(operands, null, new LimitElement(limitExpr, null));
  :}
  |
    union_operand_list:operands
    KW_ORDER KW_BY order_by_elements:orderByClause
    KW_LIMIT expr:limitExpr opt_offset_param:offsetExpr
  {:
    if (operands.size() == 1) {
      parser.parseError("order", SqlParserSymbols.KW_ORDER);
    }
    RESULT = new UnionStmt(operands, orderByClause,
        new LimitElement(limitExpr, offsetExpr));
  :}
  ;

union_operand ::=
  select_stmt:select
  {: RESULT = select; :}
  | values_stmt:values
  {: RESULT = values; :}
  | LPAREN query_stmt:query RPAREN
  {: RESULT = query; :}
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

values_stmt ::=
  KW_VALUES values_operand_list:operands
  opt_order_by_clause:orderByClause
  opt_limit_offset_clause:limitOffsetClause
  {:
    RESULT = new ValuesStmt(operands, orderByClause, limitOffsetClause);
  :}
  | KW_VALUES LPAREN values_operand_list:operands RPAREN
    opt_order_by_clause:orderByClause
    opt_limit_offset_clause:limitOffsetClause
  {:
    RESULT = new ValuesStmt(operands, orderByClause, limitOffsetClause);
  :}
  ;

values_operand_list ::=
  LPAREN select_list:selectList RPAREN
  {:
    List<UnionOperand> operands = new ArrayList<UnionOperand>();
    operands.add(new UnionOperand(
        new SelectStmt(selectList, null, null, null, null, null, null), null));
    RESULT = operands;
  :}
  | values_operand_list:operands COMMA LPAREN select_list:selectList RPAREN
  {:
    operands.add(new UnionOperand(
        new SelectStmt(selectList, null, null, null, null, null, null), Qualifier.ALL));
    RESULT = operands;
  :}
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

show_stats_stmt ::=
  KW_SHOW KW_TABLE KW_STATS table_name:table
  {: RESULT = new ShowStatsStmt(table, false); :}
  | KW_SHOW KW_COLUMN KW_STATS table_name:table
  {: RESULT = new ShowStatsStmt(table, true); :}
  ;

show_partitions_stmt ::=
  KW_SHOW KW_PARTITIONS table_name:table
  {: RESULT = new ShowPartitionsStmt(table); :}
  ;

show_functions_stmt ::=
  KW_SHOW opt_function_category:fn_type KW_FUNCTIONS
  {: RESULT = new ShowFunctionsStmt(null, null, fn_type); :}
  | KW_SHOW opt_function_category:fn_type KW_FUNCTIONS show_pattern:showPattern
  {: RESULT = new ShowFunctionsStmt(null, showPattern, fn_type); :}
  | KW_SHOW opt_function_category:fn_type KW_FUNCTIONS KW_IN IDENT:db
  {: RESULT = new ShowFunctionsStmt(db, null, fn_type); :}
  | KW_SHOW opt_function_category:fn_type KW_FUNCTIONS KW_IN IDENT:db
      show_pattern:showPattern
  {: RESULT = new ShowFunctionsStmt(db, showPattern, fn_type); :}
  ;

opt_function_category ::=
  KW_AGGREGATE
  {: RESULT = TFunctionCategory.AGGREGATE; :}
  | KW_ANALYTIC
  {: RESULT = TFunctionCategory.ANALYTIC; :}
  | /* empty */
  {: RESULT = TFunctionCategory.SCALAR; :}
  ;

show_data_srcs_stmt ::=
  KW_SHOW KW_DATA sources_ident:is_sources_id
  {: RESULT = new ShowDataSrcsStmt(); :}
  | KW_SHOW KW_DATA sources_ident:is_sources_id show_pattern:showPattern
  {: RESULT = new ShowDataSrcsStmt(showPattern); :}
  ;

show_pattern ::=
  STRING_LITERAL:showPattern
  {: RESULT = showPattern; :}
  | KW_LIKE STRING_LITERAL:showPattern
  {: RESULT = showPattern; :}
  ;

show_create_tbl_stmt ::=
  KW_SHOW KW_CREATE KW_TABLE table_name:table
  {: RESULT = new ShowCreateTableStmt(table); :}
  ;

show_files_stmt ::=
  KW_SHOW KW_FILES KW_IN table_name:table opt_partition_spec:partition
  {: RESULT = new ShowFilesStmt(table, partition); :}
  ;

describe_stmt ::=
  KW_DESCRIBE describe_output_style:style dotted_path:path
  {: RESULT = new DescribeStmt(path, style); :}
  ;

describe_output_style ::=
  KW_FORMATTED
  {: RESULT = TDescribeTableOutputStyle.FORMATTED; :}
  | /* empty */
  {: RESULT = TDescribeTableOutputStyle.MINIMAL; :}
  ;

select_stmt ::=
    select_clause:selectList
  {:
    RESULT = new SelectStmt(selectList, null, null, null, null, null, null);
  :}
  |
    select_clause:selectList
    from_clause:tableRefList
    where_clause:wherePredicate
    group_by_clause:groupingExprs
    having_clause:havingPredicate
    opt_order_by_clause:orderByClause
    opt_limit_offset_clause:limitOffsetClause
  {:
    RESULT = new SelectStmt(selectList, tableRefList, wherePredicate, groupingExprs,
                            havingPredicate, orderByClause, limitOffsetClause);
  :}
  ;

select_clause ::=
  KW_SELECT opt_plan_hints:hints select_list:l
  {:
    l.setPlanHints(hints);
    RESULT = l;
  :}
  | KW_SELECT KW_ALL opt_plan_hints:hints select_list:l
  {:
    l.setPlanHints(hints);
    RESULT = l;
  :}
  | KW_SELECT KW_DISTINCT opt_plan_hints:hints select_list:l
  {:
    l.setIsDistinct(true);
    l.setPlanHints(hints);
    RESULT = l;
  :}
  ;

set_stmt ::=
  KW_SET IDENT:key EQUAL literal:l
  {: RESULT = new SetStmt(key, l.getStringValue()); :}
  | KW_SET IDENT:key EQUAL IDENT:ident
  {: RESULT = new SetStmt(key, ident); :}
  | KW_SET
  {: RESULT = new SetStmt(null, null); :}
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
  | star_expr:expr
  {: RESULT = expr; :}
  ;

alias_clause ::=
  KW_AS IDENT:ident
  {: RESULT = ident; :}
  | IDENT:ident
  {: RESULT = ident; :}
  | KW_AS STRING_LITERAL:l
  {: RESULT = l; :}
  | STRING_LITERAL:l
  {: RESULT = l; :}
  ;

star_expr ::=
  STAR
  {: RESULT = SelectListItem.createStarItem(null); :}
  | dotted_path:path DOT STAR
  {: RESULT = SelectListItem.createStarItem(path); :}
  ;

table_name ::=
  IDENT:tbl
  {: RESULT = new TableName(null, tbl); :}
  | IDENT:db DOT IDENT:tbl
  {: RESULT = new TableName(db, tbl); :}
  ;

function_name ::=
  // Use 'dotted_path' to avoid a reduce/reduce with slot_ref.
  dotted_path:path
  {: RESULT = new FunctionName(path); :}
  ;

from_clause ::=
  KW_FROM table_ref_list:l
  {: RESULT = l; :}
  ;

table_ref_list ::=
  table_ref:table
  {:
    ArrayList<TableRef> list = new ArrayList<TableRef>();
    list.add(table);
    RESULT = list;
  :}
  | table_ref_list:list COMMA table_ref:table
  {:
    list.add(table);
    RESULT = list;
  :}
  | table_ref_list:list KW_CROSS KW_JOIN opt_plan_hints:hints table_ref:table
  {:
    table.setJoinOp(JoinOperator.CROSS_JOIN);
    // We will throw an AnalysisException if there are join hints so that we can provide
    // a better error message than a parser exception.
    table.setJoinHints(hints);
    list.add(table);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op opt_plan_hints:hints table_ref:table
  {:
    table.setJoinOp((JoinOperator) op);
    table.setJoinHints(hints);
    list.add(table);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op opt_plan_hints:hints table_ref:table
    KW_ON expr:e
  {:
    table.setJoinOp((JoinOperator) op);
    table.setJoinHints(hints);
    table.setOnClause(e);
    list.add(table);
    RESULT = list;
  :}
  | table_ref_list:list join_operator:op opt_plan_hints:hints table_ref:table
    KW_USING LPAREN ident_list:colNames RPAREN
  {:
    table.setJoinOp((JoinOperator) op);
    table.setJoinHints(hints);
    table.setUsingClause(colNames);
    list.add(table);
    RESULT = list;
  :}
  ;

table_ref ::=
  dotted_path:path
  {: RESULT = new TableRef(path, null); :}
  | dotted_path:path alias_clause:alias
  {: RESULT = new TableRef(path, alias); :}
  | LPAREN query_stmt:query RPAREN alias_clause:alias
  {: RESULT = new InlineViewRef(alias, query); :}
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
  | KW_RIGHT KW_SEMI KW_JOIN
  {: RESULT = JoinOperator.RIGHT_SEMI_JOIN; :}
  | KW_LEFT KW_ANTI KW_JOIN
  {: RESULT = JoinOperator.LEFT_ANTI_JOIN; :}
  | KW_RIGHT KW_ANTI KW_JOIN
  {: RESULT = JoinOperator.RIGHT_ANTI_JOIN; :}
  ;

opt_inner ::=
  KW_INNER
  |
  ;

opt_outer ::=
  KW_OUTER
  |
  ;

opt_plan_hints ::=
  COMMENTED_PLAN_HINTS:l
  {:
    ArrayList<String> hints = new ArrayList<String>();
    String[] tokens = l.split(",");
    for (String token: tokens) {
      String trimmedToken = token.trim();
      if (trimmedToken.length() > 0) hints.add(trimmedToken);
    }
    RESULT = hints;
  :}
  /* legacy straight_join hint style */
  | KW_STRAIGHT_JOIN
  {:
    ArrayList<String> hints = new ArrayList<String>();
    hints.add("straight_join");
    RESULT = hints;
  :}
  /* legacy plan-hint style */
  | LBRACKET ident_list:l RBRACKET
  {: RESULT = l; :}
  | /* empty */
  {: RESULT = null; :}
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
  KW_WHERE expr:e
  {: RESULT = e; :}
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
  KW_HAVING expr:e
  {: RESULT = e; :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_order_by_clause ::=
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
  expr:e opt_order_param:o opt_nulls_order_param:n
  {: RESULT = new OrderByElement(e, o, n); :}
  ;

opt_order_param ::=
  KW_ASC
  {: RESULT = true; :}
  | KW_DESC
  {: RESULT = false; :}
  | /* empty */
  {: RESULT = true; :}
  ;

opt_nulls_order_param ::=
  KW_NULLS KW_FIRST
  {: RESULT = true; :}
  | KW_NULLS KW_LAST
  {: RESULT = false; :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_offset_param ::=
  KW_OFFSET expr:e
  {: RESULT = e; :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_limit_offset_clause ::=
  opt_limit_clause:limitExpr opt_offset_clause:offsetExpr
  {: RESULT = new LimitElement(limitExpr, offsetExpr); :}
  ;

opt_limit_clause ::=
  KW_LIMIT expr:limitExpr
  {: RESULT = limitExpr; :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_offset_clause ::=
  KW_OFFSET expr:offsetExpr
  {: RESULT = offsetExpr; :}
  | /* empty */
  {: RESULT = null; :}
  ;

cast_expr ::=
  KW_CAST LPAREN expr:e KW_AS type_def:targetType RPAREN
  {: RESULT = new CastExpr(targetType, e); :}
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
  | case_when_clause_list:list KW_WHEN expr:whenExpr
    KW_THEN expr:thenExpr
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

sign_chain_expr ::=
  SUBTRACT expr:e
  {:
    // integrate signs into literals
    // integer literals require analysis to set their type, so the instance check below
    // is not equivalent to e.getType().isNumericType()
    if (e.isLiteral() && e instanceof NumericLiteral) {
      ((LiteralExpr)e).swapSign();
      RESULT = e;
    } else {
      RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.MULTIPLY,
                                  new NumericLiteral(BigDecimal.valueOf(-1)), e);
    }
  :}
  | ADD expr:e
  {: RESULT = e; :}
  ;

expr ::=
  non_pred_expr:e
  {: RESULT = e; :}
  | predicate:p
  {: RESULT = p; :}
  ;

exists_predicate ::=
  KW_EXISTS subquery:s
  {: RESULT = new ExistsPredicate(s, false); :}
  ;

non_pred_expr ::=
  sign_chain_expr:e
  {: RESULT = e; :}
  | literal:l
  {: RESULT = l; :}
  | function_call_expr:e
  {: RESULT = e; :}
  | analytic_expr:e
  {: RESULT = e; :}
  /* Since "IF", "TRUNCATE" are keywords, need to special case these functions */
  | KW_IF LPAREN expr_list:exprs RPAREN
  {: RESULT = new FunctionCallExpr("if", exprs); :}
  | KW_TRUNCATE LPAREN expr_list:exprs RPAREN
  {: RESULT = new FunctionCallExpr("truncate", exprs); :}
  | cast_expr:c
  {: RESULT = c; :}
  | case_expr:c
  {: RESULT = c; :}
  | slot_ref:c
  {: RESULT = c; :}
  | timestamp_arithmetic_expr:e
  {: RESULT = e; :}
  | arithmetic_expr:e
  {: RESULT = e; :}
  | LPAREN non_pred_expr:e RPAREN
  {:
    e.setPrintSqlInParens(true);
    RESULT = e;
  :}
  | subquery:s
  {: RESULT = s; :}
  ;

function_call_expr ::=
  function_name:fn_name LPAREN RPAREN
  {:
    RESULT = FunctionCallExpr.createExpr(
        fn_name, new FunctionParams(new ArrayList<Expr>()));
  :}
  | function_name:fn_name LPAREN function_params:params RPAREN
  {: RESULT = FunctionCallExpr.createExpr(fn_name, params); :}
  // Below is a special case for EXTRACT. Idents are used to avoid adding new keywords.
  | function_name:fn_name LPAREN IDENT:u KW_FROM expr:t RPAREN
  {:  RESULT = new ExtractFromExpr(fn_name, u, t); :}
  ;

// TODO: allow an arbitrary expr here instead of agg/fn call, and check during analysis?
// The parser errors aren't particularly easy to parse.
analytic_expr ::=
  function_call_expr:e KW_OVER
    LPAREN opt_partition_by_clause:p opt_order_by_clause:o opt_window_clause:w RPAREN
  {:
    // Handle cases where function_call_expr resulted in a plain Expr
    if (!(e instanceof FunctionCallExpr)) {
      parser.parseError("over", SqlParserSymbols.KW_OVER);
    }
    FunctionCallExpr f = (FunctionCallExpr)e;
    f.setIsAnalyticFnCall(true);
    RESULT = new AnalyticExpr(f, p, o, w);
  :}
  %prec KW_OVER
  ;

opt_partition_by_clause ::=
  KW_PARTITION KW_BY expr_list:l
  {: RESULT = l; :}
  | /* empty */
  {: RESULT = null; :}
  ;

opt_window_clause ::=
  window_type:t window_boundary:b
  {: RESULT = new AnalyticWindow(t, b); :}
  | window_type:t KW_BETWEEN window_boundary:l KW_AND window_boundary:r
  {: RESULT = new AnalyticWindow(t, l, r); :}
  | /* empty */
  {: RESULT = null; :}
  ;

window_type ::=
  KW_ROWS
  {: RESULT = AnalyticWindow.Type.ROWS; :}
  | KW_RANGE
  {: RESULT = AnalyticWindow.Type.RANGE; :}
  ;

window_boundary ::=
  KW_UNBOUNDED KW_PRECEDING
  {:
    RESULT = new AnalyticWindow.Boundary(
        AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING, null);
  :}
  | KW_UNBOUNDED KW_FOLLOWING
  {:
    RESULT = new AnalyticWindow.Boundary(
        AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING, null);
  :}
  | KW_CURRENT KW_ROW
  {:
    RESULT = new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null);
  :}
  | expr:e KW_PRECEDING
  {: RESULT = new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.PRECEDING, e); :}
  | expr:e KW_FOLLOWING
  {: RESULT = new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.FOLLOWING, e); :}
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
  | expr:e NOT
  {: RESULT = new ArithmeticExpr(ArithmeticExpr.Operator.FACTORIAL, e, null); :}
  %prec FACTORIAL
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
  // We use expr_list instead of expr to avoid a shift/reduce conflict with
  // expr_list on COMMA, and report an error if the list contains more than one expr.
  // Although we don't want to accept function names as the expr, we can't parse it
  // as just an IDENT due to the precedence conflict with function_name.
  | function_name:functionName LPAREN expr_list:l COMMA
    KW_INTERVAL expr:v IDENT:u RPAREN
  {:
    if (l.size() > 1) {
      // Report parsing failure on keyword interval.
      parser.parseError("interval", SqlParserSymbols.KW_INTERVAL);
    }
    ArrayList<String> fnNamePath = functionName.getFnNamePath();
    if (fnNamePath.size() > 1) {
      // This production should not accept fully qualified function names
      throw new Exception("interval should not be qualified by database name");
    }
    RESULT = new TimestampArithmeticExpr(fnNamePath.get(0), l.get(0), v, u);
  :}
  ;

literal ::=
  INTEGER_LITERAL:l
  {: RESULT = new NumericLiteral(l); :}
  | DECIMAL_LITERAL:l
  {: RESULT = new NumericLiteral(l); :}
  | STRING_LITERAL:l
  {: RESULT = new StringLiteral(l); :}
  | KW_TRUE
  {: RESULT = new BoolLiteral(true); :}
  | KW_FALSE
  {: RESULT = new BoolLiteral(false); :}
  | KW_NULL
  {: RESULT = new NullLiteral(); :}
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

function_params ::=
  STAR
  {: RESULT = FunctionParams.createStarParam(); :}
  | KW_ALL STAR
  {: RESULT = FunctionParams.createStarParam(); :}
  | expr_list:exprs
  {: RESULT = new FunctionParams(false, exprs); :}
  | KW_ALL expr_list:exprs
  {: RESULT = new FunctionParams(false, exprs); :}
  | KW_DISTINCT:distinct expr_list:exprs
  {: RESULT = new FunctionParams(true, exprs); :}
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
  | exists_predicate:p
  {: RESULT = p; :}
  | like_predicate:p
  {: RESULT = p; :}
  | LPAREN predicate:p RPAREN
  {:
    p.setPrintSqlInParens(true);
    RESULT = p;
  :}
  ;

comparison_predicate ::=
  expr:e1 EQUAL expr:e2
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.EQ, e1, e2); :}
  | expr:e1 NOTEQUAL expr:e2 // single != token
  {: RESULT = new BinaryPredicate(BinaryPredicate.Operator.NE, e1, e2); :}
  | expr:e1 NOT EQUAL expr:e2 // separate ! and = tokens
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

// Avoid a reduce/reduce conflict with compound_predicate by explicitly
// using non_pred_expr and predicate separately instead of expr.
between_predicate ::=
  expr:e1 KW_BETWEEN non_pred_expr:e2 KW_AND expr:e3
  {: RESULT = new BetweenPredicate(e1, e2, e3, false); :}
  | expr:e1 KW_BETWEEN predicate:e2 KW_AND expr:e3
  {: RESULT = new BetweenPredicate(e1, e2, e3, false); :}
  | expr:e1 KW_NOT KW_BETWEEN non_pred_expr:e2 KW_AND expr:e3
  {: RESULT = new BetweenPredicate(e1, e2, e3, true); :}
  | expr:e1 KW_NOT KW_BETWEEN predicate:e2 KW_AND expr:e3
  {: RESULT = new BetweenPredicate(e1, e2, e3, true); :}
  ;

in_predicate ::=
  expr:e KW_IN LPAREN expr_list:l RPAREN
  {: RESULT = new InPredicate(e, l, false); :}
  | expr:e KW_NOT KW_IN LPAREN expr_list:l RPAREN
  {: RESULT = new InPredicate(e, l, true); :}
  | expr:e KW_IN subquery:s
  {: RESULT = new InPredicate(e, s, false); :}
  | expr:e KW_NOT KW_IN subquery:s
  {: RESULT = new InPredicate(e, s, true); :}
  ;

subquery ::=
  LPAREN subquery:s RPAREN
  {: RESULT = s; :}
  | LPAREN query_stmt:s RPAREN
  {: RESULT = new Subquery(s); :}
  ;

compound_predicate ::=
  expr:e1 KW_AND expr:e2
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.AND, e1, e2); :}
  | expr:e1 KW_OR expr:e2
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.OR, e1, e2); :}
  | KW_NOT expr:e
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.NOT, e, null); :}
  | NOT expr:e
  {: RESULT = new CompoundPredicate(CompoundPredicate.Operator.NOT, e, null); :}
  ;

slot_ref ::=
  dotted_path:path
  {: RESULT = new SlotRef(path); :}
  ;

dotted_path ::=
  IDENT:ident
  {:
    ArrayList<String> list = new ArrayList<String>();
    list.add(ident);
    RESULT = list;
  :}
  | dotted_path:list DOT IDENT:ident
  {:
    list.add(ident);
    RESULT = list;
  :}
  ;

type_def ::=
  type:t
  {: RESULT = new TypeDef(t); :}
  ;

type ::=
  KW_TINYINT
  {: RESULT = Type.TINYINT; :}
  | KW_SMALLINT
  {: RESULT = Type.SMALLINT; :}
  | KW_INT
  {: RESULT = Type.INT; :}
  | KW_BIGINT
  {: RESULT = Type.BIGINT; :}
  | KW_BOOLEAN
  {: RESULT = Type.BOOLEAN; :}
  | KW_FLOAT
  {: RESULT = Type.FLOAT; :}
  | KW_DOUBLE
  {: RESULT = Type.DOUBLE; :}
  | KW_DATE
  {: RESULT = Type.DATE; :}
  | KW_DATETIME
  {: RESULT = Type.DATETIME; :}
  | KW_TIMESTAMP
  {: RESULT = Type.TIMESTAMP; :}
  | KW_STRING
  {: RESULT = Type.STRING; :}
  | KW_VARCHAR LPAREN INTEGER_LITERAL:len RPAREN
  {: RESULT = ScalarType.createVarcharType(len.intValue()); :}
  | KW_VARCHAR
  {: RESULT = Type.STRING; :}
  | KW_BINARY
  {: RESULT = Type.BINARY; :}
  | KW_CHAR LPAREN INTEGER_LITERAL:len RPAREN
  {: RESULT = ScalarType.createCharType(len.intValue()); :}
  | KW_DECIMAL LPAREN INTEGER_LITERAL:precision RPAREN
  {: RESULT = ScalarType.createDecimalType(precision.intValue()); :}
  | KW_DECIMAL LPAREN INTEGER_LITERAL:precision COMMA INTEGER_LITERAL:scale RPAREN
  {: RESULT = ScalarType.createDecimalType(precision.intValue(), scale.intValue()); :}
  | KW_DECIMAL
  {: RESULT = ScalarType.createDecimalType(); :}
  | KW_ARRAY LESSTHAN type:value_type GREATERTHAN
  {: RESULT = new ArrayType(value_type); :}
  | KW_MAP LESSTHAN type:key_type COMMA type:value_type GREATERTHAN
  {: RESULT = new MapType(key_type, value_type); :}
  | KW_STRUCT LESSTHAN struct_field_def_list:fields GREATERTHAN
  {: RESULT = new StructType(fields); :}
  ;

struct_field_def ::=
  IDENT:name COLON type:t comment_val:comment
  {: RESULT = new StructField(name, t, comment); :}
  ;

struct_field_def_list ::=
  struct_field_def:field_def
  {:
    ArrayList<StructField> list = new ArrayList<StructField>();
    list.add(field_def);
    RESULT = list;
  :}
  | struct_field_def_list:list COMMA struct_field_def:field_def
  {:
    list.add(field_def);
    RESULT = list;
  :}
  ;
