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

import java_cup.runtime.Symbol;
import java.lang.Integer;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Iterator;
import java.util.Arrays;
import java.util.HashSet;

import com.google.common.base.Preconditions;
import org.apache.impala.analysis.SqlParserSymbols;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TReservedWordsVersion;

%%

%class SqlScanner
%cup
%public
%final
%eofval{
  return newToken(SqlParserSymbols.EOF, null);
%eofval}
%unicode
%line
%column
%{
  // Map from keyword string to token id.
  // We use a linked hash map because the insertion order is important.
  // for example, we want "and" to come after "&&" to make sure error reporting
  // uses "and" as a display name and not "&&".
  // Please keep the puts sorted alphabetically by keyword (where the order
  // does not affect the desired error reporting)
  static Map<String, Integer> keywordMap;
  // Reserved words are words that cannot be used as identifiers. It is a superset of
  // keywords.
  static Set<String> reservedWords;
  // map from token id to token description
  static HashMap<Integer, String> tokenIdMap;

  public static void init(TReservedWordsVersion reservedWordsVersion) {
    // initilize keywords
    keywordMap = new LinkedHashMap<>();
    keywordMap.put("&&", SqlParserSymbols.KW_AND);
    keywordMap.put("add", SqlParserSymbols.KW_ADD);
    keywordMap.put("aggregate", SqlParserSymbols.KW_AGGREGATE);
    keywordMap.put("all", SqlParserSymbols.KW_ALL);
    keywordMap.put("alter", SqlParserSymbols.KW_ALTER);
    keywordMap.put("analytic", SqlParserSymbols.KW_ANALYTIC);
    keywordMap.put("and", SqlParserSymbols.KW_AND);
    keywordMap.put("anti", SqlParserSymbols.KW_ANTI);
    keywordMap.put("api_version", SqlParserSymbols.KW_API_VERSION);
    keywordMap.put("array", SqlParserSymbols.KW_ARRAY);
    keywordMap.put("as", SqlParserSymbols.KW_AS);
    keywordMap.put("asc", SqlParserSymbols.KW_ASC);
    keywordMap.put("authorization", SqlParserSymbols.KW_AUTHORIZATION);
    keywordMap.put("avro", SqlParserSymbols.KW_AVRO);
    keywordMap.put("between", SqlParserSymbols.KW_BETWEEN);
    keywordMap.put("bigint", SqlParserSymbols.KW_BIGINT);
    keywordMap.put("binary", SqlParserSymbols.KW_BINARY);
    keywordMap.put("block_size", SqlParserSymbols.KW_BLOCKSIZE);
    keywordMap.put("boolean", SqlParserSymbols.KW_BOOLEAN);
    keywordMap.put("buckets", SqlParserSymbols.KW_BUCKETS);
    keywordMap.put("by", SqlParserSymbols.KW_BY);
    keywordMap.put("cached", SqlParserSymbols.KW_CACHED);
    keywordMap.put("cascade", SqlParserSymbols.KW_CASCADE);
    keywordMap.put("case", SqlParserSymbols.KW_CASE);
    keywordMap.put("cast", SqlParserSymbols.KW_CAST);
    keywordMap.put("change", SqlParserSymbols.KW_CHANGE);
    keywordMap.put("char", SqlParserSymbols.KW_CHAR);
    keywordMap.put("class", SqlParserSymbols.KW_CLASS);
    keywordMap.put("clustered", SqlParserSymbols.KW_CLUSTERED);
    keywordMap.put("close_fn", SqlParserSymbols.KW_CLOSE_FN);
    keywordMap.put("column", SqlParserSymbols.KW_COLUMN);
    keywordMap.put("columns", SqlParserSymbols.KW_COLUMNS);
    keywordMap.put("comment", SqlParserSymbols.KW_COMMENT);
    keywordMap.put("compression", SqlParserSymbols.KW_COMPRESSION);
    keywordMap.put("compute", SqlParserSymbols.KW_COMPUTE);
    keywordMap.put("constraint", SqlParserSymbols.KW_CONSTRAINT);
    keywordMap.put("convert", SqlParserSymbols.KW_CONVERT);
    keywordMap.put("copy", SqlParserSymbols.KW_COPY);
    keywordMap.put("create", SqlParserSymbols.KW_CREATE);
    keywordMap.put("cross", SqlParserSymbols.KW_CROSS);
    keywordMap.put("cube", Integer.valueOf(SqlParserSymbols.KW_CUBE));
    keywordMap.put("current", SqlParserSymbols.KW_CURRENT);
    keywordMap.put("data", SqlParserSymbols.KW_DATA);
    keywordMap.put("database", SqlParserSymbols.KW_DATABASE);
    keywordMap.put("databases", SqlParserSymbols.KW_DATABASES);
    keywordMap.put("date", SqlParserSymbols.KW_DATE);
    keywordMap.put("datetime", SqlParserSymbols.KW_DATETIME);
    keywordMap.put("decimal", SqlParserSymbols.KW_DECIMAL);
    keywordMap.put("default", SqlParserSymbols.KW_DEFAULT);
    keywordMap.put("delete", SqlParserSymbols.KW_DELETE);
    keywordMap.put("delimited", SqlParserSymbols.KW_DELIMITED);
    keywordMap.put("desc", SqlParserSymbols.KW_DESC);
    keywordMap.put("describe", SqlParserSymbols.KW_DESCRIBE);
    keywordMap.put("disable", SqlParserSymbols.KW_DISABLE);
    keywordMap.put("distinct", SqlParserSymbols.KW_DISTINCT);
    keywordMap.put("div", SqlParserSymbols.KW_DIV);
    keywordMap.put("double", SqlParserSymbols.KW_DOUBLE);
    keywordMap.put("drop", SqlParserSymbols.KW_DROP);
    keywordMap.put("else", SqlParserSymbols.KW_ELSE);
    keywordMap.put("enable", SqlParserSymbols.KW_ENABLE);
    keywordMap.put("encoding", SqlParserSymbols.KW_ENCODING);
    keywordMap.put("end", SqlParserSymbols.KW_END);
    keywordMap.put("enforced", SqlParserSymbols.KW_ENFORCED);
    keywordMap.put("escaped", SqlParserSymbols.KW_ESCAPED);
    keywordMap.put("except", SqlParserSymbols.KW_EXCEPT);
    keywordMap.put("exists", SqlParserSymbols.KW_EXISTS);
    keywordMap.put("execute", SqlParserSymbols.KW_EXECUTE);
    keywordMap.put("explain", SqlParserSymbols.KW_EXPLAIN);
    keywordMap.put("extended", SqlParserSymbols.KW_EXTENDED);
    keywordMap.put("external", SqlParserSymbols.KW_EXTERNAL);
    keywordMap.put("false", SqlParserSymbols.KW_FALSE);
    keywordMap.put("fields", SqlParserSymbols.KW_FIELDS);
    keywordMap.put("fileformat", SqlParserSymbols.KW_FILEFORMAT);
    keywordMap.put("files", SqlParserSymbols.KW_FILES);
    keywordMap.put("finalize_fn", SqlParserSymbols.KW_FINALIZE_FN);
    keywordMap.put("first", SqlParserSymbols.KW_FIRST);
    keywordMap.put("float", SqlParserSymbols.KW_FLOAT);
    keywordMap.put("following", SqlParserSymbols.KW_FOLLOWING);
    keywordMap.put("for", SqlParserSymbols.KW_FOR);
    keywordMap.put("foreign", SqlParserSymbols.KW_FOREIGN);
    keywordMap.put("format", SqlParserSymbols.KW_FORMAT);
    keywordMap.put("formatted", SqlParserSymbols.KW_FORMATTED);
    keywordMap.put("from", SqlParserSymbols.KW_FROM);
    keywordMap.put("full", SqlParserSymbols.KW_FULL);
    keywordMap.put("function", SqlParserSymbols.KW_FUNCTION);
    keywordMap.put("functions", SqlParserSymbols.KW_FUNCTIONS);
    keywordMap.put("grant", SqlParserSymbols.KW_GRANT);
    keywordMap.put("group", SqlParserSymbols.KW_GROUP);
    keywordMap.put("grouping", Integer.valueOf(SqlParserSymbols.KW_GROUPING));
    keywordMap.put("hash", SqlParserSymbols.KW_HASH);
    keywordMap.put("having", SqlParserSymbols.KW_HAVING);
    keywordMap.put("hudiparquet", SqlParserSymbols.KW_HUDIPARQUET);
    keywordMap.put("iceberg", SqlParserSymbols.KW_ICEBERG);
    keywordMap.put("if", SqlParserSymbols.KW_IF);
    keywordMap.put("ignore", SqlParserSymbols.KW_IGNORE);
    keywordMap.put("ilike", SqlParserSymbols.KW_ILIKE);
    keywordMap.put("in", SqlParserSymbols.KW_IN);
    keywordMap.put("incremental", SqlParserSymbols.KW_INCREMENTAL);
    keywordMap.put("init_fn", SqlParserSymbols.KW_INIT_FN);
    keywordMap.put("inner", SqlParserSymbols.KW_INNER);
    keywordMap.put("inpath", SqlParserSymbols.KW_INPATH);
    keywordMap.put("insert", SqlParserSymbols.KW_INSERT);
    keywordMap.put("int", SqlParserSymbols.KW_INT);
    keywordMap.put("integer", SqlParserSymbols.KW_INT);
    keywordMap.put("intermediate", SqlParserSymbols.KW_INTERMEDIATE);
    keywordMap.put("intersect", SqlParserSymbols.KW_INTERSECT);
    keywordMap.put("interval", SqlParserSymbols.KW_INTERVAL);
    keywordMap.put("into", SqlParserSymbols.KW_INTO);
    keywordMap.put("invalidate", SqlParserSymbols.KW_INVALIDATE);
    keywordMap.put("iregexp", SqlParserSymbols.KW_IREGEXP);
    keywordMap.put("is", SqlParserSymbols.KW_IS);
    keywordMap.put("jdbc", SqlParserSymbols.KW_JDBC);
    keywordMap.put("join", SqlParserSymbols.KW_JOIN);
    keywordMap.put("jsonfile", SqlParserSymbols.KW_JSONFILE);
    keywordMap.put("kudu", SqlParserSymbols.KW_KUDU);
    keywordMap.put("last", SqlParserSymbols.KW_LAST);
    keywordMap.put("left", SqlParserSymbols.KW_LEFT);
    keywordMap.put("lexical", SqlParserSymbols.KW_LEXICAL);
    keywordMap.put("like", SqlParserSymbols.KW_LIKE);
    keywordMap.put("limit", SqlParserSymbols.KW_LIMIT);
    keywordMap.put("lines", SqlParserSymbols.KW_LINES);
    keywordMap.put("load", SqlParserSymbols.KW_LOAD);
    keywordMap.put("location", SqlParserSymbols.KW_LOCATION);
    keywordMap.put("managedlocation", SqlParserSymbols.KW_MANAGED_LOCATION);
    keywordMap.put("map", SqlParserSymbols.KW_MAP);
    keywordMap.put("merge_fn", SqlParserSymbols.KW_MERGE_FN);
    keywordMap.put("metadata", SqlParserSymbols.KW_METADATA);
    keywordMap.put("minus", SqlParserSymbols.KW_MINUS);
    keywordMap.put("non", SqlParserSymbols.KW_NON);
    keywordMap.put("norely", SqlParserSymbols.KW_NORELY);
    keywordMap.put("not", SqlParserSymbols.KW_NOT);
    keywordMap.put("novalidate", SqlParserSymbols.KW_NOVALIDATE);
    keywordMap.put("null", SqlParserSymbols.KW_NULL);
    keywordMap.put("nulls", SqlParserSymbols.KW_NULLS);
    keywordMap.put("of", SqlParserSymbols.KW_OF);
    keywordMap.put("offset", SqlParserSymbols.KW_OFFSET);
    keywordMap.put("on", SqlParserSymbols.KW_ON);
    keywordMap.put("optimize", SqlParserSymbols.KW_OPTIMIZE);
    keywordMap.put("or", SqlParserSymbols.KW_OR);
    keywordMap.put("||", SqlParserSymbols.KW_LOGICAL_OR);
    keywordMap.put("orc", SqlParserSymbols.KW_ORC);
    keywordMap.put("order", SqlParserSymbols.KW_ORDER);
    keywordMap.put("outer", SqlParserSymbols.KW_OUTER);
    keywordMap.put("over", SqlParserSymbols.KW_OVER);
    keywordMap.put("overwrite", SqlParserSymbols.KW_OVERWRITE);
    keywordMap.put("parquet", SqlParserSymbols.KW_PARQUET);
    keywordMap.put("parquetfile", SqlParserSymbols.KW_PARQUETFILE);
    keywordMap.put("partition", SqlParserSymbols.KW_PARTITION);
    keywordMap.put("partitioned", SqlParserSymbols.KW_PARTITIONED);
    keywordMap.put("partitions", SqlParserSymbols.KW_PARTITIONS);
    keywordMap.put("preceding", SqlParserSymbols.KW_PRECEDING);
    keywordMap.put("prepare_fn", SqlParserSymbols.KW_PREPARE_FN);
    keywordMap.put("primary", SqlParserSymbols.KW_PRIMARY);
    keywordMap.put("produced", SqlParserSymbols.KW_PRODUCED);
    keywordMap.put("purge", SqlParserSymbols.KW_PURGE);
    keywordMap.put("range", SqlParserSymbols.KW_RANGE);
    keywordMap.put("rcfile", SqlParserSymbols.KW_RCFILE);
    keywordMap.put("real", SqlParserSymbols.KW_DOUBLE);
    keywordMap.put("recover", SqlParserSymbols.KW_RECOVER);
    keywordMap.put("references", SqlParserSymbols.KW_REFERENCES);
    keywordMap.put("refresh", SqlParserSymbols.KW_REFRESH);
    keywordMap.put("regexp", SqlParserSymbols.KW_REGEXP);
    keywordMap.put("rely", SqlParserSymbols.KW_RELY);
    keywordMap.put("rename", SqlParserSymbols.KW_RENAME);
    keywordMap.put("repeatable", SqlParserSymbols.KW_REPEATABLE);
    keywordMap.put("replace", SqlParserSymbols.KW_REPLACE);
    keywordMap.put("replication", SqlParserSymbols.KW_REPLICATION);
    keywordMap.put("restrict", SqlParserSymbols.KW_RESTRICT);
    keywordMap.put("returns", SqlParserSymbols.KW_RETURNS);
    keywordMap.put("revoke", SqlParserSymbols.KW_REVOKE);
    keywordMap.put("right", SqlParserSymbols.KW_RIGHT);
    keywordMap.put("rlike", SqlParserSymbols.KW_RLIKE);
    keywordMap.put("role", SqlParserSymbols.KW_ROLE);
    keywordMap.put("roles", SqlParserSymbols.KW_ROLES);
    keywordMap.put("rollup", Integer.valueOf(SqlParserSymbols.KW_ROLLUP));
    keywordMap.put("row", SqlParserSymbols.KW_ROW);
    keywordMap.put("rows", SqlParserSymbols.KW_ROWS);
    keywordMap.put("rwstorage", SqlParserSymbols.KW_RWSTORAGE);
    keywordMap.put("schema", SqlParserSymbols.KW_SCHEMA);
    keywordMap.put("schemas", SqlParserSymbols.KW_SCHEMAS);
    keywordMap.put("select", SqlParserSymbols.KW_SELECT);
    keywordMap.put("selectivity", SqlParserSymbols.KW_SELECTIVITY);
    keywordMap.put("semi", SqlParserSymbols.KW_SEMI);
    keywordMap.put("sequencefile", SqlParserSymbols.KW_SEQUENCEFILE);
    keywordMap.put("serdeproperties", SqlParserSymbols.KW_SERDEPROPERTIES);
    keywordMap.put("serialize_fn", SqlParserSymbols.KW_SERIALIZE_FN);
    keywordMap.put("set", SqlParserSymbols.KW_SET);
    keywordMap.put("sets", Integer.valueOf(SqlParserSymbols.KW_SETS));
    keywordMap.put("show", SqlParserSymbols.KW_SHOW);
    keywordMap.put("smallint", SqlParserSymbols.KW_SMALLINT);
    keywordMap.put("sort", SqlParserSymbols.KW_SORT);
    keywordMap.put("spec", SqlParserSymbols.KW_SPEC);
    keywordMap.put("stats", SqlParserSymbols.KW_STATS);
    keywordMap.put("stored", SqlParserSymbols.KW_STORED);
    keywordMap.put("storagehandler_uri", SqlParserSymbols.KW_STORAGE_HANDLER_URI);
    keywordMap.put("straight_join", SqlParserSymbols.KW_STRAIGHT_JOIN);
    keywordMap.put("string", SqlParserSymbols.KW_STRING);
    keywordMap.put("struct", SqlParserSymbols.KW_STRUCT);
    keywordMap.put("symbol", SqlParserSymbols.KW_SYMBOL);
    keywordMap.put("system_time", SqlParserSymbols.KW_SYSTEM_TIME);
    keywordMap.put("system_version", SqlParserSymbols.KW_SYSTEM_VERSION);
    keywordMap.put("table", SqlParserSymbols.KW_TABLE);
    keywordMap.put("tables", SqlParserSymbols.KW_TABLES);
    keywordMap.put("tablesample", SqlParserSymbols.KW_TABLESAMPLE);
    keywordMap.put("tblproperties", SqlParserSymbols.KW_TBLPROPERTIES);
    keywordMap.put("terminated", SqlParserSymbols.KW_TERMINATED);
    keywordMap.put("textfile", SqlParserSymbols.KW_TEXTFILE);
    keywordMap.put("then", SqlParserSymbols.KW_THEN);
    keywordMap.put("timestamp", SqlParserSymbols.KW_TIMESTAMP);
    keywordMap.put("tinyint", SqlParserSymbols.KW_TINYINT);
    keywordMap.put("to", SqlParserSymbols.KW_TO);
    keywordMap.put("true", SqlParserSymbols.KW_TRUE);
    keywordMap.put("truncate", SqlParserSymbols.KW_TRUNCATE);
    keywordMap.put("user_defined_fn", SqlParserSymbols.KW_UDF);
    keywordMap.put("unbounded", SqlParserSymbols.KW_UNBOUNDED);
    keywordMap.put("uncached", SqlParserSymbols.KW_UNCACHED);
    keywordMap.put("union", SqlParserSymbols.KW_UNION);
    keywordMap.put("unique", SqlParserSymbols.KW_UNIQUE);
    keywordMap.put("unknown", SqlParserSymbols.KW_UNKNOWN);
    keywordMap.put("unnest", SqlParserSymbols.KW_UNNEST);
    keywordMap.put("unset", SqlParserSymbols.KW_UNSET);
    keywordMap.put("update", SqlParserSymbols.KW_UPDATE);
    keywordMap.put("update_fn", SqlParserSymbols.KW_UPDATE_FN);
    keywordMap.put("upsert", SqlParserSymbols.KW_UPSERT);
    keywordMap.put("use", SqlParserSymbols.KW_USE);
    keywordMap.put("using", SqlParserSymbols.KW_USING);
    keywordMap.put("validate", SqlParserSymbols.KW_VALIDATE);
    keywordMap.put("values", SqlParserSymbols.KW_VALUES);
    keywordMap.put("varchar", SqlParserSymbols.KW_VARCHAR);
    keywordMap.put("view", SqlParserSymbols.KW_VIEW);
    keywordMap.put("views", SqlParserSymbols.KW_VIEWS);
    keywordMap.put("when", SqlParserSymbols.KW_WHEN);
    keywordMap.put("where", SqlParserSymbols.KW_WHERE);
    keywordMap.put("with", SqlParserSymbols.KW_WITH);
    keywordMap.put("zorder", SqlParserSymbols.KW_ZORDER);

    // Initilize tokenIdMap for error reporting
    tokenIdMap = new HashMap<>();
    for (Map.Entry<String, Integer> entry : keywordMap.entrySet()) {
      tokenIdMap.put(entry.getValue(), entry.getKey().toUpperCase());
    }
    // add non-keyword tokens. Please keep this in the same order as they are used in this
    // file.
    tokenIdMap.put(SqlParserSymbols.EOF, "EOF");
    tokenIdMap.put(SqlParserSymbols.DOTDOTDOT, "...");
    tokenIdMap.put(SqlParserSymbols.COLON, ":");
    tokenIdMap.put(SqlParserSymbols.SEMICOLON, ";");
    tokenIdMap.put(SqlParserSymbols.COMMA, "COMMA");
    tokenIdMap.put(SqlParserSymbols.DOT, ".");
    tokenIdMap.put(SqlParserSymbols.STAR, "*");
    tokenIdMap.put(SqlParserSymbols.LPAREN, "(");
    tokenIdMap.put(SqlParserSymbols.RPAREN, ")");
    tokenIdMap.put(SqlParserSymbols.LBRACKET, "[");
    tokenIdMap.put(SqlParserSymbols.RBRACKET, "]");
    tokenIdMap.put(SqlParserSymbols.DIVIDE, "/");
    tokenIdMap.put(SqlParserSymbols.MOD, "%");
    tokenIdMap.put(SqlParserSymbols.ADD, "+");
    tokenIdMap.put(SqlParserSymbols.SUBTRACT, "-");
    tokenIdMap.put(SqlParserSymbols.BITAND, "&");
    tokenIdMap.put(SqlParserSymbols.BITOR, "|");
    tokenIdMap.put(SqlParserSymbols.BITXOR, "^");
    tokenIdMap.put(SqlParserSymbols.BITNOT, "~");
    tokenIdMap.put(SqlParserSymbols.EQUAL, "=");
    tokenIdMap.put(SqlParserSymbols.NOT, "!");
    tokenIdMap.put(SqlParserSymbols.LESSTHAN, "<");
    tokenIdMap.put(SqlParserSymbols.GREATERTHAN, ">");
    tokenIdMap.put(SqlParserSymbols.UNMATCHED_STRING_LITERAL, "UNMATCHED STRING LITERAL");
    tokenIdMap.put(SqlParserSymbols.NOTEQUAL, "!=");
    tokenIdMap.put(SqlParserSymbols.INTEGER_LITERAL, "INTEGER LITERAL");
    tokenIdMap.put(SqlParserSymbols.NUMERIC_OVERFLOW, "NUMERIC OVERFLOW");
    tokenIdMap.put(SqlParserSymbols.DECIMAL_LITERAL, "DECIMAL LITERAL");
    tokenIdMap.put(SqlParserSymbols.EMPTY_IDENT, "EMPTY IDENTIFIER");
    tokenIdMap.put(SqlParserSymbols.IDENT, "IDENTIFIER");
    tokenIdMap.put(SqlParserSymbols.STRING_LITERAL, "STRING LITERAL");
    tokenIdMap.put(SqlParserSymbols.COMMENTED_PLAN_HINT_START,
        "COMMENTED_PLAN_HINT_START");
    tokenIdMap.put(SqlParserSymbols.COMMENTED_PLAN_HINT_END, "COMMENTED_PLAN_HINT_END");
    tokenIdMap.put(SqlParserSymbols.UNEXPECTED_CHAR, "Unexpected character");
    // There are 4 symbols not in the tokenIdMap:
    // - UNUSED_RESERVED_WORD. It is handled separately in sql-parser.cup
    // - FACTORIAL and UNARYSIGN. These are placeholders to work around precedence.
    // - error. It's a symbol defined by cup.
    Preconditions.checkState(tokenIdMap.size() + 4 ==
        SqlParserSymbols.class.getFields().length, "The sizes of tokenIdMap and " +
        "SqlParserSymbols don't match. sql-scanner.flex should be updated.");

    // Initilize reservedWords. For impala 2.11, reserved words = keywords.
    if (reservedWordsVersion == TReservedWordsVersion.IMPALA_2_11) {
      reservedWords = keywordMap.keySet();
      return;
    }
    // For impala 3.0, reserved words = keywords + sql16ReservedWords - builtinFunctions
    // - whitelist
    // unused reserved words = reserved words - keywords. These words are reserved for
    // forward compatibility purposes.
    reservedWords = new HashSet<>(keywordMap.keySet());
    // Add SQL:2016 reserved words
    reservedWords.addAll(Arrays.asList(new String[] {
        "abs", "acos", "allocate", "any", "are", "array_agg", "array_max_cardinality",
        "asensitive", "asin", "asymmetric", "at", "atan", "atomic", "avg", "begin",
        "begin_frame", "begin_partition", "blob", "both", "call", "called", "cardinality",
        "cascaded", "ceil", "ceiling", "char_length", "character", "character_length",
        "check", "classifier", "clob", "close", "coalesce", "collate", "collect",
        "commit", "condition", "connect", "constraint", "contains", "convert", "copy",
        "corr", "corresponding", "cos", "cosh", "count", "covar_pop", "covar_samp",
        "cube", "cume_dist", "current_catalog", "current_date",
        "current_default_transform_group", "current_path", "current_path", "current_role",
        "current_role", "current_row", "current_schema", "current_time",
        "current_timestamp", "current_transform_group_for_type", "current_user", "cursor",
        "cycle", "day", "deallocate", "dec", "decfloat", "declare", "define",
        "dense_rank", "deref", "deterministic", "disconnect", "dynamic", "each",
        "element", "empty", "end-exec", "end_frame", "end_partition", "equals", "escape",
        "every", "except", "exec", "execute", "exp", "extract", "fetch", "filter",
        "first_value", "floor", "foreign", "frame_row", "free", "fusion", "get", "global",
        "grouping", "groups", "hold", "hour", "identity", "indicator", "initial", "inout",
        "insensitive", "integer", "intersect", "intersection", "json_array",
        "json_arrayagg", "json_exists", "json_object", "json_objectagg", "json_query",
        "json_table", "json_table_primitive", "json_value", "lag", "language", "large",
        "last_value", "lateral", "lead", "leading", "like_regex", "listagg", "ln",
        "local", "localtime", "localtimestamp", "log", "log10 ", "lower", "match",
        "match_number", "match_recognize", "matches", "max", "member", "merge", "method",
        "min", "minute", "mod", "modifies", "module", "month", "multiset", "national",
        "natural", "nchar", "nclob", "new", "no", "none", "normalize", "nth_value",
        "ntile", "nullif", "numeric", "occurrences_regex", "octet_length", "of", "old",
        "omit", "one", "only", "open", "out", "overlaps", "overlay", "parameter",
        "pattern", "per", "percent", "percent_rank", "percentile_cont", "percentile_disc",
        "period", "portion", "position", "position_regex", "power", "precedes",
        "precision", "prepare", "procedure", "ptf", "rank", "reads", "real", "recursive",
        "ref", "references", "referencing", "regr_avgx", "regr_avgy", "regr_count",
        "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy",
        "release", "result", "return", "rollback", "rollup", "row_number", "running",
        "savepoint", "scope", "scroll", "search", "second", "seek", "sensitive",
        "session_user", "similar", "sin", "sinh", "skip", "some", "specific",
        "specifictype", "sql", "sqlexception", "sqlstate", "sqlwarning", "sqrt", "start",
        "static", "stddev_pop", "stddev_samp", "submultiset", "subset", "substring",
        "substring_regex", "succeeds", "sum", "symmetric", "system", "system_time",
        "system_user", "tan", "tanh", "time", "timezone_hour", "timezone_minute",
        "trailing", "translate", "translate_regex", "translation", "treat", "trigger",
        "trim", "trim_array", "uescape", "unknown", "update  ",
        "upper", "user", "value", "value_of", "var_pop", "var_samp", "varbinary",
        "varying", "versioning", "whenever", "width_bucket", "window", "within",
        "without", "year"}));
    // Remove impala builtin function names
    reservedWords.removeAll(BuiltinsDb.getInstance().getAllFunctions().keySet());
    // Remove whitelist words. These words might be heavily used in production, and
    // impala is unlikely to implement SQL features around these words in the near future.
    reservedWords.removeAll(Arrays.asList(new String[] {
        // time units
        "year", "month", "day", "hour", "minute", "second",
        "begin", "call", "check", "classifier", "close", "identity", "language",
        "localtime", "member", "module", "new", "nullif", "old", "open", "parameter",
        "period", "result", "return", "rollback", "sql", "start", "system", "time",
        "user", "value"
    }));
  }

  static {
    // Default-initilize the static members for FE tests. Outside of FE tests, init() is
    // called again in BackendConfig.create() once the backend configuration is passed to
    // the FE, overwriting this initilization.
    init(TReservedWordsVersion.IMPALA_3_0);
  }

  static boolean isReserved(String token) {
    return token != null && reservedWords.contains(token.toLowerCase());
  }

  static boolean isKeyword(Integer tokenId) {
    String token = tokenIdMap.get(tokenId);
    return token != null && keywordMap.containsKey(token.toLowerCase());
  }

  private Symbol newToken(int id, Object value) {
    return new Symbol(id, yyline+1, yycolumn+1, value);
  }
%}

LineTerminator = \r|\n|\r\n
Whitespace = {LineTerminator} | [ \t\f]

// Order of rules to resolve ambiguity:
// The rule for recognizing integer literals must come before the rule for
// double literals to, e.g., recognize "1234" as an integer literal.
// The rule for recognizing double literals must come before the rule for
// identifiers to, e.g., recognize "1e6" as a double literal.
IntegerLiteral = [:digit:][:digit:]*
FLit1 = [0-9]+ \. [0-9]*
FLit2 = \. [0-9]+
FLit3 = [0-9]+
Exponent = [eE] [+-]? [0-9]+
DecimalLiteral = ({FLit1}|{FLit2}|{FLit3}) {Exponent}?

Identifier = [:digit:]*[:jletter:][:jletterdigit:]*
// Without \. {Identifier}, a dot followed by an identifier starting with digits will
// always be lexed to Flit2.
IdentifierOrKw =  {Identifier} | \. {Identifier} | "&&" | "||"
QuotedIdentifier = \`(\\.|[^\\\`])*\`
SingleQuoteStringLiteral = \'(\\.|[^\\\'])*\'
DoubleQuoteStringLiteral = \"(\\.|[^\\\"])*\"

EolHintBegin = "--" " "* "+"
CommentedHintBegin = "/*" " "* "+"
CommentedHintEnd = "*/"

// Both types of plan hints must appear within a single line.
HintContent = " "* "+" [^\r\n]*

Comment = {TraditionalComment} | {EndOfLineComment}

// Match anything that has a comment end (*/) in it.
ContainsCommentEnd = [^]* "*/" [^]*
// Match anything that has a line terminator in it.
ContainsLineTerminator = [^]* {LineTerminator} [^]*

// A traditional comment is anything that starts and ends like a comment and has neither a
// plan hint inside nor a CommentEnd (*/).
TraditionalComment = "/*" !({HintContent}|{ContainsCommentEnd}) "*/"
// Similar for a end-of-line comment.
EndOfLineComment = "--" !({HintContent}|{ContainsLineTerminator}) {LineTerminator}?

// This additional state is needed because newlines signal the end of a end-of-line hint
// if one has been started earlier. Hence we need to discern between newlines within and
// outside of end-of-line hints.
%state EOLHINT

%%
// Put '...' before '.'
"..." { return newToken(SqlParserSymbols.DOTDOTDOT, null); }

// single-character tokens
":" { return newToken(SqlParserSymbols.COLON, null); }
";" { return newToken(SqlParserSymbols.SEMICOLON, null); }
"," { return newToken(SqlParserSymbols.COMMA, null); }
"." { return newToken(SqlParserSymbols.DOT, null); }
"*" { return newToken(SqlParserSymbols.STAR, null); }
"(" { return newToken(SqlParserSymbols.LPAREN, null); }
")" { return newToken(SqlParserSymbols.RPAREN, null); }
"[" { return newToken(SqlParserSymbols.LBRACKET, null); }
"]" { return newToken(SqlParserSymbols.RBRACKET, null); }
"/" { return newToken(SqlParserSymbols.DIVIDE, null); }
"%" { return newToken(SqlParserSymbols.MOD, null); }
"+" { return newToken(SqlParserSymbols.ADD, null); }
"-" { return newToken(SqlParserSymbols.SUBTRACT, null); }
"&" { return newToken(SqlParserSymbols.BITAND, null); }
"|" { return newToken(SqlParserSymbols.BITOR, null); }
"^" { return newToken(SqlParserSymbols.BITXOR, null); }
"~" { return newToken(SqlParserSymbols.BITNOT, null); }
"=" { return newToken(SqlParserSymbols.EQUAL, null); }
"!" { return newToken(SqlParserSymbols.NOT, null); }
"<" { return newToken(SqlParserSymbols.LESSTHAN, null); }
">" { return newToken(SqlParserSymbols.GREATERTHAN, null); }
"\"" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }
"'" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }
"`" { return newToken(SqlParserSymbols.UNMATCHED_STRING_LITERAL, null); }

// double-character tokens
"!=" { return newToken(SqlParserSymbols.NOTEQUAL, null); }

// The rules for IntegerLiteral and DecimalLiteral are the same, but it is useful
// to distinguish them, e.g., so the Parser can use integer literals without analysis.
{IntegerLiteral} {
  try {
    return newToken(SqlParserSymbols.INTEGER_LITERAL, new BigDecimal(yytext()));
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }
}

{DecimalLiteral} {
  try {
    return newToken(SqlParserSymbols.DECIMAL_LITERAL, new BigDecimal(yytext()));
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }
}

{QuotedIdentifier} {
  // Remove the quotes and trim whitespace.
  String trimmedIdent = yytext().substring(1, yytext().length() - 1).trim();
  if (trimmedIdent.isEmpty()) {
    return newToken(SqlParserSymbols.EMPTY_IDENT, yytext());
  }
  return newToken(SqlParserSymbols.IDENT, trimmedIdent);
}

{IdentifierOrKw} {
  String text = yytext();
  if (text.startsWith(".")) {
    // If we see an identifier that starts with a dot, we push back the identifier
    // minus the dot back into the input stream.
    yypushback(text.length() - 1);
    return newToken(SqlParserSymbols.DOT, yytext());
  }
  Integer kw_id = keywordMap.get(text.toLowerCase());
  if (kw_id != null) {
    return newToken(kw_id, text);
  } else if (isReserved(text)) {
    return newToken(SqlParserSymbols.UNUSED_RESERVED_WORD, text);
  } else {
    return newToken(SqlParserSymbols.IDENT, text);
  }
}

{SingleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL, yytext().substring(1, yytext().length()-1));
}

{DoubleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL, yytext().substring(1, yytext().length()-1));
}

{CommentedHintBegin} {
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_START, null);
}

{CommentedHintEnd} {
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_END, null);
}

{EolHintBegin} {
  yybegin(EOLHINT);
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_START, null);
}

<EOLHINT> {LineTerminator} {
  yybegin(YYINITIAL);
  return newToken(SqlParserSymbols.COMMENTED_PLAN_HINT_END, null);
}

{Comment} { /* ignore */ }
{Whitespace} { /* ignore */ }

// Provide a default error token when nothing matches, otherwise the user sees
// "Error: could not match input" which is confusing.
[^] { return newToken(SqlParserSymbols.UNEXPECTED_CHAR, yytext()); }