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

import java_cup.runtime.Symbol;
import java.lang.Integer;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import com.cloudera.impala.analysis.SqlParserSymbols;

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
  // map from keyword string to token id
  // we use a linked hash map because the insertion order is important.
  // for example, we want "and" to come after "&&" to make sure error reporting
  // uses "and" as a display name and not "&&"
  private static final Map<String, Integer> keywordMap =
      new LinkedHashMap<String, Integer>();
  static {
    keywordMap.put("&&", new Integer(SqlParserSymbols.KW_AND));
    keywordMap.put("add", new Integer(SqlParserSymbols.KW_ADD));
    keywordMap.put("all", new Integer(SqlParserSymbols.KW_ALL));
    keywordMap.put("alter", new Integer(SqlParserSymbols.KW_ALTER));
    keywordMap.put("and", new Integer(SqlParserSymbols.KW_AND));    
    keywordMap.put("as", new Integer(SqlParserSymbols.KW_AS));
    keywordMap.put("asc", new Integer(SqlParserSymbols.KW_ASC));
    keywordMap.put("avg", new Integer(SqlParserSymbols.KW_AVG));
    keywordMap.put("between", new Integer(SqlParserSymbols.KW_BETWEEN));
    keywordMap.put("bigint", new Integer(SqlParserSymbols.KW_BIGINT));
    keywordMap.put("boolean", new Integer(SqlParserSymbols.KW_BOOLEAN));
    keywordMap.put("by", new Integer(SqlParserSymbols.KW_BY));
    keywordMap.put("distinctpc",
       new Integer(SqlParserSymbols.KW_DISTINCTPC));
    keywordMap.put("distinctpcsa",
       new Integer(SqlParserSymbols.KW_DISTINCTPCSA));
    keywordMap.put("case", new Integer(SqlParserSymbols.KW_CASE));
    keywordMap.put("cast", new Integer(SqlParserSymbols.KW_CAST));    
    keywordMap.put("change", new Integer(SqlParserSymbols.KW_CHANGE));
    keywordMap.put("column", new Integer(SqlParserSymbols.KW_COLUMN));
    keywordMap.put("columns", new Integer(SqlParserSymbols.KW_COLUMNS));
    keywordMap.put("comment", new Integer(SqlParserSymbols.KW_COMMENT));
    keywordMap.put("count", new Integer(SqlParserSymbols.KW_COUNT));
    keywordMap.put("create", new Integer(SqlParserSymbols.KW_CREATE));
    keywordMap.put("database", new Integer(SqlParserSymbols.KW_DATABASE));
    keywordMap.put("databases", new Integer(SqlParserSymbols.KW_DATABASES));
    keywordMap.put("data", new Integer(SqlParserSymbols.KW_DATA));
    keywordMap.put("date", new Integer(SqlParserSymbols.KW_DATE));
    keywordMap.put("datetime", new Integer(SqlParserSymbols.KW_DATETIME));
    keywordMap.put("delimited", new Integer(SqlParserSymbols.KW_DELIMITED));
    keywordMap.put("desc", new Integer(SqlParserSymbols.KW_DESC));
    keywordMap.put("describe", new Integer(SqlParserSymbols.KW_DESCRIBE));		
    keywordMap.put("distinct", new Integer(SqlParserSymbols.KW_DISTINCT));
    keywordMap.put("div", new Integer(SqlParserSymbols.KW_DIV));
    keywordMap.put("double", new Integer(SqlParserSymbols.KW_DOUBLE));
    keywordMap.put("drop", new Integer(SqlParserSymbols.KW_DROP));
    keywordMap.put("else", new Integer(SqlParserSymbols.KW_ELSE));
    keywordMap.put("end", new Integer(SqlParserSymbols.KW_END));
    keywordMap.put("escaped", new Integer(SqlParserSymbols.KW_ESCAPED));
    keywordMap.put("exists", new Integer(SqlParserSymbols.KW_EXISTS));
    keywordMap.put("explain", new Integer(SqlParserSymbols.KW_EXPLAIN));
    keywordMap.put("external", new Integer(SqlParserSymbols.KW_EXTERNAL));
    keywordMap.put("false", new Integer(SqlParserSymbols.KW_FALSE));    
    keywordMap.put("fields", new Integer(SqlParserSymbols.KW_FIELDS));
    keywordMap.put("fileformat", new Integer(SqlParserSymbols.KW_FILEFORMAT));
    keywordMap.put("float", new Integer(SqlParserSymbols.KW_FLOAT));
    keywordMap.put("format", new Integer(SqlParserSymbols.KW_FORMAT));
    keywordMap.put("formatted", new Integer(SqlParserSymbols.KW_FORMATTED));
    keywordMap.put("from", new Integer(SqlParserSymbols.KW_FROM));
    keywordMap.put("full", new Integer(SqlParserSymbols.KW_FULL));
    keywordMap.put("group", new Integer(SqlParserSymbols.KW_GROUP));
    keywordMap.put("having", new Integer(SqlParserSymbols.KW_HAVING));
    keywordMap.put("if", new Integer(SqlParserSymbols.KW_IF));
    keywordMap.put("is", new Integer(SqlParserSymbols.KW_IS));
    keywordMap.put("in", new Integer(SqlParserSymbols.KW_IN));
    keywordMap.put("inner", new Integer(SqlParserSymbols.KW_INNER));
    keywordMap.put("inpath", new Integer(SqlParserSymbols.KW_INPATH));
    keywordMap.put("insert", new Integer(SqlParserSymbols.KW_INSERT));
    keywordMap.put("interval", new Integer(SqlParserSymbols.KW_INTERVAL));
    keywordMap.put("into", new Integer(SqlParserSymbols.KW_INTO)); 
    keywordMap.put("int", new Integer(SqlParserSymbols.KW_INT));    
    keywordMap.put("integer", new Integer(SqlParserSymbols.KW_INT));
    keywordMap.put("join", new Integer(SqlParserSymbols.KW_JOIN));
    keywordMap.put("left", new Integer(SqlParserSymbols.KW_LEFT));
    keywordMap.put("like", new Integer(SqlParserSymbols.KW_LIKE));
    keywordMap.put("limit", new Integer(SqlParserSymbols.KW_LIMIT));
    keywordMap.put("lines", new Integer(SqlParserSymbols.KW_LINES));
    keywordMap.put("load", new Integer(SqlParserSymbols.KW_LOAD));
    keywordMap.put("location", new Integer(SqlParserSymbols.KW_LOCATION));
    keywordMap.put("min", new Integer(SqlParserSymbols.KW_MIN));    
    keywordMap.put("max", new Integer(SqlParserSymbols.KW_MAX));
    keywordMap.put("not", new Integer(SqlParserSymbols.KW_NOT));
    keywordMap.put("null", new Integer(SqlParserSymbols.KW_NULL));
    keywordMap.put("on", new Integer(SqlParserSymbols.KW_ON));    
    keywordMap.put("||", new Integer(SqlParserSymbols.KW_OR));
    keywordMap.put("or", new Integer(SqlParserSymbols.KW_OR));    
    keywordMap.put("order", new Integer(SqlParserSymbols.KW_ORDER));
    keywordMap.put("outer", new Integer(SqlParserSymbols.KW_OUTER));
    keywordMap.put("overwrite", new Integer(SqlParserSymbols.KW_OVERWRITE));
    keywordMap.put("parquetfile", new Integer(SqlParserSymbols.KW_PARQUETFILE));
    keywordMap.put("partition", new Integer(SqlParserSymbols.KW_PARTITION));
    keywordMap.put("partitioned", new Integer(SqlParserSymbols.KW_PARTITIONED));
    keywordMap.put("rcfile", new Integer(SqlParserSymbols.KW_RCFILE));   
    keywordMap.put("real", new Integer(SqlParserSymbols.KW_DOUBLE));
    keywordMap.put("regexp", new Integer(SqlParserSymbols.KW_REGEXP));
    keywordMap.put("rename", new Integer(SqlParserSymbols.KW_RENAME));
    keywordMap.put("replace", new Integer(SqlParserSymbols.KW_REPLACE));
    keywordMap.put("rlike", new Integer(SqlParserSymbols.KW_RLIKE));
    keywordMap.put("right", new Integer(SqlParserSymbols.KW_RIGHT));
    keywordMap.put("row", new Integer(SqlParserSymbols.KW_ROW));
    keywordMap.put("schema", new Integer(SqlParserSymbols.KW_SCHEMA));
    keywordMap.put("schemas", new Integer(SqlParserSymbols.KW_SCHEMAS));
    keywordMap.put("select", new Integer(SqlParserSymbols.KW_SELECT));
    keywordMap.put("semi", new Integer(SqlParserSymbols.KW_SEMI));
    keywordMap.put("set", new Integer(SqlParserSymbols.KW_SET));
    keywordMap.put("show", new Integer(SqlParserSymbols.KW_SHOW));
    keywordMap.put("smallint", new Integer(SqlParserSymbols.KW_SMALLINT));
    keywordMap.put("stored", new Integer(SqlParserSymbols.KW_STORED));
    keywordMap.put("string", new Integer(SqlParserSymbols.KW_STRING));
    keywordMap.put("sum", new Integer(SqlParserSymbols.KW_SUM));
    keywordMap.put("sequencefile", new Integer(SqlParserSymbols.KW_SEQUENCEFILE));
    keywordMap.put("table", new Integer(SqlParserSymbols.KW_TABLE));
    keywordMap.put("tables", new Integer(SqlParserSymbols.KW_TABLES));
    keywordMap.put("terminated", new Integer(SqlParserSymbols.KW_TERMINATED)); 
    keywordMap.put("textfile", new Integer(SqlParserSymbols.KW_TEXTFILE)); 
    keywordMap.put("tinyint", new Integer(SqlParserSymbols.KW_TINYINT));
    keywordMap.put("to", new Integer(SqlParserSymbols.KW_TO));
    keywordMap.put("use", new Integer(SqlParserSymbols.KW_USE));
    keywordMap.put("using", new Integer(SqlParserSymbols.KW_USING));
    keywordMap.put("union", new Integer(SqlParserSymbols.KW_UNION));
    keywordMap.put("when", new Integer(SqlParserSymbols.KW_WHEN));
    keywordMap.put("where", new Integer(SqlParserSymbols.KW_WHERE));
    keywordMap.put("then", new Integer(SqlParserSymbols.KW_THEN));
    keywordMap.put("true", new Integer(SqlParserSymbols.KW_TRUE));
    keywordMap.put("timestamp", new Integer(SqlParserSymbols.KW_TIMESTAMP));
    keywordMap.put("values", new Integer(SqlParserSymbols.KW_VALUES));
    keywordMap.put("with", new Integer(SqlParserSymbols.KW_WITH));
  }
    
  // map from token id to token description
  public static final Map<Integer, String> tokenIdMap =
      new HashMap<Integer, String>();
  static {
    Iterator<Map.Entry<String, Integer>> it = keywordMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>) it.next();
      tokenIdMap.put(pairs.getValue(), pairs.getKey().toUpperCase());
    }
    
    // add non-keyword tokens
    tokenIdMap.put(new Integer(SqlParserSymbols.IDENT), "IDENTIFIER");
    tokenIdMap.put(new Integer(SqlParserSymbols.COMMA), "COMMA");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITNOT), "~");
    tokenIdMap.put(new Integer(SqlParserSymbols.LPAREN), "(");
    tokenIdMap.put(new Integer(SqlParserSymbols.RPAREN), ")");
    tokenIdMap.put(new Integer(SqlParserSymbols.LBRACKET), "[");
    tokenIdMap.put(new Integer(SqlParserSymbols.RBRACKET), "]");
    tokenIdMap.put(new Integer(SqlParserSymbols.FLOATINGPOINT_LITERAL),
        "FLOATING POINT LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.INTEGER_LITERAL), "INTEGER LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.NOT), "!");
    tokenIdMap.put(new Integer(SqlParserSymbols.LESSTHAN), "<");
    tokenIdMap.put(new Integer(SqlParserSymbols.GREATERTHAN), ">");
    tokenIdMap.put(new Integer(SqlParserSymbols.UNMATCHED_STRING_LITERAL),
        "UNMATCHED STRING LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.MOD), "%");
    tokenIdMap.put(new Integer(SqlParserSymbols.ADD), "+");
    tokenIdMap.put(new Integer(SqlParserSymbols.DIVIDE), "/");
    tokenIdMap.put(new Integer(SqlParserSymbols.EQUAL), "=");
    tokenIdMap.put(new Integer(SqlParserSymbols.STAR), "*");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITOR), "|");
    tokenIdMap.put(new Integer(SqlParserSymbols.DOT), ".");
    tokenIdMap.put(new Integer(SqlParserSymbols.STRING_LITERAL), "STRING LITERAL");
    tokenIdMap.put(new Integer(SqlParserSymbols.EOF), "EOF");
    tokenIdMap.put(new Integer(SqlParserSymbols.SUBTRACT), "-");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITAND), "&");
    tokenIdMap.put(new Integer(SqlParserSymbols.error), "ERROR");
    tokenIdMap.put(new Integer(SqlParserSymbols.BITXOR), "^");
    tokenIdMap.put(new Integer(SqlParserSymbols.NUMERIC_OVERFLOW), "NUMERIC OVERFLOW");
  }
  
  public static boolean isKeyword(Integer tokenId) {        
    String token = tokenIdMap.get(tokenId);
    return keywordMap.containsKey(token.toLowerCase());
  }
    
  private Symbol newToken(int id, Object value) {
    return new Symbol(id, yyline+1, yycolumn+1, value);
  }
%}

LineTerminator = \r|\n|\r\n
NonTerminator = [^\r\n]
Whitespace = {LineTerminator} | [ \t\f]

IdentifierOrKwContents = [:digit:]*[:jletter:][:jletterdigit:]* | "&&" | "||"

IdentifierOrKw = \`{IdentifierOrKwContents}\` | {IdentifierOrKwContents}
IntegerLiteral = [:digit:][:digit:]*
SingleQuoteStringLiteral = \'(\\.|[^\\\'])*\'
DoubleQuoteStringLiteral = \"(\\.|[^\\\"])*\"

FLit1 = [0-9]+ \. [0-9]*
FLit2 = \. [0-9]+
FLit3 = [0-9]+
Exponent = [eE] [+-]? [0-9]+
DoubleLiteral = ({FLit1}|{FLit2}|{FLit3}) {Exponent}?

Comment = {TraditionalComment} | {EndOfLineComment}
TraditionalComment = "/*" [^*] ~"*/" | "/*" "*"+ "/"
EndOfLineComment = "--" {NonTerminator}* {LineTerminator}?

%%

// single-character tokens
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

{IdentifierOrKw} {
  if (yytext().startsWith("`")) {
    return newToken(SqlParserSymbols.IDENT, yytext().substring(1, yytext().length() - 1));
  }

  String text = yytext();
  Integer kw_id = keywordMap.get(text.toLowerCase());
  if (kw_id != null) {
    return newToken(kw_id.intValue(), text);
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

{IntegerLiteral} {
  BigInteger val = null;
  try {
    val = new BigInteger(yytext());
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }
  return newToken(SqlParserSymbols.INTEGER_LITERAL, val);
}

{DoubleLiteral} {
  Double val = null;
  try {
    val = new Double(yytext());
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }
  // conversion succeeded but literal is infinity or not a number
  if (val.isInfinite() || val.isNaN()) {
   return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }
  return newToken(SqlParserSymbols.FLOATINGPOINT_LITERAL, val);
}

{Comment} { /* ignore */ }
{Whitespace} { /* ignore */ }
