package com.cloudera.impala.parser;

import java_cup.runtime.Symbol;
import java.lang.Integer;
import java.util.HashMap;
import java.util.Map;

import com.cloudera.impala.parser.SqlParserSymbols;

%%

%class SqlScanner
%cup
%eofval{
  return newToken(SqlParserSymbols.EOF, null);
%eofval}
%unicode
%line
%column
%{
  // map from keyword string to token id
  private static final Map<String, Integer> keyword_map =
      new HashMap<String, Integer>();
  static {
    keyword_map.put("and", new Integer(SqlParserSymbols.KW_AND));
    keyword_map.put("&&", new Integer(SqlParserSymbols.KW_AND));
    keyword_map.put("as", new Integer(SqlParserSymbols.KW_AS));
    keyword_map.put("asc", new Integer(SqlParserSymbols.KW_ASC));
    keyword_map.put("avg", new Integer(SqlParserSymbols.KW_AVG));
    keyword_map.put("bigint", new Integer(SqlParserSymbols.KW_BIGINT));
    keyword_map.put("boolean", new Integer(SqlParserSymbols.KW_BOOLEAN));
    keyword_map.put("by", new Integer(SqlParserSymbols.KW_BY));
    keyword_map.put("case", new Integer(SqlParserSymbols.KW_CASE));
    keyword_map.put("cast", new Integer(SqlParserSymbols.KW_CAST));
    keyword_map.put("count", new Integer(SqlParserSymbols.KW_COUNT));
    keyword_map.put("date", new Integer(SqlParserSymbols.KW_DATE));
    keyword_map.put("datetime", new Integer(SqlParserSymbols.KW_DATETIME));
    keyword_map.put("desc", new Integer(SqlParserSymbols.KW_DESC));
    keyword_map.put("distinct", new Integer(SqlParserSymbols.KW_DISTINCT));
    keyword_map.put("div", new Integer(SqlParserSymbols.KW_DIV));
    keyword_map.put("double", new Integer(SqlParserSymbols.KW_DOUBLE));
    keyword_map.put("else", new Integer(SqlParserSymbols.KW_ELSE));
    keyword_map.put("end", new Integer(SqlParserSymbols.KW_END));
    keyword_map.put("false", new Integer(SqlParserSymbols.KW_FALSE));
    keyword_map.put("float", new Integer(SqlParserSymbols.KW_FLOAT));
    keyword_map.put("from", new Integer(SqlParserSymbols.KW_FROM));
    keyword_map.put("full", new Integer(SqlParserSymbols.KW_FULL));
    keyword_map.put("group", new Integer(SqlParserSymbols.KW_GROUP));
    keyword_map.put("having", new Integer(SqlParserSymbols.KW_HAVING));
    keyword_map.put("is", new Integer(SqlParserSymbols.KW_IS));
    keyword_map.put("inner", new Integer(SqlParserSymbols.KW_INNER));
    keyword_map.put("join", new Integer(SqlParserSymbols.KW_JOIN));
    keyword_map.put("int", new Integer(SqlParserSymbols.KW_INT));
    keyword_map.put("left", new Integer(SqlParserSymbols.KW_LEFT));
    keyword_map.put("like", new Integer(SqlParserSymbols.KW_LIKE));
    keyword_map.put("limit", new Integer(SqlParserSymbols.KW_LIMIT));
    keyword_map.put("min", new Integer(SqlParserSymbols.KW_MIN));
    keyword_map.put("max", new Integer(SqlParserSymbols.KW_MAX));
    keyword_map.put("not", new Integer(SqlParserSymbols.KW_NOT));
    keyword_map.put("null", new Integer(SqlParserSymbols.KW_NULL));
    keyword_map.put("on", new Integer(SqlParserSymbols.KW_ON));
    keyword_map.put("or", new Integer(SqlParserSymbols.KW_OR));
    keyword_map.put("||", new Integer(SqlParserSymbols.KW_OR));
    keyword_map.put("order", new Integer(SqlParserSymbols.KW_ORDER));
    keyword_map.put("outer", new Integer(SqlParserSymbols.KW_OUTER));
    keyword_map.put("regexp", new Integer(SqlParserSymbols.KW_REGEXP));
    keyword_map.put("rlike", new Integer(SqlParserSymbols.KW_RLIKE));
    keyword_map.put("right", new Integer(SqlParserSymbols.KW_RIGHT));
    keyword_map.put("select", new Integer(SqlParserSymbols.KW_SELECT));
    keyword_map.put("semi", new Integer(SqlParserSymbols.KW_SEMI));
    keyword_map.put("smallint", new Integer(SqlParserSymbols.KW_SMALLINT));
    keyword_map.put("string", new Integer(SqlParserSymbols.KW_STRING));
    keyword_map.put("sum", new Integer(SqlParserSymbols.KW_SUM));
    keyword_map.put("tinyint", new Integer(SqlParserSymbols.KW_TINYINT));
    keyword_map.put("using", new Integer(SqlParserSymbols.KW_USING));
    keyword_map.put("when", new Integer(SqlParserSymbols.KW_WHEN));
    keyword_map.put("where", new Integer(SqlParserSymbols.KW_WHERE));
    keyword_map.put("then", new Integer(SqlParserSymbols.KW_THEN));
    keyword_map.put("true", new Integer(SqlParserSymbols.KW_TRUE));
    keyword_map.put("timestamp", new Integer(SqlParserSymbols.KW_TIMESTAMP));
  }

  private Symbol newToken(int id, Object value) {
    return new Symbol(id, yyline+1, yycolumn+1, value);
  }
%}

LineTerminator = \r|\n|\r\n
NonTerminator = [^\r\n]
Whitespace = {LineTerminator} | [ \t\f]

IdentifierOrKw = [:jletter:][:jletterdigit:]*
IntegerLiteral = [:digit:][:digit:]*
SingleQuoteStringLiteral = \'([^\n\r\']|\\\\|\\\')*\'
DoubleQuoteStringLiteral = \"([^\n\r\"]|\\\\|\\\")*\"

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
"/" { return newToken(SqlParserSymbols.DIVIDE, null); }
"%" { return newToken(SqlParserSymbols.MOD, null); }
"+" { return newToken(SqlParserSymbols.PLUS, null); }
"-" { return newToken(SqlParserSymbols.MINUS, null); }
"&" { return newToken(SqlParserSymbols.BITAND, null); }
"|" { return newToken(SqlParserSymbols.BITOR, null); }
"^" { return newToken(SqlParserSymbols.BITXOR, null); }
"~" { return newToken(SqlParserSymbols.BITNOT, null); }
"=" { return newToken(SqlParserSymbols.EQUAL, null); }
"!" { return newToken(SqlParserSymbols.NOT, null); }
"<" { return newToken(SqlParserSymbols.LESSTHAN, null); }
">" { return newToken(SqlParserSymbols.GREATERTHAN, null); }

{IdentifierOrKw} {
  Integer kw_id = keyword_map.get(yytext().toLowerCase());
  if (kw_id != null) {
    return newToken(kw_id.intValue(), yytext());
  } else {
    return newToken(SqlParserSymbols.IDENT, yytext());
  }
}

{SingleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL, yytext().substring(1, yytext().length()-1));
}

{DoubleQuoteStringLiteral} {
  return newToken(SqlParserSymbols.STRING_LITERAL, yytext().substring(1, yytext().length()-1));
}

{IntegerLiteral} {
  Long val = null;
  try {
    val = new Long(yytext());
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }
  return newToken(SqlParserSymbols.NUMERIC_LITERAL, val);
}

{DoubleLiteral} {
  Double val = null;
  try {
    val = new Double(yytext());
  } catch (NumberFormatException e) {
    return newToken(SqlParserSymbols.NUMERIC_OVERFLOW, yytext());
  }
  return newToken(SqlParserSymbols.NUMERIC_LITERAL, val);
}

{Comment} { /* ignore */ }
{Whitespace} { /* ignore */ }
