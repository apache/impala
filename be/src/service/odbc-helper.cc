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

#include "service/odbc-helper.h"

#include <glog/logging.h>

#include "common/version.h"
#include "runtime/exec-env.h"
#include "service/frontend.h"
#include "service/impala-server.inline.h"
#include "util/auth-util.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;
using namespace apache::hive::service::cli;

namespace impala {

// ODBC reserved keywords as per ISO/IEF CLI specification and ODBC standard.
// From https://docs.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql#odbc-reserved-keywords
const string ODBC_KEYWORDS =
    "ABSOLUTE,ACTION,ADA,ADD,ALL,ALLOCATE,ALTER,AND,ANY,ARE,AS,ASC,ASSERTION,AT,"
    "AUTHORIZATION,AVG,BEGIN,BETWEEN,BIT,BIT_LENGTH,BOTH,BY,CASCADE,CASCADED,CASE,"
    "CAST,CATALOG,CHAR,CHAR_LENGTH,CHARACTER,CHARACTER_LENGTH,CHECK,CLOSE,COALESCE,"
    "COLLATE,COLLATION,COLUMN,COMMIT,CONNECT,CONNECTION,CONSTRAINT,CONSTRAINTS,"
    "CONTINUE,CONVERT,CORRESPONDING,COUNT,CREATE,CROSS,CURRENT,CURRENT_DATE,"
    "CURRENT_TIME,CURRENT_TIMESTAMP,CURRENT_USER,CURSOR,DATE,DAY,DEALLOCATE,DEC,"
    "DECIMAL,DECLARE,DEFAULT,DEFERRABLE,DEFERRED,DELETE,DESC,DESCRIBE,DESCRIPTOR,"
    "DIAGNOSTICS,DISCONNECT,DISTINCT,DOMAIN,DOUBLE,DROP,ELSE,END,ESCAPE,EXCEPT,"
    "EXCEPTION,EXEC,EXECUTE,EXISTS,EXTERNAL,EXTRACT,FALSE,FETCH,FIRST,FLOAT,FOR,"
    "FOREIGN,FORTRAN,FOUND,FROM,FULL,GET,GLOBAL,GO,GOTO,GRANT,GROUP,HAVING,HOUR,"
    "IDENTITY,IMMEDIATE,IN,INCLUDE,INDEX,INDICATOR,INITIALLY,INNER,INPUT,"
    "INSENSITIVE,INSERT,INT,INTEGER,INTERSECT,INTERVAL,INTO,IS,ISOLATION,JOIN,KEY,"
    "LANGUAGE,LAST,LEADING,LEFT,LEVEL,LIKE,LOCAL,LOWER,MATCH,MAX,MIN,MINUTE,MODULE,"
    "MONTH,NAMES,NATIONAL,NATURAL,NCHAR,NEXT,NO,NONE,NOT,NULL,NULLIF,NUMERIC,"
    "OCTET_LENGTH,OF,ON,ONLY,OPEN,OPTION,OR,ORDER,OUTER,OUTPUT,OVERLAPS,PAD,PARTIAL,"
    "PASCAL,POSITION,PRECISION,PREPARE,PRESERVE,PRIMARY,PRIOR,PRIVILEGES,PROCEDURE,"
    "PUBLIC,READ,REAL,REFERENCES,RELATIVE,RESTRICT,REVOKE,RIGHT,ROLLBACK,ROWS,"
    "SCHEMA,SCROLL,SECOND,SECTION,SELECT,SESSION,SESSION_USER,SET,SIZE,SMALLINT,"
    "SOME,SPACE,SQL,SQLCA,SQLCODE,SQLERROR,SQLSTATE,SQLWARNING,SUBSTRING,SUM,"
    "SYSTEM_USER,TABLE,TEMPORARY,THEN,TIME,TIMESTAMP,TIMEZONE_HOUR,TIMEZONE_MINUTE,"
    "TO,TRAILING,TRANSACTION,TRANSLATE,TRANSLATION,TRIM,TRUE,UNION,UNIQUE,UNKNOWN,"
    "UPDATE,UPPER,USAGE,USER,USING,VALUE,VALUES,VARCHAR,VARYING,VIEW,WHEN,WHENEVER,"
    "WHERE,WITH,WORK,WRITE,YEAR,ZONE";

void PopulateOdbcGetInfo(TGetInfoResp& return_val, TGetInfoType::type info_type,
    const shared_ptr<ImpalaServer::SessionState>& session,
    const char* sqlstate_optional_feature_not_implemented) {
  switch (info_type) {
    case TGetInfoType::CLI_SERVER_NAME:
    case TGetInfoType::CLI_DBMS_NAME:
      return_val.infoValue.__set_stringValue("Impala");
      break;
    case TGetInfoType::CLI_DBMS_VER:
      return_val.infoValue.__set_stringValue(GetDaemonBuildVersion());
      break;
    case TGetInfoType::CLI_MAX_COLUMN_NAME_LEN:
      return_val.infoValue.__set_lenValue(767);
      break;
    case TGetInfoType::CLI_MAX_SCHEMA_NAME_LEN:
      return_val.infoValue.__set_lenValue(128);
      break;
    case TGetInfoType::CLI_MAX_TABLE_NAME_LEN:
      return_val.infoValue.__set_lenValue(128);
      break;
    case TGetInfoType::CLI_MAX_CATALOG_NAME_LEN:
      return_val.infoValue.__set_lenValue(128);
      break;
    case TGetInfoType::CLI_MAX_CURSOR_NAME_LEN:
      return_val.infoValue.__set_lenValue(128);
      break;
    case TGetInfoType::CLI_MAX_USER_NAME_LEN:
      return_val.infoValue.__set_lenValue(128);
      break;
    case TGetInfoType::CLI_MAX_IDENTIFIER_LEN:
      return_val.infoValue.__set_lenValue(128);
      break;
    case TGetInfoType::CLI_IDENTIFIER_CASE:
      // SQL_IC_LOWER = 2 (case insensitive, stored in lowercase)
      return_val.infoValue.__set_smallIntValue(2);
      break;
    case TGetInfoType::CLI_IDENTIFIER_QUOTE_CHAR:
      return_val.infoValue.__set_stringValue("`");
      break;
    case TGetInfoType::CLI_SEARCH_PATTERN_ESCAPE:
      return_val.infoValue.__set_stringValue("\\");
      break;
    case TGetInfoType::CLI_DATA_SOURCE_READ_ONLY:
      // SQL_FALSE = 0 (not read-only)
      return_val.infoValue.__set_smallIntValue(0);
      break;
    case TGetInfoType::CLI_TXN_CAPABLE:
      // SQL_TC_NONE = 0 (no transaction support)
      return_val.infoValue.__set_smallIntValue(0);
      break;
    case TGetInfoType::CLI_USER_NAME:
      return_val.infoValue.__set_stringValue(GetEffectiveUser(*session));
      break;
    case TGetInfoType::CLI_ORDER_BY_COLUMNS_IN_SELECT:
      // ODBC expects "Y"/"N" string: set "N" because Impala does
      // not require ORDER BY columns to be in SELECT list.
      return_val.infoValue.__set_stringValue("N");
      break;
    case TGetInfoType::CLI_MAX_COLUMNS_IN_SELECT:
      // No hard documented limit for number of columns in a SELECT
      // ODBC: 0 => "no fixed limit / driver dependent"
      return_val.infoValue.__set_lenValue(0);
      break;
    case TGetInfoType::CLI_MAX_COLUMNS_IN_TABLE:
      // No hard documented limit for number of columns in a table
      return_val.infoValue.__set_lenValue(0);
      break;
    case TGetInfoType::CLI_MAX_COLUMNS_IN_GROUP_BY:
      return_val.infoValue.__set_lenValue(0);  // No limit
      break;
    case TGetInfoType::CLI_MAX_COLUMNS_IN_ORDER_BY:
      return_val.infoValue.__set_lenValue(0);  // No limit
      break;
    case TGetInfoType::CLI_MAX_TABLES_IN_SELECT:
      return_val.infoValue.__set_lenValue(0);  // No limit
      break;
    case TGetInfoType::CLI_MAX_STATEMENT_LEN: {
      // Prefer the server's configured max_statement_length_bytes if available.
      // If the option is not set or is <= 0, return 0 (ODBC: unknown/unlimited).
      int32_t max_stmt_bytes = 0;
      if (session && session->QueryOptions().__isset.max_statement_length_bytes) {
        max_stmt_bytes = session->QueryOptions().max_statement_length_bytes;
      }
      if (max_stmt_bytes <= 0) {
        return_val.infoValue.__set_lenValue(0);
      } else {
        // SQL_MAX_STATEMENT_LEN expects number of characters; using bytes is
        // acceptable if the server's limit is in bytes and the client and server
        // agree on encoding. Use bytes here to match Impala option units.
        return_val.infoValue.__set_lenValue(max_stmt_bytes);
      }
      break;
    }
    case TGetInfoType::CLI_MAX_ROW_SIZE: {
      // Prefer the session's configured MAX_ROW_SIZE if available; otherwise fall back
      // to the documented default of 524288 (512 KB). MAX_ROW_SIZE is in bytes.
      int64_t max_row_size = 524288;  // Default from TQueryOptions.max_row_size
      if (session && session->QueryOptions().__isset.max_row_size) {
        max_row_size = session->QueryOptions().max_row_size;
      }
      if (max_row_size <= 0) {
        // Invalid or unset value: return the default
        return_val.infoValue.__set_lenValue(524288);
      } else {
        return_val.infoValue.__set_lenValue(max_row_size);
      }
      break;
    }
    case TGetInfoType::CLI_SPECIAL_CHARACTERS:
      // Per ODBC SQL_SPECIAL_CHARACTERS: list characters that can appear in identifiers
      // beyond a-z/A-Z/0-9/_. Impala identifiers (unquoted)
      // allow only underscore as "special"
      // Impala does not allow arbitrary special characters in unquoted identifiers,
      // so return an empty string (no special chars allowed unquoted).
      return_val.infoValue.__set_stringValue("");
      break;
    case TGetInfoType::CLI_NULL_COLLATION:
      // SQL_NC_HIGH = 2 -> NULLs sort high
      // (Impala treats NULL > all other values by default)
      return_val.infoValue.__set_smallIntValue(2);
      break;
    case TGetInfoType::CLI_ALTER_TABLE:
      // Bitmask of ALTER TABLE capabilities per ODBC SQLGetInfo(SQL_ALTER_TABLE):
      // - SQL_AT_ADD_COLUMN = 0x1 (Impala supports ADD COLUMN(S))
      // - SQL_AT_DROP_COLUMN = 0x2 (Impala supports DROP COLUMN)
      // Other bits (defaults, constraints, etc.) are not supported by Impala.
      return_val.infoValue.__set_integerBitmask(0x1 | 0x2);
      break;
    case TGetInfoType::CLI_OJ_CAPABILITIES:
      // SQL_OJ_LEFT = 1, SQL_OJ_RIGHT = 2, SQL_OJ_FULL = 4, SQL_OJ_NESTED = 8
      // SQL_OJ_NOT_ORDERED = 16, SQL_OJ_INNER = 32, SQL_OJ_ALL_COMPARISON_OPS = 64
      return_val.infoValue.__set_integerBitmask(127); // All supported
      break;
    case TGetInfoType::CLI_INTEGRITY:
      // SQL_IC_NONE = 0 (no enforced integrity constraints)
      return_val.infoValue.__set_smallIntValue(0);
      break;
    case TGetInfoType::CLI_DESCRIBE_PARAMETER:
      // SQL_FALSE = 0 (does not support DESCRIBE PARAMETER)
      return_val.infoValue.__set_smallIntValue(0);
      break;
    case TGetInfoType::CLI_XOPEN_CLI_YEAR:
      return_val.infoValue.__set_stringValue("1995");
      break;
    case TGetInfoType::CLI_DATA_SOURCE_NAME:
      return_val.infoValue.__set_stringValue("Impala");
      break;
    case TGetInfoType::CLI_ACCESSIBLE_TABLES:
      // SQL_ACCESSIBLE_TABLES = 1 (returns accessible tables)
      return_val.infoValue.__set_smallIntValue(1);
      break;
    case TGetInfoType::CLI_ACCESSIBLE_PROCEDURES:
      // SQL_FALSE = 0 (no stored procedures)
      return_val.infoValue.__set_smallIntValue(0);
      break;
    case TGetInfoType::CLI_CURSOR_COMMIT_BEHAVIOR:
      // SQL_CB_DELETE = 1 (cursors are closed on commit)
      return_val.infoValue.__set_smallIntValue(1);
      break;
    case TGetInfoType::CLI_DEFAULT_TXN_ISOLATION:
      // SQL_TXN_NONE = 0 (no transaction support)
      return_val.infoValue.__set_smallIntValue(0);
      break;
    case TGetInfoType::CLI_TXN_ISOLATION_OPTION:
      // SQL_TXN_NONE = 0 (no transaction support)
      return_val.infoValue.__set_integerBitmask(0);
      break;
    case TGetInfoType::CLI_ODBC_KEYWORDS: {
      // Return Impala-specific keywords excluding ODBC-reserved keywords
      string non_odbc_keywords;
      Status kw_status = ExecEnv::GetInstance()->frontend()->GetNonOdbcKeywords(
          ODBC_KEYWORDS, &non_odbc_keywords);
      if (kw_status.ok()) {
        return_val.infoValue.__set_stringValue(non_odbc_keywords);
      } else {
        // Fallback to empty string on error to avoid returning incorrect keywords.
        VLOG(1) << "Failed to fetch non-ODBC keywords: " << kw_status.GetDetail();
        return_val.infoValue.__set_stringValue("");
      }
      break;
    }
    case TGetInfoType::CLI_MAX_DRIVER_CONNECTIONS:
    case TGetInfoType::CLI_MAX_CONCURRENT_ACTIVITIES:
    case TGetInfoType::CLI_SCROLL_CONCURRENCY:
    case TGetInfoType::CLI_GETDATA_EXTENSIONS:
    case TGetInfoType::CLI_MAX_COLUMNS_IN_INDEX:
    case TGetInfoType::CLI_MAX_INDEX_SIZE:
    case TGetInfoType::CLI_CURSOR_SENSITIVITY:
    case TGetInfoType::CLI_CATALOG_NAME:
    case TGetInfoType::CLI_COLLATION_SEQ:
    default:
      return_val.status.__set_statusCode(thrift::TStatusCode::ERROR_STATUS);
      return_val.status.__set_errorMessage(("Unsupported operation"));
      return_val.status.__set_sqlState(sqlstate_optional_feature_not_implemented);
      // 'infoValue' is a required field of TGetInfoResp
      return_val.infoValue.__set_stringValue("");
      return;
  }
  return_val.status.__set_statusCode(thrift::TStatusCode::SUCCESS_STATUS);
}
}
