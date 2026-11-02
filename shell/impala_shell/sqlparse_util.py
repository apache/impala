# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Shared sqlparse configuration for the shell and data-loading scripts."""

import sqlparse

# Impala queries can exceed sqlparse's default grouping limits. This is only used in a
# client context, so DoS is not a concern here.
sqlparse.engine.grouping.MAX_GROUPING_DEPTH = None
sqlparse.engine.grouping.MAX_GROUPING_TOKENS = None

# Work around https://github.com/andialbrecht/sqlparse/issues/814. A backslash must
# consume the next character, so an escaped backslash cannot swallow a closing quote.
SQLPARSE_STRING_REGEX = {
    sqlparse.tokens.String.Single: r"'(?:''|\\[\s\S]|[^'\\])*'",
    sqlparse.tokens.String.Symbol: r'"(?:""|\\[\s\S]|[^"\\])*"',
}
sqlparse.lexer.Lexer.get_default_instance().set_SQL_REGEX([
    (SQLPARSE_STRING_REGEX.get(token_type, regex), token_type)
    for regex, token_type in sqlparse.keywords.SQL_REGEX
])
