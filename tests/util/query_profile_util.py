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

from __future__ import absolute_import, division, print_function

import re


def parse_db_user(profile_text):
  user = re.search(r'\n\s+User:\s+(.*?)\n', profile_text)
  assert user is not None, "User not found in query profile"
  return user.group(1)


def parse_session_id(profile_text):
  """Parses the session id from the query profile text."""
  match = re.search(r'\n\s+Session ID:\s+(.*)\n', profile_text)
  assert match is not None, "Session ID not found in query profile"
  return match.group(1)


def parse_sql(profile_text):
  """Parses the SQL statement from the query profile text."""
  sql_stmt = re.search(r'\n\s+Sql Statement:\s+(.*?)\n', profile_text)
  assert sql_stmt is not None
  return sql_stmt.group(1)


def parse_query_type(profile_text):
  """Parses the query type from the query profile text."""
  query_type = re.search(r'\n\s+Query Type:\s+(.*?)\n', profile_text)
  assert query_type is not None
  return query_type.group(1)


def parse_query_state(profile_text):
  """Parses the query state from the query profile text."""
  query_state = re.search(r'\n\s+Query State:\s+(.*?)\n', profile_text)
  assert query_state is not None
  return query_state.group(1)


def parse_impala_query_state(profile_text):
  """Parses the Impala query state from the query profile text. """
  impala_query_state = re.search(r'\n\s+Impala Query State:\s+(.*?)\n', profile_text)
  assert impala_query_state is not None
  return impala_query_state.group(1)


def parse_query_status(profile_text):
  """Parses the query status from the query profile text. Status can be multiple lines if
     the query errored."""
  # Query Status (can be multiple lines if the query errored)
  query_status = re.search(r'\n\s+Query Status:\s+(.*?)\n\s+Impala Version', profile_text,
      re.DOTALL)
  assert query_status is not None
  return query_status.group(1)


def parse_query_id(profile_text):
  """Parses the query id from the query profile text."""
  query_id = re.search(r'Query\s+\(id=(.*?)\):', profile_text)
  assert query_id is not None
  return query_id.group(1)


def parse_retry_status(profile_text):
  """Parses the retry status from the query profile text. Returns None if the query was
     not retried."""
  retry_status = re.search(r'\n\s+Retry Status:\s+(.*?)\n', profile_text)
  if retry_status is None:
    return None

  return retry_status.group(1)


def parse_original_query_id(profile_text):
  """Parses the original query id from the query profile text. Returns None if the
     original query id is not present in the profile text."""
  original_query_id = re.search(r'\n\s+Original Query Id:\s+(.*?)\n', profile_text)
  if original_query_id is None:
    return None

  return original_query_id.group(1)


def parse_retried_query_id(profile_text):
  """Parses the retried query id from the query profile text. Returns None if the
     retried query id is not present in the profile text."""
  retried_query_id = re.search(r'\n\s+Retried Query Id:\s+(.*?)\n', profile_text)
  if retried_query_id is None:
    return None

  return retried_query_id.group(1)


def parse_num_rows_fetched(profile_text):
  """Parses the number of rows fetched from the query profile text."""
  num_rows_fetched = re.search(r'\n\s+\-\sNumRowsFetched:\s+(\d+)', profile_text)
  assert num_rows_fetched is not None, "Number of Rows Fetched not found in query profile"
  return int(num_rows_fetched.group(1))


def parse_admission_result(profile_text):
  """Parses the admission result from the query profile text."""
  admission_result = re.search(r'\n\s+Admission result:\s+(.*?)\n', profile_text)
  assert admission_result is not None, "Admission Result not found in query profile"
  return admission_result.group(1)
