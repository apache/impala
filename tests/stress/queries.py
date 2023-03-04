#!/usr/bin/env impala-python
#
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

# This module implements helpers for representing queries to be executed by the
# stress test, loading them and generating them.

from __future__ import absolute_import, division, print_function
from builtins import range
import logging
import os
from textwrap import dedent

from tests.comparison.db_types import Int, TinyInt, SmallInt, BigInt
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.query_profile import DefaultProfile
from tests.comparison.model_translator import SqlWriter
from tests.util.parse_util import match_memory_estimate, parse_mem_to_mb
import tests.util.test_file_parser as test_file_parser

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


class QueryType(object):
  COMPUTE_STATS, DELETE, INSERT, SELECT, UPDATE, UPSERT = list(range(6))


class Query(object):
  """Contains a SQL statement along with expected runtime information.
  This class is used as a struct, with the fields filled out by calling
  classes."""

  def __init__(self):
    self.name = None
    self.sql = None
    # In order to be able to make good estimates for DML queries in the binary search,
    # we need to bring the table to a good initial state before excuting the sql. Running
    # set_up_sql accomplishes this task.
    self.set_up_sql = None
    self.db_name = None
    self.result_hash = None
    self.required_mem_mb_with_spilling = None
    self.required_mem_mb_without_spilling = None
    self.solo_runtime_profile_with_spilling = None
    self.solo_runtime_profile_without_spilling = None
    self.solo_runtime_secs_with_spilling = None
    self.solo_runtime_secs_without_spilling = None
    # Query options to set before running the query.
    self.options = {}
    # Determines the order in which we will populate query runtime info. Queries with the
    # lowest population_order property will be handled first.
    self.population_order = 0
    # Type of query. Can have the following values: SELECT, COMPUTE_STATS, INSERT, UPDATE,
    # UPSERT, DELETE.
    self.query_type = QueryType.SELECT

    self._logical_query_id = None

  def __repr__(self):
    return dedent("""
        <Query
        Mem: %(required_mem_mb_with_spilling)s
        Mem no-spilling: %(required_mem_mb_without_spilling)s
        Solo Runtime: %(solo_runtime_secs_with_spilling)s
        Solo Runtime no-spilling: %(solo_runtime_secs_without_spilling)s
        DB: %(db_name)s
        Options: %(options)s
        Set up SQL: %(set_up_sql)s>
        SQL: %(sql)s>
        Population order: %(population_order)r>
        """.strip() % self.__dict__)

  @property
  def logical_query_id(self):
    """
    Return a meanginful unique str identifier for the query.

    Example: "tpcds_300_decimal_parquet_q21"
    """
    if self._logical_query_id is None:
      self._logical_query_id = '{0}_{1}'.format(self.db_name, self.name)
    return self._logical_query_id

  def write_runtime_info_profiles(self, directory):
    """Write profiles for spilling and non-spilling into directory (str)."""
    profiles_to_write = [
        (self.logical_query_id + "_profile_with_spilling.txt",
         self.solo_runtime_profile_with_spilling),
        (self.logical_query_id + "_profile_without_spilling.txt",
         self.solo_runtime_profile_without_spilling),
    ]
    for filename, profile in profiles_to_write:
      if profile is None:
        LOG.debug("No profile recorded for {0}".format(filename))
        continue
      with open(os.path.join(directory, filename), "w") as fh:
        fh.write(profile)


def load_tpc_queries(workload):
  """Returns a list of TPC queries. 'workload' should either be 'tpch' or 'tpcds'."""
  LOG.info("Loading %s queries", workload)
  queries = []
  for query_name, query_sql in test_file_parser.load_tpc_queries(workload,
      include_stress_queries=True).items():
    query = Query()
    query.name = query_name
    query.sql = query_sql
    queries.append(query)
  return queries


def load_queries_from_test_file(file_path, db_name=None):
  LOG.debug("Loading queries from %s", file_path)
  test_cases = test_file_parser.parse_query_test_file(file_path)
  queries = list()
  for test_case in test_cases:
    query = Query()
    query.sql = test_file_parser.remove_comments(test_case["QUERY"])
    query.db_name = db_name
    queries.append(query)
  return queries


def generate_compute_stats_queries(cursor):
  """For each table in the database that cursor is connected to, generate several compute
  stats queries. Each query will have a different value for the MT_DOP query option.
  """
  LOG.info("Generating Compute Stats queries")
  tables = [cursor.describe_table(t) for t in cursor.list_table_names()
            if not t.endswith("_original")]
  result = []
  mt_dop_values = [str(2**k) for k in range(5)]
  for table in tables:
    for mt_dop_value in mt_dop_values:
      compute_query = Query()
      compute_query.population_order = 1
      compute_query.query_type = QueryType.COMPUTE_STATS
      compute_query.sql = "COMPUTE STATS {0}".format(table.name)
      compute_query.options["MT_DOP"] = mt_dop_value
      compute_query.db_name = cursor.db_name
      compute_query.name = "compute_stats_{0}_mt_dop_{1}".format(
          table.name, compute_query.options["MT_DOP"])
      result.append(compute_query)
      LOG.debug("Added compute stats query: {0}".format(compute_query))
  return result


def generate_DML_queries(cursor, dml_mod_values):
  """Generate insert, upsert, update, delete DML statements.

  For each table in the database that cursor is connected to, create 4 DML queries
  (insert, upsert, update, delete) for each mod value in 'dml_mod_values'. This value
  controls which rows will be affected. The generated queries assume that for each table
  in the database, there exists a table with a '_original' suffix that is never modified.

  This function has some limitations:
  1. Only generates DML statements against Kudu tables, and ignores non-Kudu tables.
  2. Requires that the type of the first column of the primary key is an integer type.
  """
  LOG.info("Generating DML queries")
  tables = [cursor.describe_table(t) for t in cursor.list_table_names()
            if not t.endswith("_original")]
  result = []
  for table in tables:
    if not table.primary_keys:
      # Skip non-Kudu tables. If a table has no primary keys, then it cannot be a Kudu
      # table.
      LOG.debug("Skipping table '{0}' because it has no primary keys.".format(table.name))
      continue
    if len(table.primary_keys) > 1:
      # TODO(IMPALA-4665): Add support for tables with multiple primary keys.
      LOG.debug("Skipping table '{0}' because it has more than "
                "1 primary key column.".format(table.name))
      continue
    primary_key = table.primary_keys[0]
    if primary_key.exact_type not in (Int, TinyInt, SmallInt, BigInt):
      # We want to be able to apply the modulo operation on the primary key. If the
      # the first primary key column happens to not be an integer, we will skip
      # generating queries for this table
      LOG.debug("Skipping table '{0}' because the first column '{1}' in the "
                "primary key is not an integer.".format(table.name, primary_key.name))
      continue
    for mod_value in dml_mod_values:
      # Insert
      insert_query = Query()
      # Populate runtime info for Insert and Upsert queries before Update and Delete
      # queries because tables remain in original state after running the Insert and
      # Upsert queries. During the binary search in runtime info population for the
      # Insert query, we first delete some rows and then reinsert them, so the table
      # remains in the original state. For the delete, the order is reversed, so the table
      # is not in the original state after running the the delete (or update) query. This
      # is why population_order is smaller for Insert and Upsert queries.
      insert_query.population_order = 1
      insert_query.query_type = QueryType.INSERT
      insert_query.name = "insert_{0}".format(table.name)
      insert_query.db_name = cursor.db_name
      insert_query.sql = (
          "INSERT INTO TABLE {0} SELECT * FROM {0}_original "
          "WHERE {1} % {2} = 0").format(table.name, primary_key.name, mod_value)
      # Upsert
      upsert_query = Query()
      upsert_query.population_order = 1
      upsert_query.query_type = QueryType.UPSERT
      upsert_query.name = "upsert_{0}".format(table.name)
      upsert_query.db_name = cursor.db_name
      upsert_query.sql = (
          "UPSERT INTO TABLE {0} SELECT * "
          "FROM {0}_original WHERE {1} % {2} = 0").format(
              table.name, primary_key.name, mod_value)
      # Update
      update_query = Query()
      update_query.population_order = 2
      update_query.query_type = QueryType.UPDATE
      update_query.name = "update_{0}".format(table.name)
      update_query.db_name = cursor.db_name
      update_list = ', '.join(
          'a.{0} = b.{0}'.format(col.name)
          for col in table.cols if not col.is_primary_key)
      update_query.sql = (
          "UPDATE a SET {update_list} FROM {table_name} a JOIN {table_name}_original b "
          "ON a.{pk} = b.{pk} + 1 WHERE a.{pk} % {mod_value} = 0").format(
              table_name=table.name, pk=primary_key.name, mod_value=mod_value,
              update_list=update_list)
      # Delete
      delete_query = Query()
      delete_query.population_order = 2
      delete_query.query_type = QueryType.DELETE
      delete_query.name = "delete_{0}".format(table.name)
      delete_query.db_name = cursor.db_name
      delete_query.sql = ("DELETE FROM {0} WHERE {1} % {2} = 0").format(
          table.name, primary_key.name, mod_value)

      if table.name + "_original" in set(table.name for table in tables):
        insert_query.set_up_sql = "DELETE FROM {0} WHERE {1} % {2} = 0".format(
            table.name, primary_key.name, mod_value)
        upsert_query.set_up_sql = insert_query.set_up_sql
        update_query.set_up_sql = (
            "UPSERT INTO TABLE {0} SELECT * FROM {0}_original "
            "WHERE {1} % {2} = 0").format(table.name, primary_key.name, mod_value)
        delete_query.set_up_sql = update_query.set_up_sql

      result.append(insert_query)
      LOG.debug("Added insert query: {0}".format(insert_query))
      result.append(update_query)
      LOG.debug("Added update query: {0}".format(update_query))
      result.append(upsert_query)
      LOG.debug("Added upsert query: {0}".format(upsert_query))
      result.append(delete_query)
      LOG.debug("Added delete query: {0}".format(delete_query))
  assert len(result) > 0, "No DML queries were added."
  return result


def generate_random_queries(impala, random_db):
  """Generator function to produce random queries. 'impala' is the Impala service
  object. random_db is the name of the database that queries should be
  generated for."""
  with impala.cursor(db_name=random_db) as cursor:
    tables = [cursor.describe_table(t) for t in cursor.list_table_names()]
  query_generator = QueryGenerator(DefaultProfile())
  model_translator = SqlWriter.create()
  while True:
    query_model = query_generator.generate_statement(tables)
    sql = model_translator.write_query(query_model)
    query = Query()
    query.sql = sql
    query.db_name = random_db
    yield query


def estimate_query_mem_mb_usage(query, impalad_conn):
  """Runs an explain plan then extracts and returns the estimated memory needed to run
  the query.
  """
  with impalad_conn.cursor() as cursor:
    LOG.debug("Using %s database", query.db_name)
    if query.db_name:
      cursor.execute('USE ' + query.db_name)
    if query.query_type == QueryType.COMPUTE_STATS:
      # Running "explain" on compute stats is not supported by Impala.
      return
    LOG.debug("Explaining query\n%s", query.sql)
    cursor.execute('EXPLAIN ' + query.sql)
    explain_rows = cursor.fetchall()
    explain_lines = [row[0] for row in explain_rows]
    mem_limit, units = match_memory_estimate(explain_lines)
    return parse_mem_to_mb(mem_limit, units)
