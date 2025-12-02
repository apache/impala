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
import pprint
import pytest
import re
import shlex

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf, SkipIfFS, SkipIfHive2
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.util.test_file_parser import QueryTestSectionReader, remove_comments
from tests.common.environ import HIVE_MAJOR_VERSION, ICEBERG_DEFAULT_FORMAT_VERSION
from tests.util.filesystem_utils import WAREHOUSE


def properties_map_regex(name):
  return "%s \\(([^)]+)\\)" % name


def get_properties_map(sql, properties_map_name, exclusions=None):
  """ Extracts a dict of key-value pairs from the 'sql' string, which should be a SHOW
  CREATE TABLE statement. The 'properties_map_name' is the name of the properties map,
  e.g. 'TBLPROPERTIES' or 'SERDEPROPERTIES'. 'exclusions' is a list of properties to
  exclude from the result.
  """
  map_match = re.search(properties_map_regex(properties_map_name), sql)
  if map_match is None:
    return dict()
  tbl_props_contents = map_match.group(1).strip()
  kv_regex = r"'([^']*)'\s*=\s*'([^']*)'\s*(?:,|$)"
  kv_results = dict(re.findall(kv_regex, tbl_props_contents))

  # Verify [TBL|SERDE]PROPERTIES contents
  stripped_sql = re.sub(r'\s+', '', tbl_props_contents)
  reconstructed = ",".join("'{}'='{}'".format(k, v) for k, v in kv_results.items())
  stripped_reconstructed = re.sub(r'\s+', '', reconstructed.strip())
  assert stripped_sql == stripped_reconstructed, \
      "[TBL|SERDE]PROPERTIES contents are not valid SQL: " + tbl_props_contents

  if exclusions is not None:
    for filtered_key in exclusions:
      if filtered_key in kv_results:
        del kv_results[filtered_key]
  return kv_results


# The purpose of the show create table tests are to ensure that the "SHOW CREATE TABLE"
# output can actually be used to recreate the table. A test consists of a table
# definition. The table is created, then the output of "SHOW CREATE TABLE" is used to
# test if the table can be recreated. This test class does not support --update-results.
class TestShowCreateTable(ImpalaTestSuite):
  VALID_SECTION_NAMES = ["CREATE_TABLE", "CREATE_VIEW", "QUERY", "RESULTS-HIVE",
                         "RESULTS-HIVE-3"]
  # Properties to filter before comparing results
  FILTER_TBL_PROPERTIES = ["bucketing_version", "OBJCAPABILITIES"]

  @classmethod
  def add_test_dimensions(cls):
    super(TestShowCreateTable, cls).add_test_dimensions()
    # don't use any exec options, running exactly once is fine
    cls.ImpalaTestMatrix.clear_dimension('exec_option')
    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: (v.get_value('table_format').file_format == 'text'
                   and v.get_value('table_format').compression_codec == 'none'))

  def test_show_create_table(self, unique_database):
    self.__run_show_create_table_test_case('QueryTest/show-create-table',
                                           unique_database)

  @SkipIfFS.hbase
  def test_show_create_table_hbase(self, unique_database):
    self.__run_show_create_table_test_case('QueryTest/show-create-table-hbase',
                                           unique_database)

  @SkipIfHive2.acid
  def test_show_create_table_full_acid(self, unique_database):
    self.__run_show_create_table_test_case('QueryTest/show-create-table-full-acid',
                                           unique_database)

  @SkipIf.not_hdfs
  def test_show_create_table_paimon(self, unique_database):
    self.__run_show_create_table_test_case('QueryTest/show-create-table-paimon',
                                           unique_database)

  def test_show_create_table_with_stats(self, unique_database):
    self.__run_show_create_table_with_stats_test_case(
        'QueryTest/show-create-table-with-stats', unique_database)

  def __run_show_create_table_test_case(self, test_file_name, unique_db_name):
    """
    Runs a show-create-table test file, containing the following sections:

    ---- CREATE_TABLE
    contains a table creation statement to create table TABLE_NAME
    ---- RESULTS
    contains the expected result of SHOW CREATE TABLE table_name

    OR

    ---- CREATE_VIEW
    contains a view creation statement to create table VIEW_NAME
    ---- RESULTS
    contains the expected result of SHOW CREATE VIEW table_name

    OR

    ---- QUERY
    a show create table query
    ---- RESULTS
    contains the expected output of the SHOW CREATE TABLE query

    unique_db_name is the name of the database to use for all tables and
    views and must be unique so as not to conflict with other tests.
    """
    sections = self.load_query_test_file(self.get_workload(), test_file_name,
                                         self.VALID_SECTION_NAMES)
    for test_section in sections:
      test_case = ShowCreateTableTestCase(test_section, test_file_name, unique_db_name)

      if not test_case.existing_table:
        # create table in Impala
        self.__exec(self.__replace_variables(test_case.create_table_sql))
      # execute "SHOW CREATE TABLE ..."
      result = self.__exec(test_case.show_create_table_sql)
      create_table_result = self.__normalize(result.data[0])

      if not test_case.existing_table:
        # drop the table
        self.__exec(test_case.drop_table_sql)

      # check the result matches the expected result
      expected_result = self.__normalize(self.__replace_variables(self.__replace_uri(
          test_case.expected_result,
          self.__get_location_uri(create_table_result))))
      self.__compare_result(expected_result, create_table_result)

      if test_case.existing_table:
        continue

      # recreate the table with the result from above
      self.__exec(create_table_result)
      try:
        # we should get the same result from "show create table ..."
        result = self.__exec(test_case.show_create_table_sql)
        new_create_table_result = self.__normalize(result.data[0])
        assert create_table_result == new_create_table_result
      finally:
        # drop the table
        self.__exec(test_case.drop_table_sql)

  def __run_show_create_table_with_stats_test_case(
    self, test_file_name, unique_db_name):
    sections = self.load_query_test_file(
      self.get_workload(), test_file_name, self.VALID_SECTION_NAMES)
    for test_section in sections:
      test_case = ShowCreateTableTestCase(test_section, test_file_name, unique_db_name)
      if not test_case.existing_table:
        # create table in Impala (support multiple setup statements)
        setup_sql = self.__replace_variables(test_case.create_table_sql)
        for stmt in re.split(r";\s*", setup_sql.strip()):
          if not stmt:
            continue
          self.__exec(stmt)

      # Set SHOW_CREATE_TABLE_PARTITION_LIMIT=1 for WITH STATS queries to test
      # partition limiting
      self.__exec("SET SHOW_CREATE_TABLE_PARTITION_LIMIT=1")

      # Check if the table is a Paimon table before running COMPUTE STATS.
      # Paimon tables do not support 'COMPUTE STATS'.
      is_paimon = False
      if not test_case.existing_table and test_case.create_table_sql:
          is_paimon = "STORED AS PAIMON" in test_case.create_table_sql.upper()

      if not is_paimon and not test_case.existing_table:
        # COMPUTE STATS before running SHOW CREATE TABLE WITH STATS
        self.__exec("COMPUTE STATS " + test_case.table_name)

      # execute "SHOW CREATE TABLE ... WITH STATS"; collect all statements
      result = self.__exec(test_case.show_create_table_sql + " WITH STATS")
      raw_rows = [row.strip() for row in result.data if row and row.strip()]

      # Single row; split into statements
      raw_sql = raw_rows[0]
      raw_create_table_result = [s for s in re.split(r";\s*", raw_sql) if s.strip()]
      create_table_result = [self.__normalize(self.__mask_dynamic_values(s))
        for s in raw_create_table_result]
      location_source = raw_sql

      if not test_case.existing_table:
        # drop the table
        self.__exec(test_case.drop_table_sql)

      # Build expected statements list and compare per-statement
      expected_sql = self.__replace_variables(self.__replace_uri(
          test_case.expected_result,
          self.__get_location_uri(location_source)
      ))
      expected_statements = [
          self.__normalize(s)
          for s in re.split(r";\s*", expected_sql.strip())
          if s.strip()
      ]

      assert len(expected_statements) == len(create_table_result), \
          ("Expected {} statements, got {}".format(
              len(expected_statements), len(create_table_result)))
      for exp_stmt, act_stmt in zip(expected_statements, create_table_result):
        self.__compare_result(exp_stmt, act_stmt)

      if test_case.existing_table:
        continue

      # Check for warnings in the normalized output.
      # If warnings are present, the output is partial, and we must skip the
      # reproducibility check as it's guaranteed to fail on the partial DDL.
      has_warnings = any(s.strip().startswith('--') for s in create_table_result)
      if has_warnings:
        continue

      # recreate the table with the WITH STATS result from above (multiple statements)
      # Skip comment lines (warnings) when recreating
      for stmt in raw_create_table_result:
        if not stmt or stmt.strip().startswith('--'):
          continue
        self.__exec(stmt)
      try:
        # we should get the same WITH STATS result again
        result = self.__exec(test_case.show_create_table_sql + " WITH STATS")
        new_raw_rows = [row.strip() for row in result.data if row and row.strip()]
        new_raw_sql = new_raw_rows[0]
        new_create_table_result = [
            self.__normalize(self.__mask_dynamic_values(s))
            for s in re.split(r";\s*", new_raw_sql)
            if s.strip()
        ]
        assert create_table_result == new_create_table_result
      finally:
        # drop the table
        self.__exec(test_case.drop_table_sql)

  def __exec(self, sql_str):
    return self.execute_query_expect_success(self.client, sql_str)

  def __get_location_uri(self, sql_str):
    m = re.search("LOCATION '([^\']+)'", sql_str)
    if m is not None:
      return m.group(1)

  def __get_partition_properties(self, sql_str):
    """ Extract properties from partition-level SET TBLPROPERTIES statements.
    Handles statements like:
    ALTER TABLE ... PARTITION (p=1) SET TBLPROPERTIES ('key'='value', ...)
    """
    return get_properties_map(sql_str, "SET TBLPROPERTIES", self.FILTER_TBL_PROPERTIES)

  def __compare_result(self, expected_sql, actual_sql):
    """ Extract all properties """
    # Partition-level properties use "SET TBLPROPERTIES" syntax,
    # while table-level properties just use "TBLPROPERTIES"
    if 'PARTITION' in expected_sql and 'SET TBLPROPERTIES' in expected_sql:
      # For partition statements: ALTER TABLE ... PARTITION (...) SET TBLPROPERTIES (...)
      expected_tbl_props = self.__get_partition_properties(expected_sql)
      actual_tbl_props = self.__get_partition_properties(actual_sql)
    else:
      # For regular table-level properties: CREATE TABLE ... TBLPROPERTIES (...)
      expected_tbl_props = self.__get_properties_map(expected_sql, "TBLPROPERTIES")
      actual_tbl_props = self.__get_properties_map(actual_sql, "TBLPROPERTIES")

    assert expected_tbl_props == actual_tbl_props, \
        ("TBLPROPERTIES mismatch:\nExpected: {} \nActual: {}".format(
            expected_tbl_props, actual_tbl_props))

    expected_serde_props = self.__get_properties_map(expected_sql, "SERDEPROPERTIES")
    actual_serde_props = self.__get_properties_map(actual_sql, "SERDEPROPERTIES")
    assert expected_serde_props == actual_serde_props

    expected_sql_filtered = self.__remove_properties_maps(expected_sql)
    actual_sql_filtered = self.__remove_properties_maps(actual_sql)
    assert expected_sql_filtered == actual_sql_filtered

  def __mask_dynamic_values(self, s):
    """ Replace dynamic/volatile values with <NUM> placeholder for comparison.
    This masks values that change between test runs or are system-generated:
    - numFiles: file count (dynamic based on data ingestion)
    - totalSize: total size in bytes (dynamic)
    - impala.events.catalogVersion: catalog version number (increments)
    - impala.events.catalogServiceId: UUID-like identifier (changes per service)

    Note: numRows is NOT masked because it's typically a known test value
    """
    # Mask dynamic file system properties
    s = re.sub(r"('numFiles'\s*=\s*)'[0-9]+'", r"\1'<NUM>'", s)
    s = re.sub(r"('totalSize'\s*=\s*)'[0-9]+'", r"\1'<NUM>'", s)
    s = re.sub(r"('numRows'\s*=\s*)'[0-9]+'", r"\1'<NUM>'", s)

    # Mask Impala event system properties (catalog version and service ID)
    s = re.sub(r"('impala\.events\.catalogVersion'\s*=\s*)'[0-9]+'", r"\1'<NUM>'", s)
    s = re.sub(r"('impala\.events\.catalogServiceId'\s*=\s*)'[^']+'", r"\1'<NUM>'", s)
    s = re.sub(r"('impala\.lastComputeStatsTime'\s*=\s*)'[0-9]+'", r"\1'<NUM>'", s)
    s = re.sub(r"('impala\.computeStatsSnapshotIds'\s*=\s*)'[^']+'", r"\1'<NUM>'", s)

    return s

  def __normalize(self, s):
    """ Normalize the string to remove extra whitespaces and remove keys
    from tblproperties and serdeproperties that we don't want
    """
    s = ' '.join(s.split())
    for k in self.FILTER_TBL_PROPERTIES:
      kv_regex = r"'%s'\s*=\s*'[^\']+'\s*,?" % (k)
      s = re.sub(kv_regex, "", s)
    # If we removed the last property, there will be a dangling comma that is not valid
    # e.g. 'k1'='v1', ) -> 'k1'='v1')
    s = re.sub(r",\s*\)", ")", s)
    # Need to remove any whitespace after left parens and before right parens
    s = re.sub(r"\(\s+", "(", s)
    s = re.sub(r"\s+\)", ")", s)
    # If the only properties were removed, the properties sections may be empty, which
    # is not valid
    s = re.sub(r"TBLPROPERTIES\s*\(\s*\)", "", s)
    s = re.sub(r"SERDEPROPERTIES\s*\(\s*\)", "", s)
    # By removing properties in the middle we might ended up having extra whitespaces,
    # let's remove them.
    s = ' '.join(s.split())
    return s

  def __remove_properties_maps(self, s):
    """ Removes the tblproperties and serdeproperties from the string """
    return re.sub(
        properties_map_regex("WITH SERDEPROPERTIES"), "",
        re.sub(properties_map_regex("TBLPROPERTIES"), "", s)).strip()

  def __get_properties_map(self, s, properties_map_name):
    return get_properties_map(s, properties_map_name, self.FILTER_TBL_PROPERTIES)

  def __replace_uri(self, s, uri):
    return s if uri is None else s.replace("$$location_uri$$", uri)

  def __replace_variables(self, s):
    return s.replace("$$warehouse$$", WAREHOUSE)\
      .replace("$$iceberg_default_format_version$$", ICEBERG_DEFAULT_FORMAT_VERSION)


# Represents one show-create-table test case. Performs validation of the test sections
# and provides SQL to execute for each section.
class ShowCreateTableTestCase(object):
  RESULTS_DB_NAME_TOKEN = "show_create_table_test_db"

  def __init__(self, test_section, test_file_name, test_db_name):
    if 'QUERY' in test_section:
      self.existing_table = True
      self.show_create_table_sql = remove_comments(test_section['QUERY']).strip()
    elif 'CREATE_TABLE' in test_section:
      self.__process_create_section(
          test_section['CREATE_TABLE'], test_file_name, test_db_name, 'table')
    elif 'CREATE_VIEW' in test_section:
      self.__process_create_section(
          test_section['CREATE_VIEW'], test_file_name, test_db_name, 'view')
    else:
      assert 0, 'Error in test file %s. Test cases require a '\
          'CREATE_TABLE section.\n%s' %\
          (test_file_name, pprint.pformat(test_section))
    results_key = 'RESULTS-HIVE'
    if HIVE_MAJOR_VERSION > 2:
      if 'RESULTS-HIVE-3' in test_section:
        # If the hive version is greater than 2 use the RESULTS-HIVE-3 available
        results_key = 'RESULTS-HIVE-3'

    expected_result = remove_comments(test_section[results_key])
    self.expected_result = expected_result.replace(
        ShowCreateTableTestCase.RESULTS_DB_NAME_TOKEN, test_db_name)

  def __process_create_section(self, section, test_file_name, test_db_name, table_type):
    self.existing_table = False
    self.create_table_sql = QueryTestSectionReader.build_query(remove_comments(section))
    name = self.__get_table_name(self.create_table_sql, table_type)
    assert name.find(".") == -1, 'Error in test file %s. Found unexpected %s '\
        'name %s that is qualified with a database' % (table_type, test_file_name, name)
    self.table_name = test_db_name + '.' + name
    # Replace all occurrences of the unqualified table name with the qualified name
    # This is needed for test cases with multiple statements (e.g., CREATE + ALTER)
    self.create_table_sql = re.sub(
        r'\b' + re.escape(name) + r'\b',
        self.table_name,
        self.create_table_sql
    )
    self.show_create_table_sql = 'show create %s %s' % (table_type, self.table_name)
    self.drop_table_sql = "drop %s %s" % (table_type, self.table_name)

  def __get_table_name(self, create_table_sql, table_type):
    lexer = shlex.shlex(create_table_sql)
    tokens = list(lexer)
    # sanity check the create table statement
    if len(tokens) < 3 or tokens[0].lower() != "create":
      assert 0, 'Error in test. Invalid CREATE TABLE statement: %s' % (create_table_sql)
    if tokens[1].lower() != table_type.lower() and \
       (tokens[1].lower() != "external" or tokens[2].lower() != table_type.lower()):
      assert 0, 'Error in test. Invalid CREATE TABLE statement: %s' % (create_table_sql)

    if tokens[1].lower() == "external":
      # expect "create external table table_name ..."
      return tokens[3]
    else:
      # expect a create table table_name ...
      return tokens[2]


class TestInfraCompat(ImpalaTestSuite):
  """
  This test suite ensures our test infra (qgen, stress) can always properly read the
  output of "SHOW CREATE TABLE" and find primary keys and updatable columns.
  """

  TABLE_PRIMARY_KEYS_MAPS = [
      {'table': 'tpch.customer',
       'primary_keys': (),
       'updatable_columns': ()},
      {'table': 'tpch_kudu.customer',
       'primary_keys': ('c_custkey',),
       'updatable_columns': ('c_name', 'c_address', 'c_nationkey', 'c_phone',
                             'c_acctbal', 'c_mktsegment', 'c_comment')},
      {'table': 'tpch_kudu.lineitem',
       'primary_keys': ('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber'),
       'updatable_columns': ('l_quantity', 'l_extendedprice', 'l_discount', 'l_tax',
                             'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate',
                             'l_receiptdate', 'l_shipinstruct', 'l_shipmode',
                             'l_comment')}]

  @pytest.mark.parametrize('table_primary_keys_map', TABLE_PRIMARY_KEYS_MAPS)
  def test_primary_key_parse(self, impala_testinfra_cursor, table_primary_keys_map):
    """
    Test the query generator's Impala -> Postgres data migrator's ability to parse primary
    keys via SHOW CREATE TABLE. If this test fails, update _fetch_primary_key_names, or
    fix the SHOW CREATE TABLE defect.
    """
    assert impala_testinfra_cursor._fetch_primary_key_names(
        table_primary_keys_map['table']) == table_primary_keys_map['primary_keys']

  @pytest.mark.parametrize('table_primary_keys_map', TABLE_PRIMARY_KEYS_MAPS)
  def test_load_table_with_primary_key_attr(self, impala_testinfra_cursor,
                                            table_primary_keys_map):
    """
    Test that when we load a table for the query generator that the primary keys are
    found and stored in the object model.
    """
    table = impala_testinfra_cursor.describe_table(table_primary_keys_map['table'])
    assert table_primary_keys_map['primary_keys'] == table.primary_key_names
    assert table_primary_keys_map['updatable_columns'] == table.updatable_column_names


def get_tbl_properties_from_describe_formatted(lines):
  """Get a map of table properties from the result of a DESCRIBE FORMATTED query."""
  # The table properties are between a line that starts with 'Table Parameters' and the
  # next line that doesn't start with a '\t'.
  res = dict()

  start_idx = None
  for idx, value in enumerate(lines):
    if value.startswith('Table Parameters'):
      start_idx = idx + 1
      break
  if start_idx is None: return res

  past_end_idx = start_idx
  for idx, value in enumerate(lines[start_idx:]):
    if not value.startswith('\t'):
      break
    past_end_idx += 1

  for line in lines[start_idx:past_end_idx]:
    [key, value] = line.strip().split('\t')
    res[key.strip()] = value.strip()

  return res


class TestShowCreateTableIcebergProperties(ImpalaTestSuite):
  """
  Test that the SHOW CREATE TABLE statement does not contain irrelevant Iceberg-related
  table properties.
  """

  @classmethod
  def add_test_dimensions(cls):
    super(TestShowCreateTableIcebergProperties, cls).add_test_dimensions()
    # don't use any exec options, running exactly once is fine
    cls.ImpalaTestMatrix.clear_dimension('exec_option')
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  def test_iceberg_properties(self, unique_database):
    """
    Test that the SHOW CREATE TABLE statement does not contain irrelevant Iceberg-related
    table properties.
    """
    tbl_name = unique_database + ".a"

    create_sql = "CREATE TABLE {} (i INT, d DATE, s STRING, t TIMESTAMP) \
        PARTITIONED BY SPEC (BUCKET(5, i), MONTH(d), TRUNCATE(3, s), HOUR(t)) \
        SORT BY (i) \
        STORED AS ICEBERG".format(tbl_name)
    self.execute_query_expect_success(self.client, create_sql)

    # Some table properties are only added when a new Iceberg snapshot is created.
    # insert_sql = "insert into {} values (1)".format(tbl_name)
    insert_sql = ("INSERT INTO {} VALUES (1, '2022-01-04', 'some', '2022-01-04 10:00:00')"
        .format(tbl_name))
    self.execute_query_expect_success(self.client, insert_sql)

    do_not_update_stats_sql = ("alter table {} set TBLPROPERTIES "
        "('DO_NOT_UPDATE_STATS'='true')".format(tbl_name))
    self.execute_query_expect_success(self.client, do_not_update_stats_sql)

    describe_sql = "describe formatted {}".format(tbl_name)
    describe_res = self.execute_query_expect_success(self.client, describe_sql)

    show_create_sql = "show create table {}".format(tbl_name)
    show_create_res = self.execute_query_expect_success(self.client, show_create_sql)

    tblproperties_to_omit = [
        "storage_handler",
        "DO_NOT_UPDATE_STATS",
        "metadata_location",
        "previous_metadata_location",
        "current-schema",
        "snapshot-count",
        "current-snapshot-id",
        "current-snapshot-summary",
        "current-snapshot-timestamp-ms",
        "default-partition-spec",
        "uuid",
        "impala.events.catalogServiceId",
        "impala.events.catalogVersion",
        "sort.columns",
        "sort.order",
        "numFiles",
        "numRows",
        "totalSize"
    ]

    tbl_props_in_describe = get_tbl_properties_from_describe_formatted(describe_res.data)
    tbl_props_in_show_create = get_properties_map(
        show_create_res.data[0], 'TBLPROPERTIES', None)
    for tbl_prop in tblproperties_to_omit:
      assert tbl_prop in tbl_props_in_describe
      assert tbl_prop not in tbl_props_in_show_create
