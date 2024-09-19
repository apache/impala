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

# Targeted Impala tests for different tuple delimiters, field delimiters,
# and escape characters.
#

from __future__ import absolute_import, division, print_function
from subprocess import check_call
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)

class TestDelimitedText(ImpalaTestSuite):
  """
  Tests delimited text files with different tuple delimiters, field delimiters
  and escape characters.
  """

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDelimitedText, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Only run on delimited text with no compression.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_delimited_text(self, vector, unique_database):
    self.run_test_case('QueryTest/delimited-text', vector, unique_database)

  def test_delimited_text_newlines(self, vector, unique_database):
    """ Test text with newlines in strings - IMPALA-1943. Execute queries from Python to
    avoid issues with newline handling in test file format. """
    self.execute_query_expect_success(self.client, """
      create table if not exists %s.nl_queries
      (c1 string, c2 string, c3 string)
      row format delimited
      fields terminated by '\002'
      lines terminated by '\001'
      stored as textfile
      """ % unique_database)
    # Create test data with newlines in various places
    self.execute_query_expect_success(self.client, """
      insert into %s.nl_queries
      values ("the\\n","\\nquick\\nbrown","fox\\n"),
             ("\\njumped","over the lazy\\n","\\ndog")""" % unique_database)
    result = self.execute_query("select * from %s.nl_queries" % unique_database)
    assert len(result.data) == 2
    assert result.data[0].split("\t") == ["the\n", "\nquick\nbrown", "fox\n"]
    assert result.data[1].split("\t") == ["\njumped","over the lazy\n","\ndog"]
    # The row count may be computed without parsing each row, so could be inconsistent.
    result = self.execute_query("select count(*) from %s.nl_queries" % unique_database)
    assert len(result.data) == 1
    assert result.data[0] == "2"

  def test_delimited_text_latin_chars(self, vector, unique_database):
    """Verifies Impala is able to properly handle delimited text that contains
    extended ASCII/latin characters. Marked as running serial because of shared
    cleanup/setup"""
    self.run_test_case('QueryTest/delimited-latin-text', vector, unique_database,
      encoding="latin-1")

  def test_large_file_of_field_delimiters(self, vector, unique_database):
    """IMPALA-13161: Verifies reading a large file which has full of field delimiters
       won't causing crash due to overflows"""
    tbl = unique_database + ".tbl"
    self.execute_query("create table {}(i int)".format(tbl))
    table_loc = self._get_table_location(tbl, vector)
    # Generate a 3GB data file that has full of '\x00' (the default field delimiter)
    with open("data.txt", "wb") as f:
      long_str = "\x00" * 1024 * 1024 * 3
      [f.write(long_str) for i in range(1024)]
    check_call(["hdfs", "dfs", "-put", "data.txt", table_loc])
    self.execute_query("refresh " + tbl)
    res = self.execute_query("select count(*) from " + tbl)
    assert res.data == ["1"]
