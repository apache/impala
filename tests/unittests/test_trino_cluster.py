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

from unittest.mock import patch

import pytest

from tests.common.base_test_suite import BaseTestSuite
from tests.common.trino_cluster import normalize_trino_type
from tests.common.trino_cluster import parse_trino_describe_output
from tests.common.trino_cluster import TrinoCluster
from tests.common.trino_cluster import TrinoQueryResult


DESCRIBE_OUTPUT = """
{"Column Name":"id","Catalog":"iceberg","Schema":"db","Table":"t",\
"Type":"integer","Type Size":4,"Aliased":false}
{"Column Name":"name","Catalog":"iceberg","Schema":"db","Table":"t",\
"Type":"varchar","Type Size":0,"Aliased":false}
"""


class TestTrinoCluster(BaseTestSuite):

  @pytest.mark.parametrize('trino_type, expected', [
      ('integer', 'INT'),
      ('real', 'FLOAT'),
      ('varchar', 'STRING'),
      ('varchar(12)', 'VARCHAR'),
      ('varbinary', 'BINARY'),
      ('decimal(10,2)', 'DECIMAL'),
      ('array(integer)', 'ARRAY'),
      ('map(varchar, bigint)', 'MAP'),
      ('row(id integer, name varchar)', 'STRUCT'),
      ('timestamp(6)', 'TIMESTAMP'),
      ('timestamp(3) with time zone', 'TIMESTAMP WITH TIME ZONE'),
      ('uuid', 'UUID'),
  ])
  def test_normalize_trino_type(self, trino_type, expected):
    assert normalize_trino_type(trino_type) == expected

  def test_parse_trino_describe_output(self):
    assert parse_trino_describe_output(DESCRIBE_OUTPUT) == (
        ['id', 'name'], ['INT', 'STRING'])

  def test_trino_query_result_keeps_metadata_for_empty_results(self):
    result = TrinoQueryResult(
        'SELECT id FROM t WHERE false', '', '', 0,
        column_labels=['id'], column_types=['INT'])
    assert result.success
    assert result.column_labels == ['id']
    assert result.column_types == ['INT']
    assert result.data == []

  def test_trino_query_result_keeps_missing_metadata_explicit(self):
    result = TrinoQueryResult('SELECT id FROM t WHERE false', '', '', 0)
    assert result.success
    assert result.column_labels == []
    assert result.column_types is None
    assert result.data == []

  def test_trino_query_result_formats_primitive_wire_values(self):
    # These representations come from Trino CLI 482 with --output-format JSON.
    result = TrinoQueryResult(
        'SELECT values',
        '{"d":1.25,"dec":"1.25","c":"x  ","bin":"01",'
        '"dt":"2026-01-02"}\n', '', 0,
        column_labels=['d', 'dec', 'c', 'bin', 'dt'],
        column_types=['DOUBLE', 'DECIMAL', 'CHAR', 'BINARY', 'DATE'])
    assert result.data == ["1.25,'1.25','x  ','01','2026-01-02'"]

  @pytest.mark.parametrize('column_type, value', [
      ('FLOAT', 'NaN'),
      ('FLOAT', 'Infinity'),
      ('FLOAT', '-Infinity'),
      ('DOUBLE', 'NaN'),
      ('DOUBLE', 'Infinity'),
      ('DOUBLE', '-Infinity'),
  ])
  def test_trino_query_result_formats_non_finite_floats(
      self, column_type, value):
    result = TrinoQueryResult(
        'SELECT value', '{"value":"%s"}\n' % value, '', 0,
        column_labels=['value'], column_types=[column_type])
    assert result.data == [value]

  def test_trino_query_result_keeps_non_finite_spelling_quoted_for_strings(self):
    result = TrinoQueryResult(
        'SELECT value', '{"value":"NaN"}\n', '', 0,
        column_labels=['value'], column_types=['STRING'])
    assert result.data == ["'NaN'"]

  def test_get_query_metadata_uses_one_cli_session(self):
    cluster = TrinoCluster()
    with patch.object(
        cluster, 'run_query',
        return_value=(DESCRIBE_OUTPUT, 'PREPARE\n', 0)) as run_query:
      assert cluster.get_query_metadata(
          'SELECT id, name FROM t', schema='db', user='alice') == (
              ['id', 'name'], ['INT', 'STRING'])

    metadata_sql = run_query.call_args.args[0]
    assert metadata_sql == (
        'PREPARE __impala_test_query FROM\nSELECT id, name FROM t\n;\n'
        'DESCRIBE OUTPUT __impala_test_query')
    assert run_query.call_args.kwargs['schema'] == 'db'
    assert run_query.call_args.kwargs['user'] == 'alice'

  def test_get_query_metadata_rejects_empty_describe_output(self):
    cluster = TrinoCluster()
    with patch.object(cluster, 'run_query', return_value=('', 'PREPARE\n', 0)):
      with pytest.raises(RuntimeError, match='DESCRIBE OUTPUT returned no columns'):
        cluster.get_query_metadata('SELECT id FROM t WHERE false')
