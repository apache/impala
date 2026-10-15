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

"""Definitions of the Iceberg REST catalog tables used by the REST DML tests. CREATE
TABLE is not supported for REST catalog tables, so IcebergRestCatalogTests creates the
tables declared in RestServerProperties through the REST API.

A definition is a dict with 'fields', and optionally 'partition' (partition spec
fields) and 'properties' (table properties, including 'format-version')."""


def _rest_col(field_id, name, col_type):
  return {'id': field_id, 'name': name, 'required': False, 'type': col_type}


def _rest_part(source_id, field_id, name, transform):
  return {'source-id': source_id, 'field-id': field_id, 'name': name,
          'transform': transform}


def _v3(tables):
  return dict((name, dict(table, properties=dict(
      table.get('properties', {}), **{'format-version': '3'})))
      for name, table in tables.items())


def create_table_request(table_name, table):
  """Returns the REST API request that creates 'table_name' from its definition."""
  request = {
      'name': table_name,
      'schema': {'type': 'struct', 'schema-id': 0, 'fields': table['fields']},
      'properties': table.get('properties', {}),
  }
  if 'partition' in table:
    request['partition-spec'] = {'spec-id': 0, 'fields': table['partition']}
  return request


# The 'test_table' and 'partitioned_test_table' of RestServerProperties.
INSERT_TEST_TABLE = {
    'fields': [_rest_col(1, 'i', 'int')],
    'properties': {'format-version': '2'}}
PARTITIONED_INSERT_TEST_TABLE = {
    'fields': [_rest_col(1, 'i', 'int'), _rest_col(2, 'p', 'int')],
    'partition': [_rest_part(2, 1000, 'p', 'identity')],
    'properties': {'format-version': '2'}}


# Tables of iceberg-rest-update.test, adapted from iceberg-v3-update.test.
UPDATE_TEST_TABLES = _v3({
    'update_basic': {'fields': [_rest_col(1, 'i', 'int'), _rest_col(2, 's', 'string')]},
    'update_multi': {'fields': [_rest_col(1, 'i', 'int'), _rest_col(2, 's', 'string')]},
    'single_col': {'fields': [_rest_col(1, 'i', 'int')]},
    'ice_alltypes': {'fields': [
        _rest_col(1, 'bool_col', 'boolean'), _rest_col(2, 'int_col', 'int'),
        _rest_col(3, 'bigint_col', 'long'), _rest_col(4, 'float_col', 'float'),
        _rest_col(5, 'double_col', 'double'),
        _rest_col(6, 'dec_10_0_col', 'decimal(10, 0)'),
        _rest_col(7, 'dec_8_2_col', 'decimal(8, 2)'),
        _rest_col(8, 'date_col', 'date'), _rest_col(9, 'timestamp_col', 'timestamp'),
        _rest_col(10, 'string_col', 'string'), _rest_col(11, 'binary_col', 'binary')]},
    'ref_table': {'fields': [
        _rest_col(1, 'i', 'int'), _rest_col(2, 'bi', 'long'),
        _rest_col(3, 's', 'string'), _rest_col(4, 'd', 'date')]},
})


def _int_and(name, col_type):
  return [_rest_col(1, 'i', 'int'), _rest_col(2, name, col_type)]


# Mirrors the CREATE TABLE statements of iceberg-v3-update-partitions.test.
UPDATE_PARTITIONS_TEST_TABLES = _v3({
    'id_part': {
        'fields': [_rest_col(1, 'i', 'int'), _rest_col(2, 's', 'string')],
        'partition': [_rest_part(1, 1000, 'i', 'identity')]},
    'trunc_part': {
        'fields': [_rest_col(1, 'i', 'int'), _rest_col(2, 's', 'string')],
        'partition': [_rest_part(2, 1000, 's_trunc', 'truncate[1]')]},
    'multi_part': {
        'fields': [_rest_col(1, 'i', 'int'), _rest_col(2, 's', 'string'),
                   _rest_col(3, 'f', 'double')],
        'partition': [_rest_part(1, 1000, 'i_bucket', 'bucket[7]'),
                      _rest_part(2, 1001, 's_trunc', 'truncate[1]')],
        'properties': {'sort.columns': 'f,i', 'sort.order': 'LEXICAL'}},
    'date_year_part': {
        'fields': _int_and('d', 'date'),
        'partition': [_rest_part(2, 1000, 'd_year', 'year')]},
    'date_month_part': {
        'fields': _int_and('d', 'date'),
        'partition': [_rest_part(2, 1000, 'd_month', 'month')]},
    'date_day_part': {
        'fields': _int_and('d', 'date'),
        'partition': [_rest_part(2, 1000, 'd_day', 'day')]},
    'ts_year_part': {
        'fields': _int_and('ts', 'timestamp'),
        'partition': [_rest_part(2, 1000, 'ts_year', 'year')]},
    'ts_month_part': {
        'fields': _int_and('ts', 'timestamp'),
        'partition': [_rest_part(2, 1000, 'ts_month', 'month')]},
    'ts_day_part': {
        'fields': _int_and('ts', 'timestamp'),
        'partition': [_rest_part(2, 1000, 'ts_day', 'day')]},
    'ts_hour_part': {
        'fields': _int_and('ts', 'timestamp'),
        'partition': [_rest_part(2, 1000, 'ts_hour', 'hour')]},
    'numeric_truncate': {
        'fields': [_rest_col(1, 'id', 'int'), _rest_col(2, 'int_col', 'int'),
                   _rest_col(3, 'bigint_col', 'long'),
                   _rest_col(4, 'dec_8_0', 'decimal(8, 0)'),
                   _rest_col(5, 'dec_10_2', 'decimal(10, 2)')],
        'partition': [_rest_part(2, 1000, 'int_col_trunc', 'truncate[10]'),
                      _rest_part(3, 1001, 'bigint_col_trunc', 'truncate[1000]'),
                      _rest_part(1, 1002, 'id_null', 'void'),
                      _rest_part(4, 1003, 'dec_8_0_trunc', 'truncate[20]'),
                      _rest_part(5, 1004, 'dec_10_2_trunc', 'truncate[50]')]},
})
