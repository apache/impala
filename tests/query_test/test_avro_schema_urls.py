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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import get_fs_path

# HDFS location of the Avro schema used by avro.schema.url guard-rail tests.
AVRO_SCHEMA_URL_LOC = get_fs_path(
    "/test-warehouse/avro_schemas/functional/alltypes.json")

# Minimal valid Avro schema as a SQL string literal (single-quoted).
AVRO_SCHEMA_LITERAL_SQL = (
    "'{\"name\": \"my_record\", \"type\": \"record\", "
    "\"fields\": [{\"name\": \"s\", \"type\": \"string\"}]}'"
)


class TestAvroSchemaUrls(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAvroSchemaUrls, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'avro')

  def test_avro_schema_checks(self, vector, unique_database):
    """Tests that invalid avro.schema.literal/url values (bad JSON, unsupported types,
    invalid decimal properties) are rejected with clear errors at DDL execution time."""
    self.run_test_case('QueryTest/avro-schema-checks', vector, unique_database)

  def test_avro_schema_reconciliation(self, vector, unique_database):
    """Tests that schema reconciliation produces the correct column metadata when
    column definitions disagree with the Avro schema (count mismatch, name mismatch,
    type mismatch)."""
    self.run_test_case('QueryTest/avro-schema-reconciliation', vector, unique_database)

  def test_hdfs_valid_url_create(self, unique_database):
    """Valid HDFS avro.schema.url is accepted for CREATE TABLE."""
    self.client.execute(
        "create table {db}.t stored as avro tblproperties "
        "('avro.schema.url'='{loc}')".format(
            db=unique_database, loc=AVRO_SCHEMA_URL_LOC))

  def test_hdfs_valid_url_alter(self, unique_database):
    """Valid HDFS avro.schema.url is accepted for ALTER TABLE SET TBLPROPERTIES."""
    self.client.execute(
        "create table {db}.t (s string) stored as avro".format(db=unique_database))
    self.client.execute(
        "alter table {db}.t set tblproperties "
        "('avro.schema.url'='{loc}')".format(
            db=unique_database, loc=AVRO_SCHEMA_URL_LOC))

  def test_hdfs_valid_url_serdeproperties(self, unique_database):
    """Valid HDFS avro.schema.url is accepted for ALTER TABLE SET SERDEPROPERTIES."""
    self.client.execute(
        "create table {db}.t (s string) stored as avro".format(db=unique_database))
    self.client.execute(
        "alter table {db}.t set serdeproperties "
        "('avro.schema.url'='{loc}')".format(
            db=unique_database, loc=AVRO_SCHEMA_URL_LOC))

  def test_hdfs_nonexistent_url(self, unique_database):
    """Non-existent HDFS avro.schema.url is rejected."""
    nonexistent = get_fs_path(
        "/test-warehouse/avro_schemas/nonexistent_dir/schema.json")
    err = self.execute_query_expect_failure(
        self.client,
        "create table {db}.t stored as avro tblproperties "
        "('avro.schema.url'='{loc}')".format(db=unique_database, loc=nonexistent))
    err_str = str(err)
    assert "does not exist" in err_str

  def test_literal_overrides_http_url(self, unique_database):
    """avro.schema.literal takes precedence; avro.schema.url is never evaluated.

    When a literal schema is present it takes priority, so the URL is never reached
    and no URI privilege check is performed.
    """
    self.client.execute(
        "create table {db}.t stored as avro tblproperties "
        "('avro.schema.literal'={lit}, "
        "'avro.schema.url'='http://example.com/schema.json')".format(
            db=unique_database, lit=AVRO_SCHEMA_LITERAL_SQL))
