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
#
# Authorization tests for the avro.schema.url table property.
#
# Both HDFS and HTTP values of avro.schema.url are now subject to Ranger URI
# privilege enforcement via HdfsUri.analyze().  A user must hold the ALL
# privilege on the URI to use it in CREATE TABLE or ALTER TABLE statements.
# When avro.schema.literal is also present, the URL is never evaluated and
# no URI privilege is required.

from __future__ import absolute_import
from getpass import getuser

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.filesystem_utils import get_fs_path

# Ranger cluster args (consistent with test_ranger.py)
IMPALAD_ARGS = ("--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala --authorization_provider=ranger")
CATALOGD_ARGS = ("--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala --authorization_provider=ranger")

# An HDFS path that exists in the standard test-warehouse layout.
AVRO_SCHEMA_HDFS_URL = get_fs_path(
    "/test-warehouse/avro_schemas/functional/alltypes.json")

# A minimal valid Avro schema literal (used to verify that the literal short-circuits
# any URI privilege check).
AVRO_SCHEMA_LITERAL = (
    '{"name": "my_record", "type": "record", '
    '"fields": [{"name": "s", "type": "string"}]}'
)

# Ranger "admin" user - has server-level ALL privilege by default in the test env.
ADMIN = "admin"
OWNER_USER = getuser()

# Error substring emitted when a Ranger URI privilege is missing.
AUTHZ_ERROR = "does not have privileges"


@CustomClusterTestSuite.with_args(impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
class TestAvroSchemaUrlAuthz(CustomClusterTestSuite):
  """
  Tests that avro.schema.url values (both HDFS and HTTP) are subject to Ranger URI
  privilege enforcement.  Tests are independent of each other; each method creates and
  tears down its own resources.
  """

  # ---------------------------------------------------------------------------
  # Internal helpers
  # ---------------------------------------------------------------------------

  def _setup_db(self, admin_client, db):
    """Create *db* and grant the minimum privileges needed for OWNER_USER
    to run CREATE/ALTER TABLE statements in that database."""
    admin_client.execute(
        "drop database if exists {0} cascade".format(db), user=ADMIN)
    admin_client.execute(
        "create database {0}".format(db), user=ADMIN)
    # The user needs CREATE on the database to issue CREATE TABLE.
    admin_client.execute(
        "grant create on database {0} to user {1}".format(db, OWNER_USER), user=ADMIN)
    # The user also needs ALTER on the database to issue ALTER TABLE.
    admin_client.execute(
        "grant alter on database {0} to user {1}".format(db, OWNER_USER), user=ADMIN)
    admin_client.execute("refresh authorization", user=ADMIN)

  def _teardown_db(self, admin_client, db):
    admin_client.execute(
        "revoke create on database {0} from user {1}".format(db, OWNER_USER), user=ADMIN)
    admin_client.execute(
        "revoke alter on database {0} from user {1}".format(db, OWNER_USER), user=ADMIN)
    admin_client.execute(
        "drop database if exists {0} cascade".format(db), user=ADMIN)

  # ---------------------------------------------------------------------------
  # HDFS avro.schema.url - CREATE TABLE
  # ---------------------------------------------------------------------------

  def test_hdfs_url_create_table_denied_without_uri_privilege(self, unique_name):
    """CREATE TABLE with an HDFS avro.schema.url fails when the user has no URI ALL."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      result = self.execute_query_expect_failure(
          self.client,
          "create table {db}.t stored as avro tblproperties "
          "('avro.schema.url'='{url}')".format(db=db, url=AVRO_SCHEMA_HDFS_URL),
          user=OWNER_USER)
      assert AUTHZ_ERROR in str(result)
    finally:
      self._teardown_db(admin_client, db)

  def test_hdfs_url_create_table_allowed_with_uri_privilege(self, unique_name):
    """CREATE TABLE with an HDFS avro.schema.url succeeds when the user holds URI ALL."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      admin_client.execute(
          "grant all on uri '{url}' to user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      admin_client.execute("refresh authorization", user=ADMIN)
      self.execute_query_expect_success(
          self.client,
          "create table {db}.t stored as avro tblproperties "
          "('avro.schema.url'='{url}')".format(db=db, url=AVRO_SCHEMA_HDFS_URL),
          query_options={'sync_ddl': 1},
          user=OWNER_USER)
    finally:
      admin_client.execute(
          "revoke all on uri '{url}' from user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      self._teardown_db(admin_client, db)

  # ---------------------------------------------------------------------------
  # HDFS avro.schema.url - ALTER TABLE SET TBLPROPERTIES
  # ---------------------------------------------------------------------------

  def test_hdfs_url_alter_tblproperties_denied_without_uri_privilege(self, unique_name):
    """ALTER TABLE SET TBLPROPERTIES with HDFS avro.schema.url fails without URI ALL."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      admin_client.execute(
          "create table {db}.t (s string) stored as avro".format(db=db), user=ADMIN)
      result = self.execute_query_expect_failure(
          self.client,
          "alter table {db}.t set tblproperties "
          "('avro.schema.url'='{url}')".format(db=db, url=AVRO_SCHEMA_HDFS_URL),
          user=OWNER_USER)
      assert AUTHZ_ERROR in str(result)
    finally:
      self._teardown_db(admin_client, db)

  def test_hdfs_url_alter_tblproperties_allowed_with_uri_privilege(self, unique_name):
    """ALTER TABLE SET TBLPROPERTIES with HDFS avro.schema.url succeeds with URI ALL."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      admin_client.execute(
          "create table {db}.t (s string) stored as avro".format(db=db), user=ADMIN)
      admin_client.execute(
          "grant all on uri '{url}' to user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      admin_client.execute("refresh authorization", user=ADMIN)
      self.execute_query_expect_success(
          self.client,
          "alter table {db}.t set tblproperties "
          "('avro.schema.url'='{url}')".format(db=db, url=AVRO_SCHEMA_HDFS_URL),
          query_options={'sync_ddl': 1},
          user=OWNER_USER)
    finally:
      admin_client.execute(
          "revoke all on uri '{url}' from user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      self._teardown_db(admin_client, db)

  # ---------------------------------------------------------------------------
  # HDFS avro.schema.url - ALTER TABLE SET SERDEPROPERTIES
  # ---------------------------------------------------------------------------

  def test_hdfs_url_alter_serdeproperties_denied_without_uri_privilege(
      self, unique_name):
    """ALTER TABLE SET SERDEPROPERTIES with HDFS avro.schema.url fails without URI ALL."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      admin_client.execute(
          "create table {db}.t (s string) stored as avro".format(db=db), user=ADMIN)
      result = self.execute_query_expect_failure(
          self.client,
          "alter table {db}.t set serdeproperties "
          "('avro.schema.url'='{url}')".format(db=db, url=AVRO_SCHEMA_HDFS_URL),
          user=OWNER_USER)
      assert AUTHZ_ERROR in str(result)
    finally:
      self._teardown_db(admin_client, db)

  def test_hdfs_url_alter_serdeproperties_allowed_with_uri_privilege(self, unique_name):
    """ALTER TABLE SET SERDEPROPERTIES with HDFS avro.schema.url succeeds with URI ALL."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      admin_client.execute(
          "create table {db}.t (s string) stored as avro".format(db=db), user=ADMIN)
      admin_client.execute(
          "grant all on uri '{url}' to user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      admin_client.execute("refresh authorization", user=ADMIN)
      self.execute_query_expect_success(
          self.client,
          "alter table {db}.t set serdeproperties "
          "('avro.schema.url'='{url}')".format(db=db, url=AVRO_SCHEMA_HDFS_URL),
          query_options={'sync_ddl': 1},
          user=OWNER_USER)
    finally:
      admin_client.execute(
          "revoke all on uri '{url}' from user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      self._teardown_db(admin_client, db)

  # ---------------------------------------------------------------------------
  # HTTP avro.schema.url - CREATE TABLE
  #
  # HTTP URLs go through the same HdfsUri.analyze() path as HDFS URLs and are
  # therefore also subject to Ranger URI privilege enforcement.
  # ---------------------------------------------------------------------------

  def test_http_url_create_table_denied_without_uri_privilege(self, unique_name):
    """CREATE TABLE with an HTTP avro.schema.url fails when the user has no URI ALL."""
    db = unique_name + "_db"
    http_urls = ["http://example.com/schema.json", "https://example.com/schema.json"]
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      for http_url in http_urls:
        result = self.execute_query_expect_failure(
            self.client,
            "create table {db}.t stored as avro tblproperties "
            "('avro.schema.url'='{url}')".format(db=db, url=http_url),
            user=OWNER_USER)
        assert AUTHZ_ERROR in str(result)
    finally:
      self._teardown_db(admin_client, db)

  def test_http_url_create_table_allowed_with_uri_privilege(self, unique_name):
    """CREATE TABLE with an HTTP avro.schema.url succeeds when the user holds URI ALL."""
    db = unique_name + "_db"
    http_urls = ["http://example.com/schema.json", "https://example.com/schema.json"]
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      for http_url in http_urls:
        admin_client.execute("grant all on uri '{url}' to user {user}"
                             .format(url=http_url, user=OWNER_USER), user=ADMIN)
      admin_client.execute("refresh authorization", user=ADMIN)
      # The URI privilege check passes during analysis; the HTTP fetch itself may
      # fail at execution time, but that is outside the scope of this test.
      # We only verify that no AuthorizationException is raised.
      try:
        for http_url in http_urls:
          self.execute_query_expect_success(
              self.client,
              "create table {db}.t stored as avro tblproperties "
              "('avro.schema.url'='{url}')".format(db=db, url=http_url),
              query_options={'sync_ddl': 1},
              user=OWNER_USER)
      except Exception as e:
        assert AUTHZ_ERROR not in str(e), \
            "Got unexpected AuthorizationException: {0}".format(e)
    finally:
      for http_url in http_urls:
        admin_client.execute("revoke all on uri '{url}' from user {user}"
                             .format(url=http_url, user=OWNER_USER), user=ADMIN)
      self._teardown_db(admin_client, db)

  # ---------------------------------------------------------------------------
  # avro.schema.literal short-circuits URI privilege check
  # ---------------------------------------------------------------------------

  def test_literal_short_circuits_uri_privilege_check(self, unique_name):
    """avro.schema.literal takes precedence over avro.schema.url.

    When both properties are present, avro.schema.url is never evaluated, so no
    URI privilege is required even when the user has none.
    """
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      # No URI privilege granted - but the literal is present so the URL is ignored.
      self.execute_query_expect_success(
          self.client,
          "create table {db}.t stored as avro tblproperties ("
          "'avro.schema.literal'='{lit}', "
          "'avro.schema.url'='http://example.com/schema.json')".format(
              db=db, lit=AVRO_SCHEMA_LITERAL),
          query_options={'sync_ddl': 1},
          user=OWNER_USER)
    finally:
      self._teardown_db(admin_client, db)

  # ---------------------------------------------------------------------------
  # ALTER TABLE SET FILEFORMAT AVRO - table already has avro.schema.url
  #
  # When the target table already has avro.schema.url in its tblproperties or
  # serdeproperties, switching the file format to AVRO triggers a URI privilege
  # check for that URL, just as if the URL were being set for the first time.
  # ---------------------------------------------------------------------------

  def test_set_fileformat_avro_denied_without_uri_privilege(self, unique_name):
    """ALTER TABLE SET FILEFORMAT AVRO fails without URI ALL when table has
    avro.schema.url in its tblproperties."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      # Admin creates a non-AVRO table that already has avro.schema.url set so that
      # switching to AVRO format will trigger the URI privilege check.
      admin_client.execute(
          "create table {db}.t (s string) "
          "tblproperties ('avro.schema.url'='{url}')".format(
              db=db, url=AVRO_SCHEMA_HDFS_URL), user=ADMIN)
      # OWNER_USER has ALTER on the database but no URI ALL - expect denial.
      result = self.execute_query_expect_failure(
          self.client,
          "alter table {db}.t set fileformat avro".format(db=db),
          user=OWNER_USER)
      assert AUTHZ_ERROR in str(result)
    finally:
      self._teardown_db(admin_client, db)

  def test_set_fileformat_avro_allowed_with_uri_privilege(self, unique_name):
    """ALTER TABLE SET FILEFORMAT AVRO succeeds when the user holds ALTER on the
    table and URI ALL on the avro.schema.url already stored in tblproperties."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      admin_client.execute(
          "create table {db}.t (s string) "
          "tblproperties ('avro.schema.url'='{url}')".format(
              db=db, url=AVRO_SCHEMA_HDFS_URL), user=ADMIN)
      admin_client.execute(
          "grant all on uri '{url}' to user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      admin_client.execute("refresh authorization", user=ADMIN)
      self.execute_query_expect_success(
          self.client,
          "alter table {db}.t set fileformat avro".format(db=db),
          query_options={'sync_ddl': 1},
          user=OWNER_USER)
    finally:
      admin_client.execute(
          "revoke all on uri '{url}' from user {user}".format(
              url=AVRO_SCHEMA_HDFS_URL, user=OWNER_USER), user=ADMIN)
      self._teardown_db(admin_client, db)

  def test_set_fileformat_avro_no_url_no_uri_check(self, unique_name):
    """ALTER TABLE SET FILEFORMAT AVRO requires no URI privilege when the table
    has no avro.schema.url in its existing properties."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      # Plain table with no avro.schema.url - no URI check should fire.
      admin_client.execute(
          "create table {db}.t (s string)".format(db=db), user=ADMIN)
      self.execute_query_expect_success(
          self.client,
          "alter table {db}.t set fileformat avro".format(db=db),
          query_options={'sync_ddl': 1},
          user=OWNER_USER)
    finally:
      self._teardown_db(admin_client, db)

  def test_set_fileformat_avro_serdeproperties_url_denied(self, unique_name):
    """ALTER TABLE SET FILEFORMAT AVRO also triggers a URI check when
    avro.schema.url is in serdeproperties rather than tblproperties."""
    db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      self._setup_db(admin_client, db)
      admin_client.execute(
          "create table {db}.t (s string) "
          "with serdeproperties ('avro.schema.url'='{url}')".format(
              db=db, url=AVRO_SCHEMA_HDFS_URL), user=ADMIN)
      result = self.execute_query_expect_failure(
          self.client,
          "alter table {db}.t set fileformat avro".format(db=db),
          user=OWNER_USER)
      assert AUTHZ_ERROR in str(result)
    finally:
      self._teardown_db(admin_client, db)
