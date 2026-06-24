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

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.filesystem_utils import FILESYSTEM_URI_SCHEME

# ---------------------------------------------------------------------------
# Error substrings for flag-enforcement checks
# ---------------------------------------------------------------------------
HTTP_DISABLED_ERROR = "does not permit HTTP(S)"
HOST_NOT_ALLOWED_ERROR = "is not permitted for avro.schema.url HTTP(S) fetching"
SCHEME_NOT_ALLOWED_ERROR = "is not permitted for avro.schema.url"

# The impalad debug web UI returns JSON at this URL - used as a convenient
# HTTP test server that is always reachable within the cluster.
VARZ_HTTP_URL = "http://localhost:25000/varz?json"
VARZ_HTTPS_URL = "https://localhost:25000/varz?json"


@CustomClusterTestSuite.with_args(cluster_size=1,
    catalogd_args="--avro_schema_url_remote_http_enabled=false")
class TestAvroSchemaUrlHttpDisabled(CustomClusterTestSuite):
  """HTTP schema fetching is rejected when avro_schema_url_remote_http_enabled=false
  (the default)."""

  @pytest.mark.parametrize("url", [VARZ_HTTP_URL, VARZ_HTTPS_URL])
  def test_http_rejected_when_disabled(self, unique_name, url):
    """CREATE TABLE with an HTTP avro.schema.url fails with the SSRF-guard error."""
    try:
      result = self.execute_query_expect_failure(
          self.client,
          "create table default.{t} stored as avro tblproperties "
          "('avro.schema.url'='{url}')".format(t=unique_name, url=url))
      assert HTTP_DISABLED_ERROR in str(result)
    finally:
      self.client.execute(
          "drop table if exists default.{t}".format(t=unique_name))


@CustomClusterTestSuite.with_args(cluster_size=1,
    catalogd_args=("--avro_schema_url_remote_http_enabled=true "
                   "--avro_schema_url_http_allowed_hosts=localhost"))
class TestAvroSchemaUrlHttpEnabled(CustomClusterTestSuite):
  """HTTP schema fetching is gated on host allowlist when
  avro_schema_url_remote_http_enabled=true."""

  @pytest.mark.parametrize("url,exc", [(VARZ_HTTP_URL, 'SchemaParseException'),
                                       (VARZ_HTTPS_URL, 'SSLHandshakeException')])
  def test_allowed_host_fetch_attempted(self, unique_name, url, exc):
    """A URL whose host is in avro_schema_url_http_allowed_hosts is not blocked
    by the flag guard. localhost:25000/varz?json returns valid JSON but not a
    valid Avro schema, so the DDL fails at schema-parse time, not at the flag
    check."""
    try:
      result = self.execute_query_expect_failure(
          self.client,
          "create table default.{t} stored as avro tblproperties "
          "('avro.schema.url'='{url}')".format(t=unique_name, url=url))
      assert HTTP_DISABLED_ERROR not in str(result)
      assert HOST_NOT_ALLOWED_ERROR not in str(result)
      assert exc in str(result)
    finally:
      self.client.execute(
          "drop table if exists default.{t}".format(t=unique_name))

  @pytest.mark.parametrize("url", ["http://external.example.com/schema.json",
                                   "https://external.example.com/schema.json"])
  def test_unlisted_host_rejected(self, unique_name, url):
    """A URL whose host is absent from avro_schema_url_http_allowed_hosts is
    rejected by the flag guard."""
    try:
      result = self.execute_query_expect_failure(
          self.client,
          "create table default.{t} stored as avro tblproperties "
          "('avro.schema.url'='{url}')".format(t=unique_name, url=url))
      assert HOST_NOT_ALLOWED_ERROR in str(result)
    finally:
      self.client.execute(
          "drop table if exists default.{t}".format(t=unique_name))


@CustomClusterTestSuite.with_args(cluster_size=1,
    catalogd_args="--avro_schema_url_allowed_schemes=")
class TestAvroSchemaUrlAllowedSchemes(CustomClusterTestSuite):
  """avro_schema_url_allowed_schemes restricts which URI schemes may be fetched."""

  def test_unlisted_scheme_rejected(self, unique_name):
    """A URI whose scheme is absent from avro_schema_url_allowed_schemes is
    rejected by the flag guard."""
    fs_url = FILESYSTEM_URI_SCHEME + ":///nonexistent_path/schema.json"
    try:
      result = self.execute_query_expect_failure(
          self.client,
          "create table default.{t} stored as avro tblproperties "
          "('avro.schema.url'='{url}')".format(t=unique_name, url=fs_url))
      assert SCHEME_NOT_ALLOWED_ERROR in str(result)
    finally:
      self.client.execute(
          "drop table if exists default.{t}".format(t=unique_name))

  def test_empty_scheme_uses_default_fs(self, unique_name):
    """A URI with no scheme uses the default FS, and is always allowed.
    The path does not exist so the DDL fails, but not due to the scheme check."""
    fs_url = "/nonexistent_path/schema.json"
    try:
      result = self.execute_query_expect_failure(
          self.client,
          "create table default.{t} stored as avro tblproperties "
          "('avro.schema.url'='{url}')".format(t=unique_name, url=fs_url))
      assert SCHEME_NOT_ALLOWED_ERROR not in str(result)
      assert 'FileNotFoundException' in str(result)
    finally:
      self.client.execute(
          "drop table if exists default.{t}".format(t=unique_name))
