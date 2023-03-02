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
from __future__ import absolute_import, division, print_function
import os
import pytest
import stat
import tempfile

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf
from tests.util.filesystem_utils import WAREHOUSE

tmp = tempfile.NamedTemporaryFile(delete=False)
BAD_KEY_FILE = tmp.name

@SkipIf.not_s3
class TestS3AAccess(CustomClusterTestSuite):

  cmd_filename = ""
  @classmethod
  def setup_class(cls):
    super(TestS3AAccess, cls).setup_class()
    try:
      tmp.write('echo badkey')
    finally:
      tmp.close()
      # Make this file executable
      tmp_file_stat = os.stat(BAD_KEY_FILE)
      os.chmod(BAD_KEY_FILE, tmp_file_stat.st_mode | stat.S_IEXEC)

  @classmethod
  def teardown_class(cls):
    os.remove(BAD_KEY_FILE)

  def _get_impala_client(self):
    impalad = self.cluster.get_any_impalad()
    return impalad.service.create_beeswax_client()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    "-s3a_access_key_cmd=\"%s\"\
     -s3a_secret_key_cmd=\"%s\"" % (BAD_KEY_FILE, BAD_KEY_FILE))
  def test_ddl_keys_ignored(self, unique_database):
    '''DDL statements will ignore the S3 keys passed to Impala because the code path
    that it exercises goes through the Hive Metastore which should have the correct keys
    from the core-site configuration.'''
    client = self._get_impala_client()
    # This is repeated in the test below (because it's necessary there), but we still
    # want to make sure that it is tested separately as it's a good test practice.
    self.execute_query_expect_success(client,
        "create external table if not exists {0}.tinytable_s3 like functional.tinytable \
         location '{1}/tinytable'".format(unique_database, WAREHOUSE))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    "-s3a_access_key_cmd=\"%s\"\
     -s3a_secret_key_cmd=\"%s\"" % (BAD_KEY_FILE, BAD_KEY_FILE))
  def test_keys_do_not_work(self, unique_database):
    '''Test that using incorrect S3 access and secret keys will not allow Impala to
    query S3.
    TODO: We don't have the test infrastructure in place yet to check if the keys do work
    in a custom cluster test. (See IMPALA-3422)'''
    client = self._get_impala_client()
    self.execute_query_expect_success(client,
        "create external table if not exists {0}.tinytable_s3 like functional.tinytable \
         location '{1}/tinytable'".format(unique_database, WAREHOUSE))
    self.execute_query_expect_failure(client, "select * from {0}.tinytable_s3"
        .format(unique_database))
