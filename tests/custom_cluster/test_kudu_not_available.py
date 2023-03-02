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
from impala.dbapi import connect

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestKuduNotAvailable(CustomClusterTestSuite):
  """Check that when Impala is started without Kudu support, statements that use Kudu
     fail with the expected error message.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
      impalad_args="--disable_kudu=true",
      catalogd_args="--disable_kudu=true",
      statestored_args="--disable_kudu=true")
  def test_kudu_not_available(self):
    conn = connect()
    cursor = conn.cursor()
    try:
      cursor.execute("SHOW DATABASES")
      if "functional_kudu" not in cursor:
        # This should mean Kudu data couldn't be loaded because Kudu isn't supported on
        # the OS.
        return
      cursor.execute("USE functional_kudu")

      # CREATE TABLE succeeds, the execution is in the frontend only. See IMPALA-3233
      self.assert_failure("SELECT * FROM tinytable", cursor)
      self.assert_failure("INSERT INTO tinytable VALUES ('a', 'b')", cursor)
      self.assert_failure("DELETE FROM tinytable", cursor)
    finally:
      cursor.close()
      conn.close()

  def assert_failure(self, stmt, cursor):
    try:
      cursor.execute(stmt)
    except Exception as e:
      error_msg = str(e)
      assert "Kudu features are disabled" in error_msg \
          or "Kudu is not supported" in error_msg
      return
    assert False, "Statement should have failed: %s" % stmt
