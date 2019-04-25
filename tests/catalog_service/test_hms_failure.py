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
# Tests to validate the Catalog Service continues to function even if the HMS fails.

import pytest
import os
from subprocess import check_call

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import IS_ISILON, IS_LOCAL

class TestHiveMetaStoreFailure(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHiveMetaStoreFailure, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet' and\
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def setup_class(cls):
    super(TestHiveMetaStoreFailure, cls).setup_class()

  @classmethod
  def run_hive_server(cls):
    script = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/run-hive-server.sh')
    run_cmd = [script]
    if IS_LOCAL or IS_ISILON:
      run_cmd.append('-only_metastore')
    check_call(run_cmd, close_fds=True)

  @classmethod
  def teardown_class(cls):
    # Make sure the metastore is running even if the test aborts somewhere unexpected
    # before restarting the metastore itself.
    cls.run_hive_server()
    super(TestHiveMetaStoreFailure, cls).teardown_class()

  @pytest.mark.execute_serially
  def test_hms_service_dies(self, vector):
    """Regression test for IMPALA-823 to verify the catalog service works properly when
    HMS connections fail"""
    # Force the tables to be uncached and then kill the hive metastore.
    tbl_name = "functional.alltypes"
    self.client.execute("invalidate metadata %s" % tbl_name)
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd], close_fds=True)

    try:
      self.client.execute("describe %s" % tbl_name)
      pytest.xfail("It is possible (although unlikely) that all tables completed " +
          "loading between the time 'invalidate metadata' was called and the HMS was " +
          "killed. TODO: Make the test case more deterministic to ensure this never " +
          "happens.")
    except ImpalaBeeswaxException as e:
      print str(e)
      assert "Failed to load metadata for table: %s. Running 'invalidate metadata %s' "\
          "may resolve this problem." % (tbl_name, tbl_name) in str(e)
    self.run_hive_server()

    self.client.execute("invalidate metadata %s" % tbl_name)
    self.client.execute("describe %s" % tbl_name)
