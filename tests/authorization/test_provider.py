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
# Client tests for SQL statement authorization

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.file_utils import assert_file_in_dir_contains
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestAuthorizationProvider(CustomClusterTestSuite):
  """
  Tests for failed authorization_provider flag.

  All test cases in this testsuite
  are expected to fail cluster startup and will swallow exceptions thrown during
  setup_method().
  """

  BAD_FLAG = "foobar"
  LOG_DIR = "invalid_provider_flag"
  MINIDUMP_PATH = "invalid_provider_flag_minidump"

  pre_test_cores = None

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      expect_cores=True,
      impala_log_dir="{" + LOG_DIR + "}",
      impalad_args="--minidump_path={" + MINIDUMP_PATH + "} "
                   "--server-name=server1 "
                   "--ranger_service_type=hive "
                   "--ranger_app_id=impala "
                   "--authorization_provider=" + BAD_FLAG,
      catalogd_args="--minidump_path={" + MINIDUMP_PATH + "} "
                    "--server-name=server1 "
                    "--ranger_service_type=hive "
                    "--ranger_app_id=impala "
                    "--authorization_provider=" + BAD_FLAG,
      tmp_dir_placeholders=[LOG_DIR, MINIDUMP_PATH])
  def test_invalid_provider_flag(self):
    # parse log file for expected exception
    assert_file_in_dir_contains(self.get_tmp_dir(self.LOG_DIR),
                                "InternalException: Could not parse "
                                "authorization_provider flag: {0}"
                                .format(self.BAD_FLAG))
