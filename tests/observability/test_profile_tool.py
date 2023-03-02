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
import os.path
import tempfile
from subprocess import check_call

from tests.common.environ import impalad_basedir
from tests.common.base_test_suite import BaseTestSuite

IMPALA_HOME = os.environ['IMPALA_HOME']


def get_profile_path(filename):
  return os.path.join(IMPALA_HOME, 'testdata/impala-profiles/', filename)


class TestProfileTool(BaseTestSuite):

  def test_text_output(self):
    # Test text profiles with different verbosity levels.
    self._compare_profile_tool_output([],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        get_profile_path('impala_profile_log_tpcds_compute_stats.expected.txt'))
    self._compare_profile_tool_output(['--profile_verbosity=default'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        get_profile_path('impala_profile_log_tpcds_compute_stats_default.expected.txt'))
    self._compare_profile_tool_output(['--profile_verbosity=extended'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        get_profile_path('impala_profile_log_tpcds_compute_stats_extended.expected.txt'))

  def test_text_output_profile_v2(self):
    # Test text profiles with different verbosity levels.
    self._compare_profile_tool_output(['--profile_verbosity=default'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'),
        get_profile_path(
            'impala_profile_log_tpcds_compute_stats_v2_default.expected.txt'))
    self._compare_profile_tool_output(['--profile_verbosity=extended'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'),
        get_profile_path(
            'impala_profile_log_tpcds_compute_stats_v2_extended.expected.txt'))

  def test_json_output(self):
    # Test JSON profiles with different verbosity levels.
    self._compare_profile_tool_output(['--profile_format=json'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        get_profile_path('impala_profile_log_tpcds_compute_stats.expected.json'))
    self._compare_profile_tool_output(['--profile_format=prettyjson'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        get_profile_path('impala_profile_log_tpcds_compute_stats.expected.pretty.json'))
    self._compare_profile_tool_output(['--profile_format=prettyjson',
            '--profile_verbosity=extended'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        get_profile_path(
        'impala_profile_log_tpcds_compute_stats.expected.pretty_extended.json'))

  def _compare_profile_tool_output(self, args, input_log, expected_output):
    """Run impala-profile-tool on input_log and compare it to the contents of the
    file at 'expected_output'."""
    with tempfile.NamedTemporaryFile() as tmp:
      with open(input_log, 'r') as f:
        check_call([os.path.join(IMPALA_HOME, "bin/run-binary.sh"),
                    os.path.join(impalad_basedir, 'util/impala-profile-tool')] + args,
                    stdin=f, stdout=tmp)
        check_call(['diff', expected_output, tmp.name])
