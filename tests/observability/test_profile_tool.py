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

import os.path
import tempfile
from subprocess import PIPE, Popen, check_call

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
        'impala_profile_log_tpcds_compute_stats_extended.expected.pretty.json'))

  def test_json_output_profile_v2(self):
    # Test JSON profiles with different verbosity levels.
    self._compare_profile_tool_output(['--profile_format=json'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'),
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2.expected.json'))
    self._compare_profile_tool_output(['--profile_format=prettyjson',
            '--profile_verbosity=extended'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'),
        get_profile_path(
            'impala_profile_log_tpcds_compute_stats_v2_extended.expected.pretty.json'))

  def test_legacy_profile_text_output(self):
    legacy_profile_path = 'legacy-profiles/'
    # Test text profiles with different verbosity levels.
    self._compare_profile_tool_output([],
        get_profile_path(legacy_profile_path + 'impala_profile_log_tpcds_compute_stats'),
        get_profile_path(legacy_profile_path
            + 'impala_profile_log_tpcds_compute_stats.expected.txt'))
    self._compare_profile_tool_output(['--profile_verbosity=default'],
        get_profile_path(legacy_profile_path + 'impala_profile_log_tpcds_compute_stats'),
        get_profile_path(legacy_profile_path
            + 'impala_profile_log_tpcds_compute_stats_default.expected.txt'))
    self._compare_profile_tool_output(['--profile_verbosity=extended'],
        get_profile_path(legacy_profile_path + 'impala_profile_log_tpcds_compute_stats'),
        get_profile_path(legacy_profile_path
            + 'impala_profile_log_tpcds_compute_stats_extended.expected.txt'))

  def test_legacy_profile_json_output(self):
    legacy_profile_path = 'legacy-profiles/'
    # Test JSON profiles with different verbosity levels.
    self._compare_profile_tool_output(['--profile_format=json'],
        get_profile_path(legacy_profile_path + 'impala_profile_log_tpcds_compute_stats'),
        get_profile_path(legacy_profile_path
            + 'impala_profile_log_tpcds_compute_stats.expected.json'))
    self._compare_profile_tool_output(['--profile_format=prettyjson'],
        get_profile_path(legacy_profile_path
            + 'impala_profile_log_tpcds_compute_stats'),
        get_profile_path(legacy_profile_path
            + 'impala_profile_log_tpcds_compute_stats.expected.pretty.json'))
    self._compare_profile_tool_output(['--profile_format=prettyjson',
            '--profile_verbosity=extended'],
        get_profile_path(legacy_profile_path + 'impala_profile_log_tpcds_compute_stats'),
        get_profile_path(legacy_profile_path
            + 'impala_profile_log_tpcds_compute_stats_extended.expected.pretty.json'))

  def test_webui_thrift_profile_text_output(self):
    # WebUI thrift profile downloads contain only the archived profile string, without
    # the timestamp and query id prefix found in profile log lines.
    self._compare_webui_thrift_profile_output([],
        get_profile_path('impala_profile_log_tpcds_compute_stats'))
    self._compare_webui_thrift_profile_output([],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'))

  def test_webui_thrift_profile_prettyjson_output(self):
    self._compare_webui_thrift_profile_output(['--profile_format=prettyjson'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'))

  def test_webui_thrift_profile_ignores_surrounding_whitespace(self):
    self._compare_webui_thrift_profile_output([],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        profile_prefix=' \t', profile_suffix=' \r')

  def test_timestamp_filter(self):
    profile_log = get_profile_path('impala_profile_log_tpcds_compute_stats')
    with open(profile_log, 'r') as f:
      fields = f.readline().split(None, 2)
    assert len(fields) == 3

    self._compare_profile_tool_output(['--min_timestamp=%s' % fields[0]], profile_log,
        get_profile_path('impala_profile_log_tpcds_compute_stats.expected.txt'))

    with tempfile.NamedTemporaryFile() as tmp:
      self._run_profile_tool(['--max_timestamp=%d' % (int(fields[0]) - 1)],
          profile_log, tmp)
      assert os.path.getsize(tmp.name) == 0

  def test_oversized_timestamp_without_timestamp_filter(self):
    with open(get_profile_path('impala_profile_log_tpcds_compute_stats'), 'r') as f:
      profile_log_line = f.readline()
    fields = profile_log_line.split(None, 2)
    assert len(fields) == 3

    with tempfile.NamedTemporaryFile(mode='w+') as valid_input, \
        tempfile.NamedTemporaryFile(mode='w+') as oversized_timestamp_input, \
        tempfile.NamedTemporaryFile() as valid_output, \
        tempfile.NamedTemporaryFile() as oversized_timestamp_output:
      valid_input.write(profile_log_line)
      valid_input.flush()
      oversized_timestamp_input.write(
          '174830243931748302879977 %s %s' % (fields[1], fields[2]))
      oversized_timestamp_input.flush()

      self._run_profile_tool([], valid_input.name, valid_output)
      self._run_profile_tool([], oversized_timestamp_input.name,
          oversized_timestamp_output)
      check_call(['diff', valid_output.name, oversized_timestamp_output.name])

  def test_timestamp_filter_invalid_timestamp_error(self):
    query_id, encoded_profile = self._get_first_profile_log_entry(
        get_profile_path('impala_profile_log_tpcds_compute_stats'))
    with tempfile.NamedTemporaryFile(mode='w+') as invalid_timestamp_input:
      invalid_timestamp_input.write(
          'not-a-timestamp %s %s\n' % (query_id, encoded_profile))
      invalid_timestamp_input.flush()

      stdout, stderr = self._run_profile_tool_error(
          ['--min_timestamp=0'], invalid_timestamp_input.name)

    assert stdout == ''
    assert "Error parsing profile log timestamp prefix on line 1: " \
        "'not-a-timestamp'" in stderr
    assert 'Expected Unix epoch milliseconds' in stderr
    assert 'timestamp prefixes are parsed only when' in stderr
    assert '--min_timestamp/--max_timestamp filtering is enabled' in stderr

  def _compare_profile_tool_output(self, args, input_log, expected_output):
    """Run impala-profile-tool on input_log and compare it to the contents of the
    file at 'expected_output'."""
    with tempfile.NamedTemporaryFile() as tmp:
      self._run_profile_tool(args, input_log, tmp)
      check_call(['diff', expected_output, tmp.name])

  def _compare_webui_thrift_profile_output(
      self, args, input_log, profile_prefix='', profile_suffix=''):
    """Compare a bare WebUI thrift profile to the same profile in a profile log."""
    query_id, encoded_profile = self._get_first_profile_log_entry(input_log)
    with tempfile.NamedTemporaryFile() as thrift_profile:
      with tempfile.NamedTemporaryFile() as expected_output:
        with tempfile.NamedTemporaryFile() as actual_output:
          profile_input = profile_prefix + encoded_profile + profile_suffix + '\n'
          thrift_profile.write(profile_input.encode('utf-8'))
          thrift_profile.flush()
          self._run_profile_tool(
              args + ['--query_id=%s' % query_id], input_log, expected_output)
          self._run_profile_tool(args, thrift_profile.name, actual_output)
          check_call(['diff', expected_output.name, actual_output.name])

  def _get_first_profile_log_entry(self, input_log):
    with open(input_log, 'r') as f:
      _timestamp, query_id, encoded_profile = f.readline().split(None, 2)
    return query_id, encoded_profile.rstrip()

  def _run_profile_tool(self, args, input_log, output=None):
    with open(input_log, 'r') as f:
      command = [os.path.join(IMPALA_HOME, "bin/run-binary.sh"),
                 os.path.join(impalad_basedir, 'util/impala-profile-tool')] + args
      check_call(command, stdin=f, stdout=output)
      output.flush()

  def _run_profile_tool_error(self, args, input_log):
    with open(input_log, 'r') as f:
      command = [os.path.join(IMPALA_HOME, "bin/run-binary.sh"),
                 os.path.join(impalad_basedir, 'util/impala-profile-tool')] + args
      process = Popen(command, stdin=f, stdout=PIPE, stderr=PIPE)
      stdout, stderr = process.communicate()
      assert process.returncode != 0
      return stdout.decode('utf-8'), stderr.decode('utf-8')
