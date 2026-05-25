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
from subprocess import PIPE, Popen, check_call, check_output

from impala_py_lib import profiles
from tests.common.environ import impalad_basedir
from tests.common.base_test_suite import BaseTestSuite

IMPALA_HOME = os.environ['IMPALA_HOME']
SUMMARY_NUM_COLUMNS = 20


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

  def test_webui_thrift_profile_summary_output(self):
    self._compare_webui_thrift_profile_output(['--profile_format=summary'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'))
    self._compare_webui_thrift_profile_output(['--profile_format=summary'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'))

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

  def test_summary_output(self):
    self._compare_profile_tool_output(['--profile_format=summary'],
        get_profile_path('impala_profile_log_tpcds_compute_stats'),
        get_profile_path('impala_profile_log_tpcds_compute_stats.expected.summary'))
    self._assert_summary_tsv_shape(self._run_profile_tool(['--profile_format=summary'],
        get_profile_path('impala_profile_log_tpcds_compute_stats')))
    self._compare_profile_tool_output(['--profile_format=summary'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'),
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2.expected.summary'))
    self._assert_summary_tsv_shape(self._run_profile_tool(['--profile_format=summary'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2')))

  def test_summary_output_with_query_id_filter(self):
    query_id = '564ae7b03a77a9cc:521cf35d00000000'
    output = self._run_profile_tool(
        ['--profile_format=summary', '--query_id=%s' % query_id],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'))

    lines = self._summary_tsv_lines(output)
    assert len(lines) == 2
    assert lines[1].split('\t')[0] == query_id

  def test_summary_output_nonzero_bytes_spilled(self):
    for profile_log in ['impala_profile_log_tpcds_compute_stats',
                        'impala_profile_log_tpcds_compute_stats_v2']:
      with tempfile.NamedTemporaryFile() as input_log:
        self._write_profile_with_scratch_bytes_written(
            get_profile_path(profile_log), 64 * 1024, input_log)
        output = self._run_profile_tool(['--profile_format=summary'], input_log.name)

      lines = self._summary_tsv_lines(output)
      assert len(lines) == 2
      fields = lines[1].split('\t')
      assert fields[14] == '64.00 KB'

  def test_summary_output_failed_query(self):
    output = self._run_profile_tool(
        ['--profile_format=summary', '--summary_text_length=60'],
        get_profile_path('impala_profile_log_failed_query'))

    lines = self._summary_tsv_lines(output)
    assert len(lines) == 2
    fields = lines[1].split('\t')
    assert fields[5] == 'N/A'
    assert fields[6] == '350.000ms'
    assert fields[10] == 'N/A'
    assert fields[11] == 'N/A'
    assert fields[15] == 'EXCEPTION'
    assert fields[16] == (
        'Memory limit exceeded: could not allocate 64.00 MB without e...')

  def test_summary_output_cancelled_query(self):
    output = self._run_profile_tool(
        ['--profile_format=summary', '--summary_text_length=60'],
        get_profile_path('impala_profile_log_cancelled_query'))

    lines = self._summary_tsv_lines(output)
    assert len(lines) == 2
    fields = lines[1].split('\t')
    assert fields[5] == 'N/A'
    assert fields[6] == '350.000ms'
    assert fields[10] == 'N/A'
    assert fields[11] == 'N/A'
    assert fields[15] == 'CANCELLED'
    assert fields[16] == (
        'Cancelled by user: query cancellation requested before all f...')

  def test_summary_text_length_unlimited(self):
    output = self._run_profile_tool(
        ['--profile_format=summary', '--summary_text_length=0'],
        get_profile_path('impala_profile_log_tpcds_compute_stats_v2'))

    lines = self._summary_tsv_lines(output)
    assert len(lines) == 5
    fields = lines[3].split('\t')
    long_stmt = fields[-1]
    assert len(long_stmt) > 250
    assert not long_stmt.endswith('...')

  def _summary_tsv_lines(self, output):
    if not isinstance(output, str):
      output = output.decode('utf-8')
    self._assert_summary_tsv_shape(output)
    return output.splitlines()

  def _assert_summary_tsv_shape(self, output):
    if not isinstance(output, str):
      output = output.decode('utf-8')
    for line in output.splitlines():
      assert len(line.split('\t')) == SUMMARY_NUM_COLUMNS

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

  def _write_profile_with_scratch_bytes_written(
      self, input_log, scratch_bytes_written, output):
    with open(input_log, 'r') as f:
      for line in f:
        timestamp, query_id, encoded_profile = line.split(None, 2)
        profile_tree = profiles.decode_profile_archive(encoded_profile.rstrip())
        if self._set_first_per_node_counter(
            profile_tree, 'ScratchBytesWritten', scratch_bytes_written):
          encoded_profile = profiles.encode_profile_archive(profile_tree)
          output.write(('%s %s %s\n' % (
              timestamp, query_id, encoded_profile)).encode('utf-8'))
          output.flush()
          return
    assert False, 'No Per Node Profiles ScratchBytesWritten counter found'

  def _set_first_per_node_counter(self, profile_tree, counter_name, value):
    nodes = profile_tree.nodes
    for idx, node in enumerate(nodes):
      if not node.name.startswith('Execution Profile '):
        continue

      child_idx = idx + 1
      for _ in range(node.num_children):
        if child_idx >= len(nodes):
          break
        next_child_idx = self._next_sibling_index(nodes, child_idx)
        if nodes[child_idx].name == 'Per Node Profiles':
          host_idx = child_idx + 1
          for _ in range(nodes[child_idx].num_children):
            if host_idx >= len(nodes):
              break
            for counter in nodes[host_idx].counters:
              if counter.name == counter_name:
                counter.value = value
                return True
            host_idx = self._next_sibling_index(nodes, host_idx)
        child_idx = next_child_idx
    return False

  def _next_sibling_index(self, nodes, node_idx):
    next_idx = node_idx
    nodes_remaining = 1
    while nodes_remaining > 0 and next_idx < len(nodes):
      nodes_remaining += nodes[next_idx].num_children
      next_idx += 1
      nodes_remaining -= 1
    return next_idx

  def _run_profile_tool(self, args, input_log, output=None):
    with open(input_log, 'r') as f:
      command = [os.path.join(IMPALA_HOME, "bin/run-binary.sh"),
                 os.path.join(impalad_basedir, 'util/impala-profile-tool')] + args
      if output is None:
        return check_output(command, stdin=f)
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
