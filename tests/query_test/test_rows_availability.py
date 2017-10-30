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

import pytest
import re
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension

class TestRowsAvailability(ImpalaTestSuite):
  """Tests that the 'Rows available' timeline event is marked only after rows are
  truly available. We mark the 'Rows available' event once we advance the query
  status to a 'ready' state; this signals to the client that rows can be fetched.
  Long fetch times can trigger client timeouts at various levels (socket, app, etc.).
  This is a regression test against IMPALA-924."""

  # These queries are chosen to have different plan roots in the coordinator's fragment.
  # The WHERE clause is carefully crafted to control when result rows become available at
  # the coordinator. The selected partition of 'functional.alltypestiny' has exactly
  # two rows stored in a single file. In the scan node we sleep one second for each
  # result row. Therefore, result rows can become available no earlier that after 2s.
  TABLE = 'functional.alltypestiny'
  WHERE_CLAUSE = 'where month = 1 and bool_col = sleep(1000)'
  QUERIES = ['select * from %s %s' % (TABLE, WHERE_CLAUSE),
             'select * from %s %s order by id limit 1' % (TABLE, WHERE_CLAUSE),
             'select * from %s %s order by id' % (TABLE, WHERE_CLAUSE),
             'select count(*) from %s %s' % (TABLE, WHERE_CLAUSE),
             'select 1 union all select count(*) from %s %s' % (TABLE, WHERE_CLAUSE),
             'select count(*) over () from %s %s' % (TABLE, WHERE_CLAUSE)]
  ROWS_AVAIL_LOWER_BOUND_MS = 2000

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRowsAvailability, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('query', *cls.QUERIES))
    cls.ImpalaTestMatrix.add_constraint(lambda v: cls.__is_valid_test_vector(v))

  @classmethod
  def __is_valid_test_vector(cls, vector):
    return vector.get_value('table_format').file_format == 'text' and\
        vector.get_value('table_format').compression_codec == 'none' and\
        vector.get_value('exec_option')['batch_size'] == 0 and\
        vector.get_value('exec_option')['disable_codegen'] == False and\
        vector.get_value('exec_option')['num_nodes'] == 0

  @pytest.mark.execute_serially
  def test_rows_availability(self, vector):
    # This test is run serially because it requires the query to come back within
    # some amount of time. Running this with other tests makes it hard to bound
    # that time.
    query = vector.get_value('query')
    # Execute async to get a handle. Wait until the query has completed.
    handle = self.execute_query_async(query, vector.get_value('exec_option'))
    self.impalad_test_service.wait_for_query_state(self.client, handle,
        self.client.QUERY_STATES['FINISHED'], timeout=20)

    profile = self.client.get_runtime_profile(handle)
    start_time_ms = None
    rows_avail_time_ms = None
    for line in profile.split("\n"):
      if "Ready to start on" in line:
        start_time_ms = self.__parse_time_ms(self.__find_time(line))
      elif "Rows available:" in line:
        rows_avail_time_ms = self.__parse_time_ms(self.__find_time(line))

    if start_time_ms is None:
      assert False, "Failed to find the 'Ready to start' timeline event in the " \
                    "query profile:\n%s" % profile
    if rows_avail_time_ms is None:
      assert False, "Failed to find the 'Rows available' timeline event in the " \
                    "query profile:\n%s" % profile
    time_diff = rows_avail_time_ms - start_time_ms
    assert time_diff >= self.ROWS_AVAIL_LOWER_BOUND_MS,\
        "The 'Rows available' timeline event was marked prematurely %sms after the "\
        "'Ready to start' event.\nExpected the event to be marked no earlier than "\
        "%sms after the 'Ready to start' event.\nQuery: %s"\
        % (time_diff, self.ROWS_AVAIL_LOWER_BOUND_MS, query)
    self.close_query(handle)

  @staticmethod
  def __find_time(line):
    """Find event time point in a line from the runtime profile timeline."""
    # Given line "- Rows available: 3s311ms (2s300ms)", this function returns "3s311ms"
    match = re.search(r': (.*) \(', line)
    if match is None:
      assert False, "Failed to find time in runtime profile"
    return match.group(1)

  @staticmethod
  def __parse_time_ms(duration):
    """Parses a duration string of the form 1h2h3m4s5.6ms into milliseconds."""
    matches = re.findall(r'([0-9]+h)?([0-9]+m)?([0-9]+s)?([0-9]+(\.[0-9]+)?ms)?',
                         duration)
    # Expect exactly two matches because all groups are optional in the regex.
    if matches is None or len(matches) != 2:
      assert False, 'Failed to parse duration string %s' % duration
    hours = 0
    minutes = 0
    seconds = 0
    milliseconds = 0
    if matches[0][0]:
      hours = int(matches[0][0][:-1])
    if matches[0][1]:
      minutes = int(matches[0][1][:-1])
    if matches[0][2]:
      seconds = int(matches[0][2][:-1])
    if matches[0][3]:
      # Truncate fractional milliseconds.
      milliseconds = int(float(matches[0][3][:-2]))
    return hours * 60 * 60 * 1000 + minutes * 60 * 1000 + seconds * 1000 + milliseconds
