# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

import pytest
import re
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import TestDimension

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
    cls.TestMatrix.add_dimension(TestDimension('query', *cls.QUERIES))
    cls.TestMatrix.add_constraint(lambda v: cls.__is_valid_test_vector(v))

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

    # Parse the query profile for the 'Rows available' timeline event.
    rows_avail_line = self.__get_rows_available_event(handle)
    # Match the parenthesized delta duration between the 'Rows available' event
    # and the previous event.
    matches = re.search(r'\(.*\)', rows_avail_line)
    if matches is None:
      assert False, "Failed to find parenthesized delta time in %s" % rows_avail_line
    # Strip out parenthesis.
    rows_avail_ms_str = matches.group(0)[1:-1]
    rows_avail_ms = self.__parse_duration_ms(rows_avail_ms_str)
    assert rows_avail_ms >= self.ROWS_AVAIL_LOWER_BOUND_MS,\
        "The 'Rows available' timeline event was marked prematurely %sms after the "\
        "previous timeline event.\nExpected the event to be marked no earlier than "\
        "%sms after the previous event.\nQuery: %s"\
        % (rows_avail_ms, self.ROWS_AVAIL_LOWER_BOUND_MS, query)
    self.close_query(handle)

  def __get_rows_available_event(self, query_handle):
    profile = self.client.get_runtime_profile(query_handle)
    for line in profile.split("\n"):
      if "Rows available:" in line: return line
    assert False, "Failed to find the 'Rows available' timeline event in the "\
        "query profile:\n%s" % profile

  def __parse_duration_ms(self, duration):
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
