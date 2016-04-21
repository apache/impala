# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Injects failures  at specific locations in each of the plan nodes. Currently supports
# two types of failures - cancellation of the query and a failure test hook.
#
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_exec_option_dimension
from time import sleep

# Test injecting error logs in prepare phase and status::OK(). This tests one of race
# conditions in error reporting (IMPALA-3385).
class TestErrorLogs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestErrorLogs, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))

  def test_errorlog(self, vector):
    query = 'select count(*) from tpch.lineitem;'
    action = 'INJECT_ERROR_LOG'
    location = 'PREPARE'
    # scan node has id=0
    node_id = 0
    # Without restarting impala, reporting thread reports every 5 seconds.
    # Having a lower scan range can make the query run more than 2*5 seconds
    # Select location in prepare to guarantee at least one cleared error maps will
    # be sent to coordinator.
    debug_action = '%d:%s:%s' % (node_id, location, action)
    vector.get_value('exec_option')['debug_action'] = debug_action
    vector.get_value('exec_option')['MAX_SCAN_RANGE_LENGTH'] = '1000'
    self.__execute_inject_error_log_action(query, vector)

  def __execute_inject_error_log_action(self, query, vector):
    try:
      handle = self.execute_query_async(query, vector.get_value('exec_option'))
      # sleep() is used to terminate the query; otherwise it runs for a long time. It is
      # large enough to further guarantee at least one cleared error maps to be sent to
      # coordinator.
      sleep(30)
      cancel_result = self.client.cancel(handle)
      self.client.close_query(handle)
      assert cancel_result.status_code == 0,\
          'Unexpected status code from cancel request: %s' % cancel_result
    # As long as impala did not crash we are good.
    except ImpalaBeeswaxException:
      return
