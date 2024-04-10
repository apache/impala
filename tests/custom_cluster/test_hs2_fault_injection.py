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
from builtins import round
import pytest
import requests

from shell.ImpalaHttpClient import ImpalaHttpClient
from shell.impala_client import ImpalaHS2Client
from shell.shell_exceptions import HttpError
from tests.common.impala_test_suite import IMPALAD_HS2_HTTP_HOST_PORT
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from time import sleep


class FaultInjectingHttpClient(ImpalaHttpClient, object):
  """Class for injecting faults in the ImpalaHttpClient. Faults are injected by using the
  'enable_fault' method. The 'flush' method is overridden to check for injected faults
  and raise exceptions, if needed."""
  def __init__(self, *args, **kwargs):
    super(FaultInjectingHttpClient, self).__init__(*args, **kwargs)
    self.fault_code = None
    self.fault_message = None
    self.fault_enabled = False
    self.num_requests = 0
    self.fault_frequency = 0
    self.fault_enabled = False

  def enable_fault(self, http_code, http_message, fault_frequency, fault_body=None,
                   fault_headers=None):
    """Inject fault with given code and message at the given frequency.
    As an example, if frequency is 20% then inject fault for 1 out of every 5
    requests."""
    if fault_headers is None:
      fault_headers = {}
    self.fault_enabled = True
    self.fault_code = http_code
    self.fault_message = http_message
    self.fault_frequency = fault_frequency
    assert fault_frequency > 0 and fault_frequency <= 1
    self.num_requests = 0
    self.fault_body = fault_body
    self.fault_headers = fault_headers

  def disable_fault(self):
    self.fault_enabled = False

  def _check_code(self):
    if self.code >= 300:
      # Report any http response code that is not 1XX (informational response) or
      # 2XX (successful).
      raise HttpError(self.code, self.message, self.body, self.headers)

  def _inject_fault(self):
    if not self.fault_enabled:
      return False
    if self.fault_frequency == 1:
      return True
    if round(self.num_requests % (1 / self.fault_frequency)) == 1:
      return True
    return False

  def flush(self):
    ImpalaHttpClient.flush(self)
    self.num_requests += 1
    # Override code and message with the injected fault
    if self.fault_code is not None and self._inject_fault():
      self.code = self.fault_code
      self.message = self.fault_message
      self.body = self.fault_body
      self.headers = self.fault_headers
      self._check_code()


class FaultInjectingImpalaHS2Client(ImpalaHS2Client):
  """Fault injecting ImpalaHS2Client class using FaultInjectingHttpClient as the
  transport"""
  def __init__(self, *args, **kwargs):
    """Creates a transport with HTTP as the base."""
    super(FaultInjectingImpalaHS2Client, self).__init__(*args, **kwargs)
    assert self.use_http_base_transport
    host_and_port = "{0}:{1}".format(self.impalad_host, self.impalad_port)
    assert self.http_path
    transport = FaultInjectingHttpClient("http://{0}/{1}".
        format(host_and_port, self.http_path))
    transport.open()
    self.transport = transport

  def _get_http_transport(self, connect_timeout_ms):
    self.transport.open()
    return self.transport

  def ping(self):
    return self._ping_impala_service()


class TestHS2FaultInjection(CustomClusterTestSuite):
  """Class for testing the http fault injection in various rpcs used by the
  impala-shell client"""
  def setup(self):
    impalad = IMPALAD_HS2_HTTP_HOST_PORT.split(":")
    self.custom_hs2_http_client = FaultInjectingImpalaHS2Client(impalad, 1024,
        kerberos_host_fqdn=None, use_http_base_transport=True, http_path='cliservice')
    self.transport = self.custom_hs2_http_client.transport

  def teardown(self):
    self.transport.disable_fault()
    self.custom_hs2_http_client.close_connection()

  def connect(self):
    self.custom_hs2_http_client.connect()
    assert self.custom_hs2_http_client.connected

  def close_query(self, query_handle):
    self.transport.disable_fault()
    self.custom_hs2_http_client.close_query(query_handle)

  def __expect_msg_retry(self, impala_rpc_name):
    """Returns expected log message for rpcs which can be retried"""
    return ("Caught exception HTTP code 502: Injected Fault, "
      "type=<class 'shell.shell_exceptions.HttpError'> in {0}. "
      "Num remaining tries: 3".format(impala_rpc_name))

  def __expect_msg_retry_with_extra(self, impala_rpc_name):
    """Returns expected log message for rpcs which can be retried and where the http
    message has a message body"""
    return ("Caught exception HTTP code 503: Injected Fault [EXTRA], "
      "type=<class 'shell.shell_exceptions.HttpError'> in {0}. "
      "Num remaining tries: 3".format(impala_rpc_name))

  def __expect_msg_retry_with_retry_after(self, impala_rpc_name):
    """Returns expected log message for rpcs which can be retried and the http
    message has a body and a Retry-After header that can be correctly decoded"""
    return ("Caught exception HTTP code 503: Injected Fault [EXTRA], "
      "type=<class 'shell.shell_exceptions.HttpError'> in {0}. "
      "Num remaining tries: 3, retry after 1 secs".format(impala_rpc_name))

  def __expect_msg_retry_with_retry_after_no_extra(self, impala_rpc_name):
    """Returns expected log message for rpcs which can be retried and the http
    message has a Retry-After header that can be correctly decoded"""
    return ("Caught exception HTTP code 503: Injected Fault, "
      "type=<class 'shell.shell_exceptions.HttpError'> in {0}. "
      "Num remaining tries: 3, retry after 1 secs".format(impala_rpc_name))

  def __expect_msg_no_retry(self, impala_rpc_name):
    """Returns expected log message for rpcs which can not be retried"""
    return ("Caught exception HTTP code 502: Injected Fault, "
      "type=<class 'shell.shell_exceptions.HttpError'> in {0}. ".format(impala_rpc_name))

  @pytest.mark.execute_serially
  def test_connect(self, capsys):
    """Tests fault injection in ImpalaHS2Client's connect().
    OpenSession and CloseImpalaOperation rpcs fail.
    Retries results in a successful connection."""
    self.transport.enable_fault(502, "Injected Fault", 0.20)
    self.connect()
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry("OpenSession")
    assert output[2] == self.__expect_msg_retry("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_connect_proxy(self, capsys):
    """Tests fault injection in ImpalaHS2Client's connect().
    The injected error has a message body.
    OpenSession and CloseImpalaOperation rpcs fail.
    Retries results in a successful connection."""
    self.transport.enable_fault(503, "Injected Fault", 0.20, 'EXTRA')
    self.connect()
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry_with_extra("OpenSession")
    assert output[2] == self.__expect_msg_retry_with_extra("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_connect_proxy_no_retry(self, capsys):
    """Tests fault injection in ImpalaHS2Client's connect().
    The injected error contains headers but no Retry-After header.
    OpenSession and CloseImpalaOperation rpcs fail.
    Retries results in a successful connection."""
    self.transport.enable_fault(503, "Injected Fault", 0.20, 'EXTRA',
                                {"header1": "value1"})
    self.connect()
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry_with_extra("OpenSession")
    assert output[2] == self.__expect_msg_retry_with_extra("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_connect_proxy_bad_retry(self, capsys):
    """Tests fault injection in ImpalaHS2Client's connect().
    The injected error contains a body and a junk Retry-After header.
    OpenSession and CloseImpalaOperation rpcs fail.
    Retries results in a successful connection."""
    self.transport.enable_fault(503, "Injected Fault", 0.20, 'EXTRA',
                                {"header1": "value1",
                                 "Retry-After": "junk"})
    self.connect()
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry_with_extra("OpenSession")
    assert output[2] == self.__expect_msg_retry_with_extra("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_connect_proxy_retry(self, capsys):
    """Tests fault injection in ImpalaHS2Client's connect().
    The injected error contains a body and a Retry-After header that can be decoded.
    Retries results in a successful connection."""
    self.transport.enable_fault(503, "Injected Fault", 0.20, 'EXTRA',
                                {"header1": "value1",
                                  "Retry-After": "1"})
    self.connect()
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry_with_retry_after("OpenSession")
    assert output[2] == self.__expect_msg_retry_with_retry_after("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_connect_proxy_retry_no_body(self, capsys):
    """Tests fault injection in ImpalaHS2Client's connect().
    The injected error has no body but does have a Retry-After header that can be decoded.
    Retries results in a successful connection."""
    self.transport.enable_fault(503, "Injected Fault", 0.20, None,
                                {"header1": "value1",
                                  "Retry-After": "1"})
    self.connect()
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry_with_retry_after_no_extra("OpenSession")
    assert output[2] == self.\
      __expect_msg_retry_with_retry_after_no_extra("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_close_connection(self, capsys):
    """Tests fault injection in ImpalaHS2Client's close_connection().
    CloseSession rpc fails due to the fault, but succeeds anyways since exceptions
    are ignored."""
    self.connect()
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    self.custom_hs2_http_client.close_connection()
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_no_retry("CloseSession")

  @pytest.mark.execute_serially
  def test_ping(self, capsys):
    """Tests fault injection in ImpalaHS2Client's _ping_impala_service().
    PingImpalaHS2Service rpc fails due to the fault, but suceeds after retry"""
    self.connect()
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    version, webserver_address = None, None
    version, webserver_address = self.custom_hs2_http_client.ping()
    assert version is not None
    assert webserver_address is not None
    page = requests.get('{0}/{1}'.format(webserver_address, 'healthz'))
    assert page.status_code == requests.codes.ok
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry("PingImpalaHS2Service")

  @pytest.mark.execute_serially
  def test_execute_query(self, capsys):
    """Tests fault injection in ImpalaHS2Client's execute_query().
    ExecuteStatement rpc fails and results in error since retries are not supported."""
    self.connect()
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    query_handle = None
    try:
      query_handle = self.custom_hs2_http_client.execute_query('select 1', {})
    except Exception as e:
      assert str(e) == 'HTTP code 502: Injected Fault'
    assert query_handle is None
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_no_retry("ExecuteStatement")

  @pytest.mark.execute_serially
  def test_fetch(self, capsys):
    """Tests fault injection in ImpalaHS2Client's fetch().
    FetchResults rpc fails and results in error since retries are not supported."""
    self.connect()
    query_handle = self.custom_hs2_http_client.execute_query('select 1', {})
    rows_fetched = None
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    num_rows = None
    rows_fetched = self.custom_hs2_http_client.fetch(query_handle)
    try:
      for rows in rows_fetched:
        num_rows += 1
    except Exception as e:
      assert str(e) == 'HTTP code 502: Injected Fault'
    assert num_rows is None
    self.close_query(query_handle)
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_no_retry("FetchResults")

  @pytest.mark.execute_serially
  def test_close_dml(self, unique_database, capsys):
    """Tests fault injection in ImpalaHS2Client's close_dml().
    CloseImpalaOperation rpc fails and results in error since retries are not
    supported."""
    table = '{0}.{1}'.format(unique_database, 'tbl')
    self.client.execute('create table {0} (c1 int, c2 int)'.format(table))
    self.connect()
    dml = 'insert into {0} values (1,1)'.format(table)
    query_handle = self.custom_hs2_http_client.execute_query(dml, {})
    self.custom_hs2_http_client.wait_to_finish(query_handle)
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    exception = None
    try:
      self.custom_hs2_http_client.close_dml(query_handle)
    except Exception as e:
      exception = e
    assert exception is not None
    assert str(exception) == 'HTTP code 502: Injected Fault'
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_no_retry("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_close_query(self, capsys):
    """Tests fault injection in ImpalaHS2Client's close_query().
    CloseImpalaOperation rpc fails due to fault, but succeeds after a retry"""
    self.connect()
    query_handle = self.custom_hs2_http_client.execute_query('select 1', {})
    self.custom_hs2_http_client.wait_to_finish(query_handle)
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    self.custom_hs2_http_client.close_query(query_handle)
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry("CloseImpalaOperation")

  @pytest.mark.execute_serially
  def test_cancel_query(self, capsys):
    """Tests fault injection in ImpalaHS2Client's cancel_query().
    CancelOperation rpc fails due to fault, but succeeds after a retry"""
    self.connect()
    query_handle = self.custom_hs2_http_client.execute_query('select sleep(50000)', {})
    sleep(5)
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    success = self.custom_hs2_http_client.cancel_query(query_handle)
    assert success
    self.close_query(query_handle)
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry("CancelOperation")

  @pytest.mark.execute_serially
  def test_get_query_state(self, capsys):
    """Tests fault injection in ImpalaHS2Client's get_query_state().
    GetOperationStatus rpc fails due to fault, but succeeds after a retry"""
    self.connect()
    query_handle = self.custom_hs2_http_client.execute_query('select 1', {})
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    self.custom_hs2_http_client.wait_to_finish(query_handle)
    query_state = self.custom_hs2_http_client.get_query_state(query_handle)
    assert query_state == self.custom_hs2_http_client.FINISHED_STATE
    self.close_query(query_handle)
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry("GetOperationStatus")

  @pytest.mark.execute_serially
  def test_get_runtime_profile_summary(self, capsys):
    """Tests fault injection in ImpalaHS2Client's get_runtime_profile().
    GetRuntimeProfile and GetExecSummary rpc fails due to fault, but succeeds
    after a retry"""
    self.connect()
    query_handle = self.custom_hs2_http_client.execute_query('select 1', {})
    self.custom_hs2_http_client.wait_to_finish(query_handle)
    self.custom_hs2_http_client.close_query(query_handle)
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    profile = self.custom_hs2_http_client.get_runtime_profile(query_handle)
    assert profile is not None
    summary = self.custom_hs2_http_client.get_summary(query_handle)
    assert summary is not None
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry("GetRuntimeProfile")
    assert output[2] == self.__expect_msg_retry("GetExecSummary")

  @pytest.mark.execute_serially
  def test_get_warning_log(self, capsys):
    """Tests fault injection in ImpalaHS2Client's get_warning_log().
    GetLog rpc fails due to fault, but succeeds after a retry"""
    self.connect()
    sql = ("select * from functional.alltypes a inner join /* +foo */ "
        "functional.alltypes b on a.id=b.id")
    query_handle = self.custom_hs2_http_client.execute_query(sql, {})
    self.custom_hs2_http_client.wait_to_finish(query_handle)
    self.transport.enable_fault(502, "Injected Fault", 0.50)
    warning_log = self.custom_hs2_http_client.get_warning_log(query_handle)
    assert warning_log == 'WARNINGS: JOIN hint not recognized: foo'
    self.close_query(query_handle)
    output = capsys.readouterr()[1].splitlines()
    assert output[1] == self.__expect_msg_retry("GetLog")

  @pytest.mark.execute_serially
  def test_connection_drop(self):
    """Tests connection drop between rpcs. Each rpc starts a new connection so dropping
    connections between rpcs should have no effect"""
    self.connect()
    self.transport.close()
    query_handle = self.custom_hs2_http_client.execute_query('select 1', {})
    self.transport.close()
    self.custom_hs2_http_client.wait_to_finish(query_handle)
    self.transport.close()
    query_state = self.custom_hs2_http_client.get_query_state(query_handle)
    assert query_state == self.custom_hs2_http_client.FINISHED_STATE
    num_rows = 0
    self.transport.close()
    rows_fetched = self.custom_hs2_http_client.fetch(query_handle)
    for rows in rows_fetched:
      num_rows += len(rows)
    assert num_rows == 1
    self.transport.close()
    self.close_query(query_handle)
    self.transport.close()
    profile = self.custom_hs2_http_client.get_runtime_profile(query_handle)
    assert profile is not None
    self.transport.close()
    summary = self.custom_hs2_http_client.get_summary(query_handle)
    assert summary is not None
