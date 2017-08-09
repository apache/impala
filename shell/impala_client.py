#!/usr/bin/env python
#
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

import sasl
import time

from beeswaxd import BeeswaxService
from beeswaxd.BeeswaxService import QueryState
from ExecStats.ttypes import TExecStats
from ImpalaService import ImpalaService
from ErrorCodes.ttypes import TErrorCode
from Status.ttypes import TStatus
from thrift.protocol import TBinaryProtocol
from thrift_sasl import TSaslClientTransport
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.Thrift import TApplicationException

class RpcStatus:
  """Convenience enum to describe Rpc return statuses"""
  OK = 0
  ERROR = 1

class RPCException(Exception):
    def __init__(self, value=""):
      self.value = value
    def __str__(self):
      return self.value

class QueryStateException(Exception):
    def __init__(self, value=""):
      self.value = value
    def __str__(self):
      return self.value

class DisconnectedException(Exception):
  def __init__(self, value=""):
      self.value = value
  def __str__(self):
      return self.value

class ImpalaClient(object):

  def __init__(self, impalad, use_kerberos=False, kerberos_service_name="impala",
               use_ssl=False, ca_cert=None, user=None, ldap_password=None,
               use_ldap=False):
    self.connected = False
    self.impalad = impalad
    self.imp_service = None
    self.transport = None
    self.use_kerberos = use_kerberos
    self.kerberos_service_name = kerberos_service_name
    self.use_ssl = use_ssl
    self.ca_cert = ca_cert
    self.user, self.ldap_password = user, ldap_password
    self.use_ldap = use_ldap
    self.default_query_options = {}
    self.query_state = QueryState._NAMES_TO_VALUES
    self.fetch_batch_size = 1024

  def _options_to_string_list(self, set_query_options):
    return ["%s=%s" % (k, v) for (k, v) in set_query_options.iteritems()]

  def build_default_query_options_dict(self):
    """The default query options are retrieved from a rpc call, and are dependent
    on the impalad to which a connection has been established. They need to be
    refreshed each time a connection is made. This is particularly helpful when
    there is a version mismatch between the shell and the impalad.
    """
    try:
      get_default_query_options = self.imp_service.get_default_configuration(False)
    except:
      return
    rpc_result = self._do_rpc(lambda: get_default_query_options)
    options, status = rpc_result
    if status != RpcStatus.OK:
      raise RPCException("Unable to retrieve default query options")
    for option in options:
      self.default_query_options[option.key.upper()] = option.value

  def build_summary_table(self, summary, idx, is_fragment_root, indent_level,
      new_indent_level, output):
    """Direct translation of Coordinator::PrintExecSummary() to recursively build a list
    of rows of summary statistics, one per exec node

    summary: the TExecSummary object that contains all the summary data

    idx: the index of the node to print

    is_fragment_root: true if the node to print is the root of a fragment (and therefore
    feeds into an exchange)

    indent_level: the number of spaces to print before writing the node's label, to give
    the appearance of a tree. The 0th child of a node has the same indent_level as its
    parent. All other children have an indent_level of one greater than their parent.

    output: the list of rows into which to append the rows produced for this node and its
    children.

    Returns the index of the next exec node in summary.exec_nodes that should be
    processed, used internally to this method only.

    NOTE: This is duplicated in impala_beeswax.py, and changes made here should also be
    made there. TODO: refactor into a shared library. (IMPALA-5792)
    """
    attrs = ["latency_ns", "cpu_time_ns", "cardinality", "memory_used"]

    # Initialise aggregate and maximum stats
    agg_stats, max_stats = TExecStats(), TExecStats()
    for attr in attrs:
      setattr(agg_stats, attr, 0)
      setattr(max_stats, attr, 0)

    node = summary.nodes[idx]
    if node.exec_stats is not None:
      for stats in node.exec_stats:
        for attr in attrs:
          val = getattr(stats, attr)
          if val is not None:
            setattr(agg_stats, attr, getattr(agg_stats, attr) + val)
            setattr(max_stats, attr, max(getattr(max_stats, attr), val))

    if node.exec_stats is not None and node.exec_stats:
      avg_time = agg_stats.latency_ns / len(node.exec_stats)
    else:
      avg_time = 0

    # If the node is a broadcast-receiving exchange node, the cardinality of rows produced
    # is the max over all instances (which should all have received the same number of
    # rows). Otherwise, the cardinality is the sum over all instances which process
    # disjoint partitions.
    if node.is_broadcast:
      cardinality = max_stats.cardinality
    else:
      cardinality = agg_stats.cardinality

    est_stats = node.estimated_stats
    label_prefix = ""
    if indent_level > 0:
      label_prefix = "|"
      label_prefix += "  |" * (indent_level - 1)
      if new_indent_level:
        label_prefix += "--"
      else:
        label_prefix += "  "

    def prettyprint(val, units, divisor):
      for unit in units:
        if val < divisor:
          if unit == units[0]:
            return "%d%s" % (val, unit)
          else:
            return "%3.2f%s" % (val, unit)
        val /= divisor

    def prettyprint_bytes(byte_val):
      return prettyprint(byte_val, [' B', ' KB', ' MB', ' GB', ' TB'], 1024.0)

    def prettyprint_units(unit_val):
      return prettyprint(unit_val, ["", "K", "M", "B"], 1000.0)

    def prettyprint_time(time_val):
      return prettyprint(time_val, ["ns", "us", "ms", "s"], 1000.0)

    hosts = 0
    if node.exec_stats is not None:
      hosts = len(node.exec_stats)
    row = [ label_prefix + node.label,
            hosts,
            prettyprint_time(avg_time),
            prettyprint_time(max_stats.latency_ns),
            prettyprint_units(cardinality),
            prettyprint_units(est_stats.cardinality),
            prettyprint_bytes(max_stats.memory_used),
            prettyprint_bytes(est_stats.memory_used),
            node.label_detail ]

    output.append(row)
    try:
      sender_idx = summary.exch_to_sender_map[idx]
      # This is an exchange node, so the sender is a fragment root, and should be printed
      # next.
      self.build_summary_table(summary, sender_idx, True, indent_level, False, output)
    except (KeyError, TypeError):
      # Fall through if idx not in map, or if exch_to_sender_map itself is not set
      pass

    idx += 1
    if node.num_children > 0:
      first_child_output = []
      idx = \
        self.build_summary_table(
            summary, idx, False, indent_level, False, first_child_output)
      for child_idx in xrange(1, node.num_children):
        # All other children are indented (we only have 0, 1 or 2 children for every exec
        # node at the moment)
        idx = self.build_summary_table(
            summary, idx, False, indent_level + 1, True, output)
      output += first_child_output
    return idx

  def test_connection(self):
    """Checks to see if the current Impala connection is still alive. If not, an exception
    will be raised."""
    if self.connected:
      self.imp_service.PingImpalaService()

  def connect(self):
    """Creates a connection to an Impalad instance

    The instance of the impala service is then pinged to
    test the connection and get back the server version
    """
    if self.transport is not None:
      self.transport.close()
      self.transport = None

    self.connected = False
    self.transport = self._get_transport()
    self.transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
    self.imp_service = ImpalaService.Client(protocol)
    result = self.ping_impala_service()
    self.connected = True
    return result

  def ping_impala_service(self):
    return self.imp_service.PingImpalaService()

  def close_connection(self):
    """Close the transport if it's still open"""
    if self.transport:
      self.transport.close()

  def _get_transport(self):
    """Create a Transport.

       A non-kerberized impalad just needs a simple buffered transport. For
       the kerberized version, a sasl transport is created.

       If SSL is enabled, a TSSLSocket underlies the transport stack; otherwise a TSocket
       is used.
    """
    if self.use_ssl:
      # TSSLSocket needs the ssl module, which may not be standard on all Operating
      # Systems. Only attempt to import TSSLSocket if the user wants an SSL connection.
      from TSSLSocketWithWildcardSAN import TSSLSocketWithWildcardSAN

    # sasl does not accept unicode strings, explicitly encode the string into ascii.
    host, port = self.impalad[0].encode('ascii', 'ignore'), int(self.impalad[1])
    if self.use_ssl:
      if self.ca_cert is None:
        # No CA cert means don't try to verify the certificate
        sock = TSSLSocketWithWildcardSAN(host, port, validate=False)
      else:
        sock = TSSLSocketWithWildcardSAN(host, port, validate=True, ca_certs=self.ca_cert)
    else:
      sock = TSocket(host, port)
    if not (self.use_ldap or self.use_kerberos):
      return TBufferedTransport(sock)
    # Initializes a sasl client
    def sasl_factory():
      sasl_client = sasl.Client()
      sasl_client.setAttr("host", host)
      if self.use_ldap:
        sasl_client.setAttr("username", self.user)
        sasl_client.setAttr("password", self.ldap_password)
      else:
        sasl_client.setAttr("service", self.kerberos_service_name)
      sasl_client.init()
      return sasl_client
    # GSSASPI is the underlying mechanism used by kerberos to authenticate.
    if self.use_kerberos:
      return TSaslClientTransport(sasl_factory, "GSSAPI", sock)
    else:
      return TSaslClientTransport(sasl_factory, "PLAIN", sock)

  def create_beeswax_query(self, query_str, set_query_options):
    """Create a beeswax query object from a query string"""
    query = BeeswaxService.Query()
    query.hadoop_user = self.user
    query.query = query_str
    query.configuration = self._options_to_string_list(set_query_options)
    return query

  def execute_query(self, query):
    rpc_result = self._do_rpc(lambda: self.imp_service.query(query))
    last_query_handle, status = rpc_result
    if status != RpcStatus.OK:
      raise RPCException("Error executing the query")
    return last_query_handle

  def wait_to_finish(self, last_query_handle, periodic_callback=None):
    loop_start = time.time()
    while True:
      query_state = self.get_query_state(last_query_handle)
      if query_state == self.query_state["FINISHED"]:
        break
      elif query_state == self.query_state["EXCEPTION"]:
        if self.connected:
          raise QueryStateException(self.get_warning_log(last_query_handle))
        else:
          raise DisconnectedException("Not connected to impalad.")

      if periodic_callback is not None: periodic_callback()
      time.sleep(self._get_sleep_interval(loop_start))

  def fetch(self, query_handle):
    """Fetch all the results.
    This function returns a generator to create an iterable of the result rows.
    """
    result_rows = []
    while True:
      rpc_result = self._do_rpc(
        lambda: self.imp_service.fetch(query_handle, False,
                                       self.fetch_batch_size))

      result, status = rpc_result

      if status != RpcStatus.OK:
        raise RPCException()

      result_rows.extend(result.data)

      if len(result_rows) >= self.fetch_batch_size or not result.has_more:
        rows = [row.split('\t') for row in result_rows]
        result_rows = []
        yield rows
        if not result.has_more:
          break

  def close_dml(self, last_query_handle):
    """Fetches the results of a DML query. Returns a tuple containing the
       number of rows modified and the number of row errors, in that order. If the DML
       operation doesn't return 'num_row_errors', then the second element in the tuple
       is None."""
    rpc_result = self._do_rpc(
        lambda: self.imp_service.CloseInsert(last_query_handle))
    insert_result, status = rpc_result

    if status != RpcStatus.OK:
      raise RPCException()

    num_rows = sum([int(k) for k in insert_result.rows_modified.values()])
    return (num_rows, insert_result.num_row_errors)

  def close_query(self, last_query_handle, query_handle_closed=False):
    """Close the query handle"""
    # Make closing a query handle idempotent
    if query_handle_closed:
      return True
    rpc_result = self._do_rpc(lambda: self.imp_service.close(last_query_handle))
    _, status = rpc_result
    return status == RpcStatus.OK

  def cancel_query(self, last_query_handle, query_handle_closed=False):
    """Cancel a query on a keyboard interrupt from the shell."""
    # Cancel sets query_state to EXCEPTION before calling cancel() in the
    # co-ordinator, so we don't need to wait.
    if query_handle_closed:
      return True
    rpc_result = self._do_rpc(lambda: self.imp_service.Cancel(last_query_handle))
    _, status = rpc_result
    return status == RpcStatus.OK

  def get_query_state(self, last_query_handle):
    rpc_result = self._do_rpc(
        lambda: self.imp_service.get_state(last_query_handle))
    state, status = rpc_result
    if status != RpcStatus.OK:
      return self.query_state["EXCEPTION"]
    return state

  def get_runtime_profile(self, last_query_handle):
    rpc_result = self._do_rpc(
        lambda: self.imp_service.GetRuntimeProfile(last_query_handle))
    profile, status = rpc_result
    if status == RpcStatus.OK and profile:
      return profile

  def get_summary(self, last_query_handle):
    """Calls GetExecSummary() for the last query handle"""
    rpc_result = self._do_rpc(
      lambda: self.imp_service.GetExecSummary(last_query_handle))
    summary, status = rpc_result
    if status == RpcStatus.OK and summary:
      return summary
    return None

  def _do_rpc(self, rpc):
    """Executes the provided callable."""

    if not self.connected:
      raise DisconnectedException("Not connected (use CONNECT to establish a connection)")
      return None, RpcStatus.ERROR
    try:
      ret = rpc()
      status = RpcStatus.OK
      # TODO: In the future more advanced error detection/handling can be done based on
      # the TStatus return value. For now, just print any error(s) that were encountered
      # and validate the result of the operation was a success.
      if ret is not None and isinstance(ret, TStatus):
        if ret.status_code != TErrorCode.OK:
          if ret.error_msgs:
            raise RPCException ('RPC Error: %s' % '\n'.join(ret.error_msgs))
          status = RpcStatus.ERROR
      return ret, status
    except BeeswaxService.QueryNotFoundException:
      raise QueryStateException('Error: Stale query handle')
    # beeswaxException prints out the entire object, printing
    # just the message is far more readable/helpful.
    except BeeswaxService.BeeswaxException, b:
        raise RPCException("ERROR: %s" % b.message)
    except TTransportException, e:
      # issue with the connection with the impalad
      raise DisconnectedException("Error communicating with impalad: %s" % e)
    except TApplicationException, t:
        raise RPCException("Application Exception : %s" % t)
    return None, RpcStatus.ERROR

  def _get_sleep_interval(self, start_time):
    """Returns a step function of time to sleep in seconds before polling
    again. Maximum sleep is 1s, minimum is 0.1s"""
    elapsed = time.time() - start_time
    if elapsed < 10.0:
      return 0.1
    elif elapsed < 60.0:
      return 0.5
    return 1.0

  def get_column_names(self, last_query_handle):
    rpc_result = self._do_rpc(
        lambda: self.imp_service.get_results_metadata(last_query_handle))
    metadata, _ = rpc_result
    if not metadata is None:
      return [fs.name for fs in metadata.schema.fieldSchemas]

  def expect_result_metadata(self, query_str):
    """ Given a query string, return True if impalad expects result metadata"""
    excluded_query_types = ['use', 'drop']
    if True in set(map(query_str.startswith, excluded_query_types)):
      return False
    return True

  def get_warning_log(self, last_query_handle):
    if last_query_handle is None:
      return "Query could not be executed"
    rpc_result = self._do_rpc(
        lambda: self.imp_service.get_log(last_query_handle.log_context))
    log, status = rpc_result
    if status != RpcStatus.OK:
      return "Failed to get error log: %s" % status
    if log and log.strip():
      return "WARNINGS: %s" % log
    return ""
