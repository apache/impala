#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Impala's shell
import cmd
import time
import sys
from optparse import OptionParser

from ImpalaService import ImpalaService
from ImpalaService.constants import DEFAULT_QUERY_OPTIONS
from ImpalaService.ImpalaService import TImpalaQueryOptions
from beeswaxd import BeeswaxService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.protocol import TBinaryProtocol

# Simple Impala shell. Can issue queries (with configurable options)
# Basic usage: type connect <host:port> to connect to an impalad
# Then issue queries or other commands. Tab-completion should show the set of
# available commands.
# TODO: (amongst others)
#   - Insert support (implement do_insert)
#   - Use query rather than executeAndWait, and use Ctrl-C to cancel
#   - Column headers / metadata support
class ImpalaShell(cmd.Cmd):
  def __init__(self, options):
    cmd.Cmd.__init__(self)
    self.disconnected_prompt = "[Not connected] > "
    self.prompt = self.disconnected_prompt
    self.connected = False
    self.impalad = None
    self.imp_service = None
    self.transport = None    
    self.query_options = {}
    self.__make_default_options()
    if options.impalad != None:
      if self.do_connect(options.impalad) == False:
        sys.exit(1)

  def __make_default_options(self):
    def get_name(option): return TImpalaQueryOptions._VALUES_TO_NAMES[option]
    for option, default in DEFAULT_QUERY_OPTIONS.iteritems():
      self.query_options[get_name(option)] = default

  def __print_options(self):
    print '\n'.join(["\t%s: %s" % (k,v) for (k,v) in self.query_options.iteritems()])

  def __options_to_string_list(self):
    return ["%s:%s" % (k,v) for (k,v) in self.query_options.iteritems()]

  def do_options(self, args):
    self.__print_options()      
    
  def do_set(self, args):
    tokens = args.split(" ")
    if len(tokens) != 2:
      print "Error: SET <option> <value>"
      return False
    option_upper = tokens[0].upper()
    if option_upper not in ImpalaService.TImpalaQueryOptions._NAMES_TO_VALUES.keys():
      print "Unknown query option: %s" % (tokens[0],)
      available_options = \
          '\n\t'.join(ImpalaService.TImpalaQueryOptions._NAMES_TO_VALUES.keys())
      print "Available query options are: \n\t%s" % available_options
      return False
    self.query_options[option_upper] = tokens[1]
    return False

  def do_quit(self, args):
    print "Goodbye"
    return True

  def do_connect(self, args):
    tokens = args.split(" ")
    if len(tokens) != 1:
      print "CONNECT takes exactly one argument: <hostname:port> of impalad to connect to"
      return False
    try:
      host, port = tokens[0].split(":")
      self.impalad = (host, port)
    except ValueError:
      print "Connect string must be of form <hostname:port>"
      return False

    if self.__connect():
      self.prompt = "[%s:%s] > " % self.impalad

  def __connect(self):
    if self.transport is not None:
      self.transport.close()
      self.transport = None
      
    self.transport = TBufferedTransport(TSocket(self.impalad[0], int(self.impalad[1])))
    try:
      self.transport.open()
      protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
      self.imp_service = ImpalaService.Client(protocol)
      self.connected = True
    except TTransportException, e:
      print "Error connecting: %s" % (e,)
      self.connected = False
    return self.connected

  def do_select(self, args):
    """Executes a SELECT... query, fetching all rows"""
    query = BeeswaxService.Query()
    query.query = "select %s" % (args,)
    query.configuration = self.__options_to_string_list()
    print "Query: %s" % (query.query,)
    start, end = time.time(), 0
    (handle, ok) = \
        self.__do_rpc(lambda: self.imp_service.executeAndWait(query, "ImpalaCLI") )
    if not ok: return False
    print "RESULTS: "
    result_rows = []
    while True:
      results = self.imp_service.fetch(handle, False, -1)
      if not results.ready:
        print "Expected results to be ready"
        return False
      result_rows.extend(results.data)
      if not results.has_more:
        end = time.time()
        break

    print '\n'.join(result_rows)
    print "Returned %d row(s) in %2.2fs" % (len(result_rows), end - start)
    return False
    
  def __do_rpc(self, rpc):
    """Executes the RPC lambda provided with some error checking. Returns
       (rpc_result, True) if request was successful, (None, False) otherwise"""
    if not self.connected:
      print "Not connected (use CONNECT to establish a connection)"
      return (None, False)
    try:
      return (rpc(), True)
    except BeeswaxService.BeeswaxException, b:
      print "ERROR: %s" % (b.message)
    except TTransportException, e:
      print "Error communicating with impalad: %s" % (e,)
      self.connected = False
      self.prompt = self.disconnected_prompt
    return (None, False)

  def do_explain(self, args):
    query = BeeswaxService.Query()
    # Args is all text except for 'explain', so no need to strip it out
    query.query = args
    query.configuration = self.__options_to_string_list()
    print "Explain query: %s" % (query.query,)
    (explanation, ok) = self.__do_rpc(lambda: self.imp_service.explain(query))
    if ok:
      print explanation.textual
    return False

  def do_refresh(self, args):
    (_, ok) = self.__do_rpc(lambda: self.imp_service.ResetCatalog())
    if ok:
      print "Successfully refreshed catalog"
    return False

  def default(self, args):
    print "Unrecognized command"
    return False

WELCOME_STRING = """Welcome to the Impala shell. Press TAB twice to see a list of \
available commands.

Copyright (c) 2012 Cloudera, Inc. All rights reserved."""
  
if __name__ == "__main__":
  parser = OptionParser()
  parser.add_option("-i", "--impalad", dest="impalad", default=None,
                    help="<host:port> of impalad to connect to")
  (options, args) = parser.parse_args()

  shell = ImpalaShell(options)
  try:
    shell.cmdloop(WELCOME_STRING)
  except KeyboardInterrupt:
    print "Ctrl-C - exiting"
