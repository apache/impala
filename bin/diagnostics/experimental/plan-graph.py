#!/usr/bin/env ambari-python-wrap
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
# under the License

from __future__ import print_function
import argparse
import math
import re
import sys

from enum import Enum
from collections import defaultdict
from collections import namedtuple

# This script reads Impala plain text query profile and outputs GraphViz DOT file
# representation of the plan.
#
# Example usage:
#
#   plan-graph.py query-profile.txt > query-plan.dot
#
# If GraphViz dot program is available, we can directly create the image of the graph by
# piping the output of this script to dot, such as:
#
#   plan-graph.py --verbosity 2 query-profile.txt | dot -T jpg -o query-plan.jpg
#


class Task(Enum):
  DISCARD = 0
  PARSE_OPTIONS = 1
  PARSE_PLAN = 2
  PARSE_EXEC_SUMM = 3
  PARSE_FILTER_TABLE = 4


ParseTask = namedtuple('ParseTask', ['task', 'delimiter'])
NodePlan = namedtuple('NodePlan', ['depth', 'id', 'name'])
ExecSumm = namedtuple('ExecSumm',
    ['id', 'name', 'num_host', 'num_inst', 'avg_time', 'max_time', 'num_rows',
      'est_rows', 'peak_mem', 'est_peak_mem', 'detail'])
FilterTable = namedtuple('FilterTable',
    ['id', 'src', 'targets', 'target_types', 'is_part_filter', 'pending',
      'first_arrived', 'completed', 'enabled', 'bloom_size', 'est_fpp'])

RE_NODE = re.compile("[|\- ]*([F]?\d+:[A-Z \-]*[0-9A-Z\-]+)")
RE_RF = re.compile("[|\- ]*runtime filters: (.*)")
RE_PREDICATES = re.compile("[|\- ]*(?:hash )?predicates: (.*)")
RE_GROUPBY = re.compile("[|\- ]*group by: (.*)")
RE_BRACE = re.compile("\[([^\]]*)\]")
RE_RF_WAIT_MS = re.compile("RUNTIME_FILTER_WAIT_TIME_MS=(\d*),")
RE_RF_MAX_SIZE = re.compile("RUNTIME_FILTER_MAX_SIZE=(\d*),")
RE_RF_PLAN_SIZE = re.compile("(\d+\.\d\d [GMK]?B) \((\d+\.\d\d [GMK]?B)\)")

SECTIONS = [
    ParseTask(Task.DISCARD,
      "    Query Options (set by configuration):"),
    ParseTask(Task.PARSE_OPTIONS,
      "    Plan:"),
    ParseTask(Task.DISCARD,
      "----------------"),
    ParseTask(Task.DISCARD,
      ""),
    ParseTask(Task.PARSE_PLAN,
      "----------------"),
    ParseTask(Task.DISCARD,
      "    ExecSummary:"),
    ParseTask(Task.PARSE_EXEC_SUMM,
      "    Errors:"),
    ParseTask(Task.DISCARD,
      "    Final filter table:"),
    ParseTask(Task.PARSE_FILTER_TABLE,
      "    Per Node Peak Memory Usage:")
  ]

# Color definitions.
CL_E_RF = 'blue'
CL_E_SCAN_TO_RF = 'orange'
CL_V_BORDER = 'black'
CL_V_DEF = 'lightgrey'
CL_V_JOIN_UNION = 'yellow'
CL_V_RF_OK = 'cyan'
CL_V_RF_LATE = 'cyan3'
CL_V_RF_UNFIT = 'cyan3'
# https://www.color-hex.com/color-palette/4901
CL_V_SHADE = [CL_V_DEF, CL_V_DEF, '#f26161', '#f03939', '#ea0909']

# Time parsing.
RE_H = re.compile('([0-9\.]+)h')
RE_M = re.compile('([0-9\.]+)m')
RE_S = re.compile('([0-9\.]+)s')
RE_MS = re.compile('([0-9\.]+)ms')
RE_US = re.compile('([0-9\.]+)us')
RE_NS = re.compile('([0-9\.]+)ns')
MULTIPLIER = [10**-3, 1, 10**3, 10**6, 60 * 10**6, 60 * 60 * 10**6]
RE_TIME = [RE_NS, RE_US, RE_MS, RE_S, RE_M, RE_H]

# Other const.
DEBUG = False
MAX_ATTRIB_TOK = 5
MAX_ATTRIB_TOK_LEN = 40


def debug(s):
  if DEBUG:
    print(s)


def str_to_us(line):
  """Parse string representing duration into microsecond integer.
  For example, string '1m3s202ms' will be parsed to 63202000."""
  total_us = 0
  lit = line
  for i in range(0, len(RE_TIME)):
    m = RE_TIME[i].findall(lit)
    if m:
      num = m[0]
      total_us += float(num) * MULTIPLIER[i]
      lit = RE_TIME[i].sub('', lit)
  return total_us


def str_to_byte(line):
  """Parse byte string into integer."""
  parts = line.split(' ')
  byte = float(parts[0])
  unit = parts[1]
  if unit == 'B':
    byte *= 1
  elif unit == 'KB':
    byte *= 1024
  elif unit == 'MB':
    byte *= 1024**2
  elif unit == 'GB':
    byte *= 1024**3
  return math.trunc(byte)


class DotParser:
  """Class that parses Impala plain text query profile into GraphViz DOT format."""

  def __init__(self, verbosity=0):
    # flags
    self.verbosity = verbosity
    # plan graph
    self.edges = defaultdict(list)
    self.vertices = set()
    self.plan_stack = []
    self.plan_nodes = {}
    # runtime filters
    self.runtime_filters = {}
    self.rf_parent = {}
    self.rf_col_source = {}
    self.rf_col_dest = defaultdict(set)
    self.rf_wait_time = 10000
    self.rf_max_size = 16 * 1024**2
    self.successor_scans = defaultdict(set)
    # execution summary
    self.exec_summ_ct = 0
    self.exec_summ_map = {}
    # filter tables
    self.filter_table_ct = 0
    self.filter_table_range = []
    self.filter_table_map = {}
    # predicates
    self.predicates = {}
    # group by
    self.groupby = {}
    # graphviz dot
    self.node_fillcolor = {}
    self.node_param = {}

  def truncate(self, s, length):
    """Truncate a string if it is longer than specified length."""
    return s if len(s) < length else s[0:length] + '...'

  def tokenize(self, line, max_length=None):
    """Split a comma separated string into array of tokens."""
    start = 0
    i = 0
    par = 0
    tokens = []
    while i < len(line):
      if i == len(line) - 1 and start < i:
        tok = line[start:i + 1]
        if max_length:
          tok = self.truncate(tok, max_length)
        tokens.append(tok)
      if line[i] == ',' and par == 0 and start < i:
        tok = line[start:i]
        if max_length:
          tok = self.truncate(tok, max_length)
        tokens.append(tok)
        start = i
        if (i + 1) < len(line) and line[i + 1] == ' ':
          i = i + 2
          start = i
      else:
        if line[i] == '(':
          par += 1
        elif line[i] == ')':
          par -= 1
        i += 1
    return tokens

  def is_undersize(self, line):
    """Return True if bloom filter size is smaller than the ideal size."""
    m = RE_RF_PLAN_SIZE.search(line)
    if m:
      planned = str_to_byte(m.group(1))
      ideal = str_to_byte(m.group(2))
      return planned < ideal
    else:
      return False

  def get_depth(self, line):
    """Get the depth of a plan node relative to the left margin of text plan."""
    i = 0
    while len(line) > i and line[i] in ('|', '-', ' '):
      i += 1
    return i

  def pop_out_stack(self, depth):
    """Pop out an element from plan_stack."""
    while len(self.plan_stack) > 0 and self.plan_stack[-1].depth > depth:
      self.plan_stack.pop()

  def push_to_stack(self, depth, node):
    """Push an element into plan_stack."""
    self.pop_out_stack(depth)
    self.plan_stack.append(node)

  def find_preceding_scans(self, rf_id):
    """Given a runtime filter id, find all scan nodes that contribute towards that runtime
    filter creation."""
    ret = []
    parent = self.rf_parent[rf_id]
    consumer_scans = set(self.edges[rf_id])
    for child in self.edges[parent]:
      if child == rf_id:
        continue
      if not consumer_scans.intersection(self.successor_scans[child]):
        ret.extend(self.successor_scans[child])
    return ret

  def add_edge(self, ori, dest):
    """Add an edge from ori to dest."""
    self.edges[ori].append(dest)
    self.vertices.add(ori)
    self.vertices.add(dest)

  def parse_node(self, line, match):
    """Parse a line that represents query plan node."""
    i = self.get_depth(line)
    tok = match.split(':')
    node = NodePlan(i, tok[0], tok[1])
    self.push_to_stack(i, node)
    child = self.plan_stack[-1]
    self.plan_nodes[child.id] = child
    if len(self.plan_stack) > 1:
      # this is not a root node
      par = self.plan_stack[-2]
      self.add_edge(par.id, child.id)
      if 'SCAN' in child.name:
        for n in self.plan_stack:
          self.successor_scans[n.id].add(child.id)
    m = RE_BRACE.search(line)
    if m:
      self.node_param[child.id] = m.group(1)

  def parse_rf(self, filter_str):
    """Parse a line that represents runtime filter production/consumption."""
    filters = self.tokenize(filter_str.replace('&lt;', '<').replace('&gt;', '>'))
    v_id = self.plan_stack[-1].id
    debug(self.plan_stack[-1].name)
    debug('\n'.join(filters))
    debug('')

    for fil in filters:
      parts = fil.split(' <- ') if '<-' in fil else fil.split(' -> ')
      rf_name = parts[0]
      rf_id = rf_name[:5]
      col = parts[-1].strip() if '(' in parts[-1] else parts[-1].strip().split('.')[-1]
      self.runtime_filters[rf_id] = rf_name
      if "<" in fil:
        self.add_edge(v_id, rf_id)
        self.rf_col_source[rf_id] = col
        self.rf_parent[rf_id] = v_id
      else:
        self.add_edge(rf_id, v_id)
        self.rf_col_dest[rf_id].add(col)

  def parse_predicates(self, predicates_str):
    """Parse a line that represent predicates."""
    v_id = self.plan_stack[-1].id
    self.predicates[v_id] = predicates_str

  def parse_groupby(self, groupby_str):
    """Parse a line that represent 'group by' clause."""
    v_id = self.plan_stack[-1].id
    self.groupby[v_id] = groupby_str

  def format_plan_node_param(self, node_name, param):
    """Format a plan node parameter."""
    formatted = param
    if 'SCAN' in node_name:
      col = param.split(' ')[0]
      col_name = col.split('.')[-1].replace(',', '')
      formatted = col_name
    elif param.startswith('HASH(') and param.endswith(')'):
      formatted = self.truncate(param[5:len(param) - 1], MAX_ATTRIB_TOK_LEN)
      formatted = 'HASH(' + formatted + ')'
    return formatted

  def dict_to_dot_attrib(self, d):
    """Merge dictionary to a string of DOT vertex/edge attribute."""
    if not d:
      return ''
    attrib_str = ', '.join(['{}="{}"'.format(k, v) for k, v in sorted(d.items())])
    return ' [' + attrib_str + ']'

  def draw_vertices(self, node_alias):
    """Draw the DOT vertices."""
    for v_id in sorted(self.vertices):
      dot_name = node_alias[v_id]
      dot_label = ''
      attrib = {}
      attrib['color'] = CL_V_BORDER
      attrib['fillcolor'] = CL_V_DEF

      if v_id in self.runtime_filters:
        # this is a runtime filter.
        dot_label += self.runtime_filters[v_id]
        attrib['fillcolor'] = CL_V_RF_OK
        ft = self.filter_table_map[v_id]

        if ft and self.verbosity > 0:
          # mark late RF
          if 'REMOTE' in ft.target_types:
            arrival_time = str_to_us(ft.completed) / 10.0**3
            if arrival_time > self.rf_wait_time:
              attrib['fillcolor'] = CL_V_RF_LATE
          # mark undersize RF
          if self.is_undersize(ft.bloom_size):
            attrib['fillcolor'] = CL_V_RF_UNFIT

          for dest in self.rf_col_dest[v_id]:
            dot_label += '\\n{} \=\> {}'.format(self.rf_col_source[v_id], dest)

          if self.verbosity > 1:
            dot_label += '\\n' + (
                'LOCAL' if 'REMOTE' not in ft.target_types else ft.completed)
            if ft.est_fpp:
              dot_label += ', fpp ' + ft.est_fpp
            dot_label += ', part' if ft.is_part_filter == 'true' else ''
            dot_label += '\\n' + ft.bloom_size

      elif v_id in self.exec_summ_map:
        # This is either fragment or plan node.
        node = self.plan_nodes[v_id]
        es = self.exec_summ_map[v_id]
        if es:
          dot_label += es.id + ":" + es.name
        else:
          dot_label += node.id + ":" + node.name

        if v_id in self.node_fillcolor:
          attrib['fillcolor'] = self.node_fillcolor[v_id]

        if (attrib['fillcolor'] == CL_V_DEF
              and ('JOIN' in node.name or 'UNION' in node.name)):
          attrib['fillcolor'] = CL_V_JOIN_UNION

        if (self.verbosity > 1 or (self.verbosity > 0 and 'SCAN' in node.name)):
          if v_id in self.node_param:
            dot_label += '\\n' + self.format_plan_node_param(node.name,
                self.node_param[v_id])

          if v_id in self.predicates:
            pred = self.predicates[v_id]
            tok = self.tokenize(pred, MAX_ATTRIB_TOK_LEN)
            if len(tok) > MAX_ATTRIB_TOK:
              tok = tok[:MAX_ATTRIB_TOK] + ['...']
            dot_label += '\\n[[' + ';\\n'.join(tok) + ']]'

          if v_id in self.groupby:
            group = self.truncate(self.groupby[v_id], MAX_ATTRIB_TOK_LEN)
            dot_label += '\\ngroupby(' + group + ')'

        if es:
          dot_label += '\\n' + es.avg_time
          if es.num_rows != "-":
            attrib['xlabel'] = es.num_rows
          if self.verbosity > 1:
            dot_label += ', ' + es.num_inst + " inst"

      attrib['label'] = dot_label
      print('{}{};'.format(dot_name, self.dict_to_dot_attrib(attrib)))

  def draw_edges(self, node_alias):
    """Draw the DOT edges."""
    for k, v in sorted(self.edges.items(), key=lambda e: e[0]):
      for d in sorted(v):
        attrib = {}
        ori = node_alias[k]
        dest = node_alias[d]
        if k.startswith('RF') or d.startswith('RF'):
          attrib['color'] = CL_E_RF
        else:
          # Other edges that does not touch RF node should be drawn backward.
          attrib['dir'] = 'back'
        print('{} -> {}{};'.format(ori, dest, self.dict_to_dot_attrib(attrib)))

  def draw_dependency_edges(self, node_alias):
    """Draw dependency edges from scanner vertices to runtime filter vertices.
    An edge from scanner to runtime filter drawn here means the scanner contributes
    towards creation of the runtime filter and the scanner should have finished before
    the runtime filter is published."""
    scan_to_filter = defaultdict(set)
    for rf_id in sorted(self.runtime_filters.keys()):
      for scan in self.find_preceding_scans(rf_id):
        if scan not in self.edges[rf_id]:
          scan_to_filter[scan].add(rf_id)
    for scan, v in sorted(scan_to_filter.items(), key=lambda e: e[0]):
      for rf_id in sorted(v):
        ori = node_alias[scan]
        dest = node_alias[rf_id]
        attrib = {}
        if self.verbosity > 2:
          attrib['color'] = CL_E_SCAN_TO_RF
        else:
          attrib['style'] = 'invis'
        print('{} -> {}{};'.format(ori, dest, self.dict_to_dot_attrib(attrib)))

  def draw(self):
    """Draw the DOT format."""
    node_alias = {}
    for v_id in sorted(self.vertices):
      if v_id in self.runtime_filters:
        node_alias[v_id] = v_id
      else:
        alias = 'N' + v_id if v_id.isnumeric() else v_id
        node_alias[v_id] = alias

    print('digraph G {')
    print('node [style=filled];')
    print('rankdir = TB;')
    self.draw_vertices(node_alias)
    print('\n')
    self.draw_edges(node_alias)
    if self.verbosity > 1:
      print('\n')
      self.draw_dependency_edges(node_alias)
    print('}')

  def parse_options(self, line):
    """Parse query option section.
    This section begins with 'Query Options (set by configuration):' line in query
    profile."""
    debug('option')
    debug(line)
    m = RE_RF_WAIT_MS.search(line)
    if m:
      self.rf_wait_time = int(m.group(1))
      debug(self.rf_wait_time)

    m = RE_RF_MAX_SIZE.search(line)
    if m:
      self.rf_max_size = int(m.group(1))
      debug(self.rf_max_size)

  def parse_plan(self, line):
    """Parse query plan sections.
    This section begins with 'Plan:' line in query profile."""
    m = RE_NODE.match(line)
    if m:
      self.parse_node(line, m.group(1))
      return
    if self.verbosity > 0:
      m = RE_RF.match(line)
      if m:
        self.parse_rf(m.group(1))
        return
    if self.verbosity > 1:
      m = RE_PREDICATES.match(line)
      if m:
        self.parse_predicates(m.group(1))
        return
      m = RE_GROUPBY.match(line)
      if m:
        self.parse_groupby(m.group(1))
        return
    if line.strip().endswith('|'):
      self.pop_out_stack(len(line.strip()))

  def parse_exec_summ(self, line):
    """Parse execution summary section.
    This section begins with 'ExecSummary:' line in query profile."""
    self.exec_summ_ct += 1
    if self.exec_summ_ct <= 2:
      return

    parts = list(
        map(lambda x: x.strip(),
          filter(None,
            line.strip().replace("|", " ").replace("--", "  ").split("  "))))

    if len(parts) == 7:
      parts.insert(5, "-")
      parts.insert(6, "-")
    assert len(parts) >= 9
    if len(parts) == 9:
      parts.append("-")
    assert len(parts) == 10

    # split id and name from parts[0].
    tok = parts[0].split(':')
    parts[0] = tok[1]
    parts.insert(0, tok[0])
    assert len(parts) == 11

    es = ExecSumm._make(parts)
    debug(es)
    self.exec_summ_map[es.id] = es

  def parse_filter_table(self, line):
    """Parse the filter table section.
    This section begins with 'Final filter table:' line in query profile."""
    self.filter_table_ct += 1
    if self.filter_table_ct == 1:
      # Tokenize the table header to get the column boundaries
      headers = list(
          map(lambda x: x.strip(),
            filter(None, line.strip().split("  "))))
      begin = 0
      for head in headers:
        end = line.find(head) + len(head)
        self.filter_table_range.append((begin, end))
        begin = end
      assert len(self.filter_table_range) >= 11

    if self.filter_table_ct <= 2:
      return

    parts = []
    i = 0
    for begin, end in self.filter_table_range:
      i += 1
      tok = line[begin:end].strip()
      if (i == 1):
        # test to parse the first column into int. If fails, it means we arrived at the
        # end of final filter table.
        if not tok.isnumeric():
          return
      parts.append(line[begin:end].strip())
    parts[0] = 'RF{:0>3}'.format(parts[0])
    ft = FilterTable._make(parts[0:11])
    debug(ft)
    self.filter_table_map[ft.id] = ft

  def time_shade(self):
    """Color Impala plan nodes with red shades according to its execution time.
    Slowest plan in the query will be colored dark red."""
    op_time = {}
    max_time = 0
    for op, es in self.exec_summ_map.items():
      time = str_to_us(es.avg_time)
      op_time[op] = time
      max_time = time if time > max_time else max_time
    max_time += 1
    for op, time in op_time.items():
      group = math.trunc((float(time) / max_time) * len(CL_V_SHADE))
      debug(group)
      if group > 1:
        self.node_fillcolor[op] = CL_V_SHADE[group]

  def parse(self, inf):
    """Walk through impala query plan and parse each section accordingly.
    A section ends when delimiter of that section is found."""
    i = 0
    for line in inf:
      if i > len(SECTIONS) - 1:
        break
      if (line.rstrip().startswith(SECTIONS[i].delimiter) or
            (not SECTIONS[i].delimiter and not line.rstrip())):
        debug('Found delimiter ' + SECTIONS[i].delimiter + ' in ' + line)
        i += 1
        continue
      if SECTIONS[i].task == Task.DISCARD:
        continue
      elif SECTIONS[i].task == Task.PARSE_OPTIONS:
        self.parse_options(line)
      elif SECTIONS[i].task == Task.PARSE_PLAN:
        self.parse_plan(line)
      elif SECTIONS[i].task == Task.PARSE_EXEC_SUMM:
        self.parse_exec_summ(line)
      elif SECTIONS[i].task == Task.PARSE_FILTER_TABLE:
        self.parse_filter_table(line)
    self.time_shade()
    self.draw()


def main():
  parser = argparse.ArgumentParser(
      description="This script reads Impala plain text query profile and outputs the \
          query plan in GraphViz DOT format.")
  parser.add_argument("infile", nargs="?", type=argparse.FileType('r'),
      default=sys.stdin, help="Impala query profile in plain text format. stdin will \
          be read if argument is not supplied.")
  parser.add_argument("--verbosity", type=int, default=2, choices=range(0, 4),
      help='Verbosity level of produced graph. Default to 2.')
  args = parser.parse_args()

  dp = DotParser(args.verbosity)
  dp.parse(args.infile)


if __name__ == "__main__":
  main()
