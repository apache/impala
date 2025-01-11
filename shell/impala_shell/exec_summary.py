#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

from __future__ import absolute_import, print_function, unicode_literals

from impala_thrift_gen.ExecStats.ttypes import TExecStats


def build_exec_summary_table(summary, idx, indent_level, new_indent_level, output,
                             is_prettyprint=True, separate_prefix_column=False):
  """Direct translation of Coordinator::PrintExecSummary() to recursively build a list
  of rows of summary statistics, one per exec node

  summary: the TExecSummary object that contains all the summary data

  idx: the index of the node to print

  indent_level: the number of spaces to print before writing the node's label, to give
  the appearance of a tree. The 0th child of a node has the same indent_level as its
  parent. All other children have an indent_level of one greater than their parent.

  new_indent_level: If true, this indent level is different from the previous row's.

  output: the list of rows into which to append the rows produced for this node and its
  children.

  is_prettyprint: Optional. If True, print time, units, and bytes columns in pretty
  printed format.

  separate_prefix_column: Optional. If True, the prefix and operator name will be
  returned as separate column. Otherwise, prefix and operater name will be concatenated
  into single column.

  Returns the index of the next exec node in summary.exec_nodes that should be
  processed, used internally to this method only.
  """
  if not summary.nodes:
    # Summary nodes is empty or None. Nothing to build.
    return
  assert idx < len(summary.nodes), (
      "Index ({0}) must be less than exec summary count ({1})").format(
          idx, len(summary.nodes))

  attrs = ["latency_ns", "cpu_time_ns", "cardinality", "memory_used"]

  # Initialise aggregate and maximum stats
  agg_stats, max_stats = TExecStats(), TExecStats()
  for attr in attrs:
    setattr(agg_stats, attr, 0)
    setattr(max_stats, attr, 0)

  node = summary.nodes[idx]
  instances = 0
  if node.exec_stats:
    # exec_stats is not None or an empty list.
    instances = len(node.exec_stats)
    for stats in node.exec_stats:
      for attr in attrs:
        val = getattr(stats, attr)
        if val is not None:
          setattr(agg_stats, attr, getattr(agg_stats, attr) + val)
          setattr(max_stats, attr, max(getattr(max_stats, attr), val))
    avg_time = agg_stats.latency_ns / instances
  else:
    avg_time = 0

  is_sink = node.node_id == -1
  # If the node is a broadcast-receiving exchange node, the cardinality of rows produced
  # is the max over all instances (which should all have received the same number of
  # rows). Otherwise, the cardinality is the sum over all instances which process
  # disjoint partitions.
  if is_sink:
    cardinality = -1
  elif node.is_broadcast:
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

  latency = max_stats.latency_ns
  cardinality_est = est_stats.cardinality
  memory_used = max_stats.memory_used
  memory_est = est_stats.memory_used
  if (is_prettyprint):
    avg_time = prettyprint_time(avg_time)
    latency = prettyprint_time(latency)
    cardinality = "" if is_sink else prettyprint_units(cardinality)
    cardinality_est = "" if is_sink else prettyprint_units(cardinality_est)
    memory_used = prettyprint_bytes(memory_used)
    memory_est = prettyprint_bytes(memory_est)

  row = list()
  if separate_prefix_column:
    row.append(label_prefix)
    row.append(node.label)
  else:
    row.append(label_prefix + node.label)

  row.extend([
    node.num_hosts,
    instances,
    avg_time,
    latency,
    cardinality,
    cardinality_est,
    memory_used,
    memory_est,
    node.label_detail])

  output.append(row)
  try:
    sender_idx = summary.exch_to_sender_map[idx]
    # This is an exchange node or a join node with a separate builder, so the source
    # is a fragment root, and should be printed next.
    sender_indent_level = indent_level + node.num_children
    sender_new_indent_level = node.num_children > 0
    build_exec_summary_table(summary, sender_idx, sender_indent_level,
                             sender_new_indent_level, output, is_prettyprint,
                             separate_prefix_column)
  except (KeyError, TypeError):
    # Fall through if idx not in map, or if exch_to_sender_map itself is not set
    pass

  idx += 1
  if node.num_children > 0:
    first_child_output = []
    idx = build_exec_summary_table(summary, idx, indent_level, False, first_child_output,
                                   is_prettyprint, separate_prefix_column)
    for _ in range(1, node.num_children):
      # All other children are indented
      idx = build_exec_summary_table(summary, idx, indent_level + 1, True, output,
                                     is_prettyprint, separate_prefix_column)
    output += first_child_output
  return idx
