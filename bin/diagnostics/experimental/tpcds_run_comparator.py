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
import csv
import math
import re
import statistics
import os

from enum import Enum
from collections import namedtuple

# This script compares 2 runs of TPCDS by parsing their respective Impala plain text
# query profiles. It currently outputs the peak memory comparision of both runs where:
# 1. It compares average per-node peak memory and geo-mean per-node peak memory.
# 2. It compares max peak memory reduction among Hash operators.
#
# It can be extended to other comparisions in future.
#
# Example usage:
#
#   tpcds_run_comparator.py <path to base run profile> <path to new run profile>
#                           [path to result csv file]
#


class Task(Enum):
  DISCARD = 0
  PARSE_EXEC_SUMM = 3
  PARSE_PEAK_MEMORY = 4


ParseTask = namedtuple('ParseTask', ['task', 'delimiter'])
ExecSumm = namedtuple('ExecSumm',
    ['id', 'name', 'num_host', 'num_inst', 'avg_time', 'max_time', 'num_rows',
      'est_rows', 'peak_mem', 'est_peak_mem', 'detail'])

RE_PEAK_MEM = re.compile("\d+\.\d\d [GMK]?B")

# Defines list of sections in a profile file.
# Format: (Task Enum, Section Delimeter)
# Section Delimeter is the first line of next section in the list.
SECTIONS = [
    ParseTask(Task.DISCARD,
      "    ExecSummary:"),
    ParseTask(Task.PARSE_EXEC_SUMM,
      "    Errors:"),
    ParseTask(Task.DISCARD,
      "    Per Node Peak Memory Usage:"),
    ParseTask(Task.PARSE_PEAK_MEMORY,
      "    Per Node Bytes Read:")
  ]


# Other const.
DEBUG = False


def debug(s):
  if DEBUG:
    print(s)


def geo_mean(bytes):
  """Compute geo mean"""
  sum_log = sum([math.log(x) for x in bytes])
  return math.exp(float(sum_log) / len(bytes))


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
  else:
    raise ValueError("Invalid unit for byte string: {}".format(byte))
  return math.trunc(byte)


class ProfileParser:
  """Class that parses Impala plain text query profile"""

  def __init__(self):
    # execution summary
    self.exec_summ_ct = 0
    self.exec_summ_map = {}
    # peak memory
    self.peak_mem_ct = 0
    self.peak_mem = []
    # result
    self.ht_mem_res = []
    self.op_mem_res = []

  def parse_peak_mem(self, line):
    """Parses the peak memory"""
    debug('Parsing Peak memory')
    if self.peak_mem_ct > 1:
      return
    self.peak_mem_ct += 1
    mems = RE_PEAK_MEM.findall(line)
    self.peak_mem = [str_to_byte(i) for i in mems]
    debug('Peak memories: ' + ",".join([str(i) for i in self.peak_mem]))

  def parse_exec_summ(self, line):
    """Parse execution summary section.
    This section begins with 'ExecSummary:' line in query profile."""
    self.exec_summ_ct += 1
    if self.exec_summ_ct <= 3:
      return

    parts = list(
        map(lambda x: x.strip(),
          filter(None,
            line.strip().replace("|", " ").replace("--", "  ").split("  "))))

    # 3 columns in ExecSummary are optional and may be empty for few operators.
    # These are #Rows, Est. #Rows and Detail.
    # Normalize all the entries to 10 parts below.
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

  def parse(self, inf):
    """Walk through impala query plan and parse each section accordingly.
    A section ends when delimiter of that section is found."""
    i = 0
    for line in inf:
      if (line.rstrip().startswith(SECTIONS[i].delimiter) or
            (not SECTIONS[i].delimiter and not line.rstrip())):
        debug('Found delimiter ' + SECTIONS[i].delimiter + ' in ' + line)
        i += 1
      if i == len(SECTIONS):
        break
      if SECTIONS[i].task == Task.DISCARD:
        continue
      elif SECTIONS[i].task == Task.PARSE_EXEC_SUMM:
        self.parse_exec_summ(line)
      elif SECTIONS[i].task == Task.PARSE_PEAK_MEMORY:
        self.parse_peak_mem(line)

  def filter_exec_join(self, op):
    filter_set = {'HASH JOIN', 'JOIN BUILD', 'AGGREGATE'}
    return op.name in filter_set

  def compare_hash_table_mem(self, pp, path, ht_mem_res):
    join_list1 = filter(self.filter_exec_join, self.exec_summ_map.values())
    result = [os.path.basename(path), '0.0', '0.0', '0.0']
    max_hash_percent = 0.0
    for j in join_list1:
      bytes1 = str_to_byte(j.peak_mem)
      bytes2 = str_to_byte(pp.exec_summ_map[j.id].peak_mem)
      # Checks the percent reduction for operators greater than 10 MB.
      if bytes1 > 10 * 1024 * 1024 and bytes2 != bytes1:
        percent = float(bytes1 - bytes2) / bytes1 * 100
        if abs(percent) > max_hash_percent:
          result = []
          result = [os.path.basename(path), str(round(bytes1 / float(1024 * 1024))),
            str(round(bytes2 / float(1024 * 1024))),
            str(round(percent, 1))]
          max_hash_percent = percent
    ht_mem_res.append(result)

  def compare_peak_mem(self, pp, path, op_mem_res):
    avg1 = statistics.mean(self.peak_mem)
    debug('average1=' + str(avg1))
    avg2 = statistics.mean(pp.peak_mem)
    debug('average2=' + str(avg2))
    geo_mean1 = geo_mean(self.peak_mem)
    debug('geomean1=' + str(geo_mean1))
    geo_mean2 = geo_mean(pp.peak_mem)
    debug('geomean2=' + str(geo_mean2))
    max1 = max(self.peak_mem)
    max2 = max(pp.peak_mem)
    reduction_avg = float(avg1 - avg2) / avg1 * 100
    reduction_geomean = float(geo_mean1 - geo_mean2) / geo_mean1 * 100
    reduction_max = float(max1 - max2) / max1 * 100
    res = [avg1 / (1024 * 1024), avg2 / (1024 * 1024), reduction_avg,
      geo_mean1 / (1024 * 1024), geo_mean2 / (1024 * 1024), reduction_geomean,
      reduction_max]
    fres = [str(round(i, 1)) for i in res]
    fres.insert(0, os.path.basename(path))
    op_mem_res.append(fres)
    debug(",".join(fres))


def print_results(ht_mem_res, op_mem_res):
  print("Peak Memory Comparision")
  print("-----------------------")
  print("")
  print("1. Maximum Reduction in Per-operator Peak Memory")
  print("""TPCDS query profile, base peak memory, new peak memory""")
  print("")
  for ht in ht_mem_res:
    print(",".join(ht))
  print("")
  print("2. Reduction in Per-Node Peak Memory")
  print("""TPCDS query profile, base avg (MB), new avg (MB), avg reduction %,
    base geomean (MB), new geomean (MB), geomean reduction % """)
  for mem in op_mem_res:
    print(",".join(mem))


def results_to_csv(ht_mem_res, op_mem_res, csv_path):
  with open(csv_path, 'w') as csv_f:
    writer = csv.writer(csv_f)
    header1 = ['TPCDS query profile', 'base peak memory', 'new peak memory']
    writer.writerow(header1)
    writer.writerows(ht_mem_res)
    header2 = ['TPCDS query profile', 'base avg (MB)', 'new avg (MB)',
      'avg reduction %', 'base geomean (MB)', 'new geomean (MB)', 'geomean reduction %']
    writer.writerow(header2)
    writer.writerows(op_mem_res)


def is_dir(path):
  if os.path.isdir(path):
    return path
  else:
    raise argparse.ArgumentTypeError("{} is not a valid path".format(path))


def main():
  parser = argparse.ArgumentParser(
      description="""This script reads Impala plain text query profiles of two TPCDS
        runs and compare peak memories""")
  parser.add_argument("basedir", type=is_dir,
      help='Impala baseline query profiles directory.')
  parser.add_argument("newdir", type=is_dir,
      help='Impala new query profiles directory to be compared with baseline.')
  parser.add_argument("result", nargs='?', default=None,
      help="""Specify output CSV file path to be used to write results,
        else it would be printed out to stdout""")
  args = parser.parse_args()
  op_mem_res = []
  ht_mem_res = []
  basefiles = [f for f in os.listdir(args.basedir)
    if os.path.isfile(os.path.join(args.basedir, f))]
  for filename in basefiles:
    path1 = os.path.join(args.basedir, filename)
    path2 = os.path.join(args.newdir, filename)
    if os.path.isfile(path1) and os.path.isfile(path2):
      debug("{} is being parsed ".format(path1))
      with open(path1, "r") as f1:
        with open(path2, "r") as f2:
          pp1 = ProfileParser()
          pp1.parse(f1)
          pp2 = ProfileParser()
          pp2.parse(f2)
          pp1.compare_hash_table_mem(pp2, path1, ht_mem_res)
          pp1.compare_peak_mem(pp2, path1, op_mem_res)
  if args.result is None:
    print_results(ht_mem_res, op_mem_res)
  else:
    results_to_csv(ht_mem_res, op_mem_res, args.result)


if __name__ == "__main__":
  main()
