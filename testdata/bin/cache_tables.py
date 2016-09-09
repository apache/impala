#!/usr/bin/env impala-python
##############################################################################
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
##############################################################################
#
# This script will warm up the buffer cache with the tables required to run the input
# query.  This only works on a mini-dfs cluster.  This is remarkably difficult to do
# since hdfs which tries to hide the details of the block locations from users.
# The only way to do this is to
#   1. use the java APIs (deprecated, of course) to extract the block ids.
#   2. find the files with those block ids on the file system and read them
#
# First run testdata/bin/generate-block-ids.sh.  This will output the block locations
# to testdata/block-ids.  This file is good as long as the mini-dfs cluster does not
# get new files.  If the block-ids file is not there, this script will run
# generate-block-ids.sh.
#
# Run this script, passing it the query and it will go read every replica of every
# block of every table in the query.
import math
import os
import re
import sys
import subprocess
import tempfile
from optparse import OptionParser

# Options
parser = OptionParser()
parser.add_option("-q", "--query", dest="query", default = "",
                  help="Query to run.  If none specified, runs all queries.")

(options, args) = parser.parse_args()

block_ids_file = 'testdata/block-ids'
data_node_root = os.environ['MINI_DFS_BASE_DATA_DIR'] + '/dfs/data'
block_ids = {}

# Parse the block ids file to all the block ids for all the tables
# the format of the file is:
# <table name>: <block_id1> <block_id2> <etc>
def parse_block_ids():
  full_path = os.environ['IMPALA_HOME'] + "/" + block_ids_file;
  if not os.path.isfile(full_path):
    cmd = os.environ['IMPALA_HOME'] + '/testdata/bin/generate-block-ids.sh'
    os.system(cmd)

  if not os.path.isfile(full_path):
    raise Exception("Could not find/generate block id files: " + full_path)

  f = open(full_path);
  for line in f:
    tokens = line.split(':')
    blocks = tokens[1].strip().split(' ')
    block_ids[tokens[0].strip()] = blocks
  
# Parse for the tables used in this query
def parse_tables(query):
  table_predecessor = ['from', 'join']
  tokens = query.split(' ')
  tables = []
  next_is_table = False
  for t in tokens:
    t = t.lower()
    if next_is_table:
      tables.append(t)
      next_is_table = False
    if t in table_predecessor:
      next_is_table = True
  return tables

# Warm the buffer cache by cat-ing all the blocks to /dev/null
def warm_buffer_cache(table):
  if table not in block_ids:
    raise Exception("Table not found: " + table)

  blocks = block_ids[table]
  for block in blocks:
    cmd = 'find %s -type f -name blk_%s* -exec cat {} > /dev/null \;' % \
          (data_node_root, block)
    os.system(cmd)

tables = parse_tables(options.query)
parse_block_ids()

if len(tables) == 0:
  raise Exception("Could not parse tables in: " + options.query)

for table in tables:
  warm_buffer_cache(table)
