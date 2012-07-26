#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# This script generates the "CREATE TABLE", "INSERT", and "LOAD" statements for loading
# test data and writes them to create-*-generated.sql and
# load-*-generated.sql.
#
# The statements that are generated are based on an input test vector
# (read from a file) that describes the coverage desired. For example, currently
# we want to run benchmarks with different data sets, across different file types, and
# with different compression algorithms set. To improve data loading performance this
# script will generate an INSERT INTO statement to generate the data if the file does
# not already exist in HDFS. If the file does already exist in HDFS then we simply issue a
# LOAD statement which is much faster.
#
# The input test vectors are generated via the generate_test_vectors.py so
# ensure that script has been run (or the test vector files already exist) before
# running this script.
#
# Note: This statement generation is assuming the following data loading workflow:
# 1) Load all the data in the specified source table
# 2) Create tables for the new file formats and compression types
# 3) Run INSERT OVERWRITE TABLE SELECT * from the source table into the new tables
#    or LOAD directly if the file already exists in HDFS.
import collections
import csv
import math
import os
import random
import subprocess
import sys
import tempfile
from itertools import product
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--exploration_strategy", dest="exploration_strategy", default="core",
                  help="The exploration strategy for schema gen: 'core', "\
                  "'pairwise', or 'exhaustive'")
parser.add_option("--hive_warehouse_dir", dest="hive_warehouse_dir",
                  default="/test-warehouse",
                  help="The HDFS path to the base Hive test warehouse directory")
parser.add_option("--workload", dest="workload", default="functional",
                  help="The workload to generate schema for: tpch, hive-benchmark, ...")
parser.add_option("--force_reload", dest="force_reload", action="store_true",
                  default= False, help='Skips HDFS exists check and reloads all tables')
parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                  default = False, help="If set, outputs additional logging.")

(options, args) = parser.parse_args()

WORKLOAD_DIR = os.environ['IMPALA_HOME'] + '/testdata/workloads'
DATASET_DIR = os.environ['IMPALA_HOME'] + '/testdata/datasets'

COMPRESSION_TYPE = "SET mapred.output.compression.type=%s;"
COMPRESSION_ENABLED = "SET hive.exec.compress.output=%s;"
COMPRESSION_CODEC =\
    "SET mapred.output.compression.codec=org.apache.hadoop.io.compress.%s;"
SET_DYNAMIC_PARTITION_STATEMENT = "SET hive.exec.dynamic.partition=true;"
SET_PARTITION_MODE_NONSTRICT_STATEMENT = "SET hive.exec.dynamic.partition.mode=nonstrict;"
SET_HIVE_INPUT_FORMAT = "SET mapred.max.split.size=256000000;"\
                        "SET hive.input.format=org.apache.hadoop.hive.ql.io.%s;"

FILE_FORMAT_IDX = 0
DATA_SET_IDX = 1
CODEC_IDX = 2
COMPRESSION_TYPE_IDX = 3

COMPRESSION_MAP = {'def': 'DefaultCodec',
                   'gzip': 'GzipCodec',
                   'bzip': 'BZip2Codec',
                   'snap': 'SnappyCodec',
                   'none': ''
                  }

FILE_FORMAT_MAP = {'text': 'TEXTFILE',
                   'seq': 'SEQUENCEFILE',
                   'rc': 'RCFILE'
                  }

class SqlGenerationStatement:
  def __init__(self, base_table_name, create, insert, load_local):
    self.base_table_name = base_table_name.strip()
    self.create = create.strip()
    self.insert = insert.strip()
    self.load_local = load_local.strip()

def build_create_statement(table_template, table_name, file_format):
  create_statement = 'DROP TABLE IF EXISTS %s;\n' % table_name
  create_statement += table_template % {'table_name': table_name,
                                        'file_format': FILE_FORMAT_MAP[file_format] }
  return create_statement

def build_compression_codec_statement(codec, compression_type):
  compression_codec = COMPRESSION_MAP[codec]
  if compression_codec:
    return COMPRESSION_TYPE % compression_type.upper() + '\n' +\
           COMPRESSION_CODEC % compression_codec
  else:
    return ''

def build_codec_enabled_statement(codec):
  compression_enabled = 'false' if codec == 'none' else 'true'
  return COMPRESSION_ENABLED % compression_enabled

def build_insert_into_statement(insert, base_table_name, table_name):
  tmp_load_template = insert.replace(' % ', ' *** ')
  statement = SET_PARTITION_MODE_NONSTRICT_STATEMENT + "\n"
  statement += SET_DYNAMIC_PARTITION_STATEMENT + "\n"
  # For some reason (hive bug?) we need to have the CombineHiveInputFormat set for cases
  # where we are compressing in bzip on certain tables that have no partitions.
  if 'bzip' in table_name and 'nopart' in table_name:
    statement += SET_HIVE_INPUT_FORMAT % "CombineHiveInputFormat"
  else:
    statement += SET_HIVE_INPUT_FORMAT % "HiveInputFormat"
  statement += tmp_load_template % {'base_table_name': base_table_name,
                                    'table_name': table_name}
  return statement.replace(' *** ', ' % ')

def build_insert(insert, table_name, base_table_name, codec, compression_type):
  output = build_codec_enabled_statement(codec) + "\n"
  output += build_compression_codec_statement(codec, compression_type) + "\n"
  output += build_insert_into_statement(insert, base_table_name,
                                        table_name) + "\n"
  return output

def build_load_statement(load_template, table_name):
  tmp_load_template = load_template.replace(' % ', ' *** ')
  return (tmp_load_template % {'table_name': table_name}).replace(' *** ', ' % ')

def build_table_suffix(file_format, codec, compression_type):
  if file_format == 'text' and codec != 'none':
    print 'Unsupported combination of file_format (text) and compression codec.'
    sys.exit(1)
  elif file_format == 'text' and codec == 'none':
    return ''
  elif codec == 'none':
    return '_%s' % (file_format)
  elif compression_type == 'record':
    return '_%s_record_%s' % (file_format, codec)
  else:
    return '_%s_%s' % (file_format, codec)

def read_vector_file(file_name):
  with open(file_name, 'rb') as vector_file:
    return [line.strip().split(',')
        for line in vector_file.readlines() if not line.startswith('#')]

def write_array_to_file(file_name, array):
  with open(file_name, 'w') as f:
    f.write('\n\n'.join(array))

# Does a hdfs directory listing and returns array with all the subdir names.
def list_hdfs_subdir_names(path):
  tmp_file = tempfile.TemporaryFile("w+")
  subprocess.call(["hadoop fs -ls " + path], shell = True,
                  stderr = open('/dev/null'), stdout = tmp_file)
  tmp_file.seek(0)

  # Results look like:
  # <acls> -  <user> <group> <date> /directory/subdirectory
  # So to get subdirectory names just return everything after the last '/'
  return [line[line.rfind('/') + 1:].strip() for line in tmp_file.readlines()]

def write_statements_to_file_based_on_input_vector(output_name, input_file_name,
                                                   statements):
  output_create = []
  output_load = []
  output_load_base = []
  results = read_vector_file(input_file_name)
  existing_tables = list_hdfs_subdir_names(options.hive_warehouse_dir)
  for row in results:
    file_format, data_set, codec, compression_type = row[:4]
    for s in statements[data_set.strip()]:
      create = s.create
      insert = s.insert
      load_local = s.load_local
      table_name = s.base_table_name +\
                   build_table_suffix(file_format, codec, compression_type)

      # HBase only supports text format and mixed format tables have formats defined.
      # TODO: Implement a better way to tag a table as only being generated with a fixed
      # set of file formats.
      if (("hbase" in table_name or "mixedformat" in table_name) and
          "text" not in file_format):
        continue

      output_create.append(build_create_statement(create, table_name, file_format))

      # If the directory already exists in HDFS, assume that data files already exist
      # and skip loading the data. Otherwise, the data is generated using either an
      # INSERT INTO statement or a LOAD statement.
      data_path = os.path.join(options.hive_warehouse_dir, table_name)
      if not options.force_reload and table_name in existing_tables:
        print 'Path:', data_path, 'already exists in HDFS. Data loading can be skipped.'
      else:
        print 'Path:', data_path, 'does not exists in HDFS. Data file will be generated.'
        if table_name == s.base_table_name:
          if load_local:
            output_load_base.append(build_load_statement(load_local, table_name))
          else:
            print 'Empty base table load for %s. Skipping load generation' % table_name
        elif insert:
          output_load.append(build_insert(insert, table_name, s.base_table_name,
                                          codec, compression_type))
        else:
            print 'Empty insert for table %s. Skipping insert generation' % table_name

  # Make sure we create the base tables before the remaining tables
  output_load = output_create + output_load_base + output_load
  write_array_to_file('load-' + output_name + '-generated.sql', output_load)

def parse_benchmark_file(file_name):
  template = open(file_name, 'rb')
  statements = collections.defaultdict(list)

  for section in template.read().split('===='):
    sub_section = section.split('----')
    if len(sub_section) == 5:
      data_set = sub_section[0]
      gen_statement = SqlGenerationStatement(*sub_section[1:5])
      statements[data_set.strip()].append(gen_statement)
    elif options.verbose:
      print 'Skipping invalid subsection:', sub_section
  return statements

if (options.exploration_strategy != 'core' and
    options.exploration_strategy != 'pairwise' and
    options.exploration_strategy != 'exhaustive'):
  print 'Invalid exploration strategy:', options.exploration_strategy
  sys.exit(1)

schema_template_file = os.path.join(DATASET_DIR, options.workload,
                                    '%s_schema_template.sql' % (options.workload))

if not os.path.isfile(schema_template_file):
  print 'Schema file not found: ' + schema_template_file
  sys.exit(1)

test_vector_file = os.path.join(WORKLOAD_DIR, options.workload,
                                '%s_%s.csv' % (options.workload,
                                               options.exploration_strategy))

if not os.path.isfile(schema_template_file):
  print 'Vector file not found: ' + schema_template_file
  sys.exit(1)

statements = parse_benchmark_file(schema_template_file)
write_statements_to_file_based_on_input_vector(
    '%s-%s' % (options.workload, options.exploration_strategy),
    test_vector_file, statements)
