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
from tests.util.test_file_parser import *

parser = OptionParser()
parser.add_option("-e", "--exploration_strategy", dest="exploration_strategy",
                  default="core", help="The exploration strategy for schema gen: 'core',"\
                  " 'pairwise', or 'exhaustive'")
parser.add_option("--hive_warehouse_dir", dest="hive_warehouse_dir",
                  default="/test-warehouse",
                  help="The HDFS path to the base Hive test warehouse directory")
parser.add_option("-w", "--workload", dest="workload",
                  help="The workload to generate schema for: tpch, hive-benchmark, ...")
parser.add_option("-s", "--scale_factor", dest="scale_factor", default="",
                  help="An optional scale factor to generate the schema for")
parser.add_option("-f", "--force_reload", dest="force_reload", action="store_true",
                  default= False, help='Skips HDFS exists check and reloads all tables')
parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                  default = False, help="If set, outputs additional logging.")
parser.add_option("-b", "--backend", dest="backend", default="localhost:21000",
                  help="Backend connection to use, default: localhost:21000")
parser.add_option("--table_names", dest="table_names", default=None,
                  help="Only load the specified tables - specified as a comma-seperated "\
                  "list of base table names")
(options, args) = parser.parse_args()

if options.workload is None:
  print "A workload name must be specified."
  parser.print_help()
  sys.exit(1)

WORKLOAD_DIR = os.environ['IMPALA_HOME'] + '/testdata/workloads'
DATASET_DIR = os.environ['IMPALA_HOME'] + '/testdata/datasets'

COMPRESSION_TYPE = "SET mapred.output.compression.type=%s;"
COMPRESSION_ENABLED = "SET hive.exec.compress.output=%s;"
COMPRESSION_CODEC =\
    "SET mapred.output.compression.codec=%s;"
SET_DYNAMIC_PARTITION_STATEMENT = "SET hive.exec.dynamic.partition=true;"
SET_PARTITION_MODE_NONSTRICT_STATEMENT = "SET hive.exec.dynamic.partition.mode=nonstrict;"
SET_HIVE_INPUT_FORMAT = "SET mapred.max.split.size=256000000;\n"\
                        "SET hive.input.format=org.apache.hadoop.hive.ql.io.%s;\n"
FILE_FORMAT_IDX = 0
DATASET_IDX = 1
CODEC_IDX = 2
COMPRESSION_TYPE_IDX = 3

COMPRESSION_MAP = {'def': 'org.apache.hadoop.io.compress.DefaultCodec',
                   'gzip': 'org.apache.hadoop.io.compress.GzipCodec',
                   'bzip': 'org.apache.hadoop.io.compress.BZip2Codec',
                   'snap': 'org.apache.hadoop.io.compress.SnappyCodec',
                   'lzo': 'com.hadoop.compression.lzo.LzopCodec',
                   'none': ''
                  }

TREVNI_COMPRESSION_MAP = {'def': 'deflate',
                          'snap': 'snappy',
                          'none': 'none'
                         }

FILE_FORMAT_MAP = {'text': 'TEXTFILE',
                   'seq': 'SEQUENCEFILE',
                   'rc': 'RCFILE',
                   'trevni': '\n' +
                     'INPUTFORMAT \'org.apache.hadoop.hive.ql.io.TrevniInputFormat\'\n' +
                     'OUTPUTFORMAT \'org.apache.hadoop.hive.ql.io.TrevniOutputFormat\'',
                   'text_lzo': '\n' +
                     'INPUTFORMAT \'com.hadoop.mapred.DeprecatedLzoTextInputFormat\'\n' +
                     'OUTPUTFORMAT ' + 
                     '\'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat\'\n'
                  }

TREVNI_ALTER_STATEMENT = "ALTER TABLE %(table_name)s SET\n\
     SERDEPROPERTIES ('blocksize' = '1073741824', 'compression' = '%(compression)s');"
KNOWN_EXPLORATION_STRATEGIES = ['core', 'pairwise', 'exhaustive', 'lzo']


def build_create_statement(table_template, table_name, file_format,
                           compression, scale_factor):
  create_statement = 'DROP TABLE IF EXISTS %s;\n' % table_name
  if compression == 'lzo':
    file_format = '%s_%s' % (file_format, compression)
  create_statement += table_template % {'table_name': table_name,
                                        'file_format': FILE_FORMAT_MAP[file_format],
                                        'scale_factor': scale_factor}
  if file_format != 'trevni':
    return create_statement

  # Hive does not support a two part name in ALTER statements.
  parts = table_name.split('.')
  if len(parts) == 1:
    return create_statement + '\n\n' + \
                      (TREVNI_ALTER_STATEMENT % {'table_name': table_name,
                      'compression': TREVNI_COMPRESSION_MAP[compression]})
  else:
    return create_statement + '\n\n' + 'use ' + parts[0] + ';\n' + \
                      (TREVNI_ALTER_STATEMENT % {'table_name': parts[1],
                      'compression': TREVNI_COMPRESSION_MAP[compression]}) + \
                      'use default;\n'

def build_compression_codec_statement(codec, compression_type):
  compression_codec = COMPRESSION_MAP[codec]
  if compression_codec:
    return COMPRESSION_TYPE % compression_type.upper() + '\n' +\
           COMPRESSION_CODEC % compression_codec
  return ''

def build_codec_enabled_statement(codec):
  compression_enabled = 'false' if codec == 'none' else 'true'
  return COMPRESSION_ENABLED % compression_enabled

def build_insert_into_statement(insert, base_table_name, table_name, file_format,
                                for_impala=False):
  tmp_load_template = insert.replace(' % ', ' *** ')
  insert_statement = tmp_load_template % {'base_table_name': base_table_name,
                                          'table_name': table_name,
                                          'file_format': FILE_FORMAT_MAP[file_format]}
  insert_statement = insert_statement.replace(' *** ', ' % ')
  if for_impala:
    return insert_statement

  statement = SET_PARTITION_MODE_NONSTRICT_STATEMENT + "\n"
  statement += SET_DYNAMIC_PARTITION_STATEMENT + "\n"
  # For some reason (hive bug?) we need to have the CombineHiveInputFormat set for cases
  # where we are compressing in bzip on certain tables that have multiple files.
  if 'bzip' in table_name and 'multi' in table_name:
    statement += SET_HIVE_INPUT_FORMAT % "CombineHiveInputFormat"
  else:
    statement += SET_HIVE_INPUT_FORMAT % "HiveInputFormat"
  return statement + insert_statement

def build_insert(insert, table_name, file_format,
    base_table_name, codec, compression_type):
  output = build_codec_enabled_statement(codec) + "\n"
  output += build_compression_codec_statement(codec, compression_type) + "\n"
  output += build_insert_into_statement(insert, base_table_name,
                                        table_name, file_format) + "\n"
  return output

def build_load_statement(load_template, table_name, scale_factor):
  tmp_load_template = load_template.replace(' % ', ' *** ')
  return (tmp_load_template % {'table_name': table_name,
                               'scale_factor': scale_factor}).replace(' *** ', ' % ')

def build_trevni(trevni_template, table_name, base_table_name):
  statement =\
      trevni_template % {'table_name': table_name, 'base_table_name': base_table_name}
  # Filter out Impala unsupported 'SET' statements.
  return '\n'.join([line for line in statement.split('\n') if 'set ' not in line.lower()])


def build_table_suffix(file_format, codec, compression_type):
  if file_format == 'text' and codec != 'none' and codec != 'lzo':
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

# Vector files have the format:
# dimension_name1:value1, dimension_name2:value2, ...
def read_vector_file(file_name):
  vector_values = []
  with open(file_name, 'rb') as vector_file:
    for line in vector_file.readlines():
      if line.strip().startswith('#'):
        continue
      vector_values.append([value.split(':')[1].strip() for value in line.split(',')])
  return vector_values

def read_table_constraints(constraints_file):
  """
  Reads a table contraints file, if one exists

  TODO: once the python test frame changes are committed this can be moved to a common
  utility function so the tests themselves can make use of this code.
  """
  schema_include = defaultdict(list)
  schema_exclude = defaultdict(list)
  if not os.path.isfile(constraints_file):
    print 'No schema constraints file file found'
  else:
    with open(constraints_file, 'rb') as constraints_file:
      for line in constraints_file.readlines():
        line = line.strip()
        if not line or line.startswith('#'):
          continue
        # Format: table_name:<name>, contraint_type:<type>, file_format:<t1>,<t2>,...
        table_name, constraint_type, file_types =\
            [value.split(':')[1].strip() for value in line.split(',', 2)]
        if constraint_type == 'restrict_to':
          schema_include[table_name.lower()] += file_types.split(',')
        elif constraint_type == 'exclude':
          schema_exclude[table_name.lower()] += file_types.split(',')
        else:
          print 'Unknown constraint type: ' % constraint_type
          sys.exit(1)
  return schema_include, schema_exclude

def write_trevni_to_file(file_name, array):
  # Strip out all the hive SET statements
  array.insert(0, 'refresh;\n')
  write_array_to_file(file_name, 'w', array)

def write_array_to_file(file_name, mode, array):
  with open(file_name, mode) as f:
    f.write('\n\n'.join(array))

# Does a hdfs directory listing and returns array with all the subdir names.
def get_hdfs_subdirs_with_data(path):
  tmp_file = tempfile.TemporaryFile("w+")
  cmd = "hadoop fs -du %s | grep -v '^0' | awk '{print $2}'" % path
  subprocess.call([cmd], shell = True, stderr = open('/dev/null'), stdout = tmp_file)
  tmp_file.seek(0)

  # Results look like:
  # <acls> -  <user> <group> <date> /directory/subdirectory
  # So to get subdirectory names just return everything after the last '/'
  return [line[line.rfind('/') + 1:].strip() for line in tmp_file.readlines()]

def generate_statements(output_name, test_vectors,
    sections, schema_include_constraints, schema_exclude_constraints):
  output_stats = [SET_HIVE_INPUT_FORMAT % "HiveInputFormat"]
  output_create = []
  output_load = []
  output_load_base = []
  output_trevni = []
  table_names = None
  if options.table_names:
    table_names = [name.lower() for name in options.table_names.split(',')]

  existing_tables = get_hdfs_subdirs_with_data(options.hive_warehouse_dir)
  for row in test_vectors:
    file_format, data_set, codec, compression_type = row[:4]
    for section in sections:
      create = section['CREATE']
      insert = section['DEPENDENT_LOAD']
      load_local = section['LOAD']
      base_table_name = section['BASE_TABLE_NAME']

      base_table_name = base_table_name % {'scale_factor' : options.scale_factor}
      table_name = base_table_name + \
                       build_table_suffix(file_format, codec, compression_type)

      if table_names and (base_table_name.lower() not in table_names):
        print 'Skipping table: %s' % base_table_name
        continue

      if schema_include_constraints[base_table_name.lower()] and \
         file_format not in schema_include_constraints[base_table_name.lower()]:
        print 'Skipping \'%s\' due to include constraint match' % table_name
        continue

      if schema_exclude_constraints[base_table_name.lower()] and\
         file_format in schema_exclude_constraints[base_table_name.lower()]:
        print 'Skipping \'%s\' due to exclude constraint match' % table_name
        continue

      output_create.append(build_create_statement(create, table_name, file_format, codec,
                                                  options.scale_factor))

      # If the directory already exists in HDFS, assume that data files already exist
      # and skip loading the data. Otherwise, the data is generated using either an
      # INSERT INTO statement or a LOAD statement.
      data_path = os.path.join(options.hive_warehouse_dir, table_name)
      if not options.force_reload and table_name in existing_tables:
        print 'HDFS path:', data_path, 'contains data. Data loading can be skipped.'
      else:
        print 'HDFS path:', data_path, 'does not exists or is empty. Data will be loaded.'
        if table_name == base_table_name:
          if load_local:
            output_load_base.append(build_load_statement(load_local, table_name,
                                                         options.scale_factor))
          else:
            print 'Empty base table load for %s. Skipping load generation' % table_name
        elif file_format == 'trevni':
          if insert:
            # In most cases the same load logic can be used for the trevni and non-trevi
            # case, but sometimes it needs to be special cased.
            insert = insert if 'LOAD_TREVNI' not in section else section['LOAD_TREVNI']
            output_trevni.append(build_insert_into_statement(
                insert, base_table_name, table_name, 'trevni', for_impala=True))
          else:
            print \
                'Empty trevni load for table %s. Skipping insert generation' % table_name
        else:
          if insert:
            output_load.append(build_insert(insert, table_name, file_format,
              base_table_name, codec, compression_type))
          else:
              print 'Empty insert for table %s. Skipping insert generation' % table_name

  # Make sure we create the base tables first
  output_load = output_create + output_load_base + output_load
  write_array_to_file('load-' + output_name + '-generated.sql', 'w', output_load)
  write_trevni_to_file('load-trevni-' + output_name + '-generated.sql', output_trevni);

def parse_schema_template_file(file_name):
  VALID_SECTION_NAMES = ['DATASET', 'BASE_TABLE_NAME', 'CREATE', 'DEPENDENT_LOAD',
                         'LOAD', 'LOAD_TREVNI']
  return parse_test_file(file_name, VALID_SECTION_NAMES, skip_unknown_sections=False)

if __name__ == "__main__":
  if options.exploration_strategy not in KNOWN_EXPLORATION_STRATEGIES:
    print 'Invalid exploration strategy:', options.exploration_strategy
    print 'Valid values:', ', '.join(KNOWN_EXPLORATION_STRATEGIES)
    sys.exit(1)

  test_vector_file = os.path.join(WORKLOAD_DIR, options.workload,
                                  '%s_%s.csv' % (options.workload,
                                                 options.exploration_strategy))

  if not os.path.isfile(test_vector_file):
    print 'Vector file not found: ' + test_vector_file
    sys.exit(1)

  test_vectors = read_vector_file(test_vector_file)

  if len(test_vectors) == 0:
    print 'No test vectors found in file: ' + test_vector_file
    sys.exit(1)

  target_dataset = test_vectors[0][DATASET_IDX]
  print 'Target Dataset: ' + target_dataset
  schema_template_file = os.path.join(DATASET_DIR, target_dataset,
                                      '%s_schema_template.sql' % target_dataset)

  if not os.path.isfile(schema_template_file):
    print 'Schema file not found: ' + schema_template_file
    sys.exit(1)

  constraints_file = os.path.join(DATASET_DIR, target_dataset, 'schema_constraints.csv')
  include_constraints, exclude_constraints = read_table_constraints(constraints_file)

  sections = parse_schema_template_file(schema_template_file)
  generate_statements('%s-%s' % (options.workload, options.exploration_strategy),
      test_vectors, sections, include_constraints, exclude_constraints)
