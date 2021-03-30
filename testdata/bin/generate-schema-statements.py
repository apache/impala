#!/usr/bin/env impala-python
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
#
# This script generates statements to create and populate
# tables in a variety of formats. The tables and formats are
# defined through a combination of files:
# 1. Workload format specifics specify for each workload
#    which formats are part of core, exhaustive, etc.
#    This operates via the normal test dimensions.
#    (see tests/common/test_dimension.py and
#     testdata/workloads/*/*.csv)
# 2. Workload table availability constraints specify which
#    tables exist for which formats.
#    (see testdata/datasets/*/schema_constraints.csv)
# The arguments to this script specify the workload and
# exploration strategy and can optionally restrict it
# further to individual tables.
#
# This script is generating several SQL scripts to be
# executed by bin/load-data.py. The two scripts are tightly
# coupled and any change in files generated must be
# reflected in bin/load-data.py. Currently, this script
# generates three things:
# 1. It creates the directory (destroying the existing
#    directory if necessary)
#    ${IMPALA_DATA_LOADING_SQL_DIR}/${workload}
# 2. It creates and populates a subdirectory
#    avro_schemas/${workload} with JSON files specifying
#    the Avro schema for each table.
# 3. It generates SQL files with the following naming schema:
#
#    Using the following variables:
#    workload_exploration = ${workload}-${exploration_strategy} and
#    file_format_suffix = ${file_format}-${codec}-${compression_type}
#
#    A. Impala table creation scripts run in Impala to create tables, partitions,
#       and views. There is one for each file format. They take the form:
#       create-${workload_exploration}-impala-generated-${file_format_suffix}.sql
#
#    B. Hive creation/load scripts run in Hive to load data into tables and create
#       tables or views that Impala does not support. There is one for each
#       file format. They take the form:
#       load-${workload_exploration}-hive-generated-${file_format_suffix}.sql
#
#    C. HBase creation script runs through the hbase commandline to create
#       HBase tables. (Only generated if loading HBase table.) It takes the form:
#       load-${workload_exploration}-hbase-generated.create
#
#    D. HBase postload script runs through the hbase commandline to flush
#       HBase tables. (Only generated if loading HBase table.) It takes the form:
#       post-load-${workload_exploration}-hbase-generated.sql
#
#    E. Impala load scripts run in Impala to load data. Only Parquet and Kudu
#       are loaded through Impala. There is one for each of those formats loaded.
#       They take the form:
#       load-${workload_exploration}-impala-generated-${file_format_suffix}.sql
#
#    F. Invalidation script runs through Impala to invalidate/refresh metadata
#       for tables. It takes the form:
#       invalidate-${workload_exploration}-impala-generated.sql
#
# In summary, table "CREATE" statements are mostly done by Impala. Any "CREATE"
# statements that Impala does not support are done through Hive. Loading data
# into tables mostly runs in Hive except for Parquet and Kudu tables.
# Loading proceeds in two parts: First, data is loaded into text tables.
# Second, almost all other formats are populated by inserts from the text
# table. Since data loaded in Hive may not be visible in Impala, all tables
# need to have metadata refreshed or invalidated before access in Impala.
# This means that loading Parquet or Kudu requires invalidating source
# tables. It also means that invalidate needs to happen at the end of dataload.
#
# For tables requiring customized actions to create schemas or place data,
# this script allows the table specification to include commands that
# this will execute as part of generating the SQL for table. If the command
# generates output, that output is used for that section. This is useful
# for custom tables that rely on loading specific files into HDFS or
# for tables where specifying the schema is tedious (e.g. wide tables).
# This should be used sparingly, because these commands are executed
# serially.
#
import collections
import csv
import glob
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
from itertools import product
from optparse import OptionParser
from tests.common.environ import HIVE_MAJOR_VERSION
from tests.util.test_file_parser import *
from tests.common.test_dimensions import *

parser = OptionParser()
parser.add_option("-e", "--exploration_strategy", dest="exploration_strategy",
                  default="core", help="The exploration strategy for schema gen: 'core',"\
                  " 'pairwise', or 'exhaustive'")
parser.add_option("--hive_warehouse_dir", dest="hive_warehouse_dir",
                  default="/test-warehouse",
                  help="The HDFS path to the base Hive test warehouse directory")
parser.add_option("-w", "--workload", dest="workload",
                  help="The workload to generate schema for: tpch, tpcds, ...")
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
parser.add_option("--table_formats", dest="table_formats", default=None,
                  help="Override the test vectors and load using the specified table "\
                  "formats. Ex. --table_formats=seq/snap/block,text/none")
parser.add_option("--hdfs_namenode", dest="hdfs_namenode", default="localhost:20500",
                  help="HDFS name node for Avro schema URLs, default localhost:20500")
(options, args) = parser.parse_args()

if options.workload is None:
  print "A workload name must be specified."
  parser.print_help()
  sys.exit(1)

WORKLOAD_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata', 'workloads')
DATASET_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata', 'datasets')
SQL_OUTPUT_DIR = os.environ['IMPALA_DATA_LOADING_SQL_DIR']
AVRO_SCHEMA_DIR = "avro_schemas"
DEFAULT_FS=os.environ['DEFAULT_FS']
IMPALA_SUPPORTED_INSERT_FORMATS = ['parquet', 'hbase', 'text', 'kudu']

COMPRESSION_TYPE = "SET mapred.output.compression.type=%s;"
COMPRESSION_ENABLED = "SET hive.exec.compress.output=%s;"
COMPRESSION_CODEC = "SET mapred.output.compression.codec=%s;"
AVRO_COMPRESSION_CODEC = "SET avro.output.codec=%s;"
SET_DYNAMIC_PARTITION_STATEMENT = "SET hive.exec.dynamic.partition=true;"
SET_PARTITION_MODE_NONSTRICT_STATEMENT = "SET hive.exec.dynamic.partition.mode=nonstrict;"
SET_MAX_DYNAMIC_PARTITIONS_STATEMENT = "SET hive.exec.max.dynamic.partitions=10000;\n"\
    "SET hive.exec.max.dynamic.partitions.pernode=10000;"
SET_HIVE_INPUT_FORMAT = "SET mapred.max.split.size=256000000;\n"\
                        "SET hive.input.format=org.apache.hadoop.hive.ql.io.%s;\n"
SET_HIVE_HBASE_BULK_LOAD = "SET hive.hbase.bulk = true"
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

AVRO_COMPRESSION_MAP = {
  'def': 'deflate',
  'snap': 'snappy',
  'none': '',
  }

FILE_FORMAT_MAP = {
  'text': 'TEXTFILE',
  'seq': 'SEQUENCEFILE',
  'rc': 'RCFILE',
  'orc': 'ORC',
  'parquet': 'PARQUET',
  'hudiparquet': 'HUDIPARQUET',
  'text_lzo':
    "\nINPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'" +
    "\nOUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'",
  'avro': 'AVRO',
  'hbase': "'org.apache.hadoop.hive.hbase.HBaseStorageHandler'",
  'kudu': "KUDU",
  }

HIVE_TO_AVRO_TYPE_MAP = {
  'STRING': 'string',
  'INT': 'int',
  'TINYINT': 'int',
  'SMALLINT': 'int',
  'BIGINT': 'long',
  'BOOLEAN': 'boolean',
  'FLOAT': 'float',
  'DOUBLE': 'double',
  # Avro has no timestamp type, so convert to string
  # TODO: this allows us to create our Avro test tables, but any tests that use
  # a timestamp column will fail. We probably want to convert back to timestamps
  # in our tests.
  'TIMESTAMP': 'string',
  }

PARQUET_ALTER_STATEMENT = "ALTER TABLE %(table_name)s SET\n\
     SERDEPROPERTIES ('blocksize' = '1073741824', 'compression' = '%(compression)s');"

HBASE_CREATE_STATEMENT = """
CREATE EXTERNAL TABLE IF NOT EXISTS {{db_name}}{{db_suffix}}.{{table_name}} (
{columns})
STORED BY {{file_format}}
WITH SERDEPROPERTIES (
  "hbase.columns.mapping" =
  "{hbase_column_mapping}")
{tbl_properties}{{hdfs_location}}"""

KNOWN_EXPLORATION_STRATEGIES = ['core', 'pairwise', 'exhaustive', 'lzo']

def build_create_statement(table_template, table_name, db_name, db_suffix,
                           file_format, compression, hdfs_location,
                           force_reload):
  create_stmt = ''
  if (force_reload):
    create_stmt += 'DROP TABLE IF EXISTS %s%s.%s;\n' % (db_name, db_suffix, table_name)
  if compression == 'lzo':
    file_format = '%s_%s' % (file_format, compression)
  # hbase / kudu tables are external, and not read from hdfs. We don't need an
  # hdfs_location.
  if file_format in ['hbase', 'kudu']:
    hdfs_location = str()
    # Remove location part from the format string
    table_template = table_template.replace("LOCATION '{hdfs_location}'", "")

  create_stmt += table_template.format(db_name=db_name,
                                       db_suffix=db_suffix,
                                       table_name=table_name,
                                       file_format=FILE_FORMAT_MAP[file_format],
                                       hdfs_location=hdfs_location)
  return create_stmt


def parse_table_properties(file_format, table_properties):
  """
  Read the properties specified in the TABLE_PROPERTIES section.
  The table properties can be restricted to a file format or are applicable
  for all formats.
  For specific format the syntax is <fileformat>:<key>=<val>
  """
  tblproperties = {}
  TABLE_PROPERTY_RE = re.compile(
      # Optional '<data-format>:' prefix, capturing just the 'data-format' part.
      r'(?:(\w+):)?' +
      # Required key=value, capturing the key and value
      r'(.+?)=(.*)')
  for table_property in filter(None, table_properties.split("\n")):
    m = TABLE_PROPERTY_RE.match(table_property)
    if not m:
      raise Exception("Invalid table property line: {0}", format(table_property))
    only_format, key, val = m.groups()
    if only_format is not None and only_format != file_format:
      continue
    tblproperties[key] = val

  return tblproperties


def build_table_template(file_format, columns, partition_columns, row_format,
                         avro_schema_dir, table_name, tblproperties):
  if file_format == 'hbase':
    return build_hbase_create_stmt_in_hive(columns, partition_columns, table_name)

  primary_keys_clause = ""

  partitioned_by = str()
  if partition_columns:
    partitioned_by = 'PARTITIONED BY (%s)' % ', '.join(partition_columns.split('\n'))

  row_format_stmt = str()
  if row_format and file_format != 'kudu':
    row_format_stmt = 'ROW FORMAT ' + row_format

  file_format_string = "STORED AS {file_format}"

  tblproperties_clause = "TBLPROPERTIES (\n{0}\n)"

  external = "" if is_transactional(tblproperties) else "EXTERNAL"

  if file_format == 'avro':
    # TODO Is this flag ever used?
    if options.hdfs_namenode is None:
      tblproperties["avro.schema.url"] = "%s/%s/%s/{table_name}.json" \
        % (DEFAULT_FS, options.hive_warehouse_dir, avro_schema_dir)
    else:
      tblproperties["avro.schema.url"] = "hdfs://%s/%s/%s/{table_name}.json" \
        % (options.hdfs_namenode, options.hive_warehouse_dir, avro_schema_dir)
  elif file_format in ['parquet', 'orc']:  # columnar formats don't need row format
    row_format_stmt = str()
  elif file_format == 'kudu':
    # Use partitioned_by to set a trivial hash distribution
    assert not partitioned_by, "Kudu table shouldn't have partition cols defined"
    partitioned_by = "partition by hash partitions 3"

    row_format_stmt = str()
    primary_keys_clause = ", PRIMARY KEY (%s)" % columns.split("\n")[0].split(" ")[0]
    # Kudu's test tables are managed.
    external = ""

  all_tblproperties = []
  for key, value in tblproperties.iteritems():
    all_tblproperties.append("'{0}' = '{1}'".format(key, value))

  # If there are no properties to set avoid the TBLPROPERTIES clause altogether.
  if not all_tblproperties:
    tblproperties_clause = ""
  else:
    tblproperties_clause = tblproperties_clause.format(",\n".join(all_tblproperties))

  # Note: columns are ignored but allowed if a custom serde is specified
  # (e.g. Avro)
  stmt = """
CREATE {external} TABLE IF NOT EXISTS {{db_name}}{{db_suffix}}.{{table_name}} (
{columns}
{primary_keys})
{partitioned_by}
{row_format}
{file_format_string}
LOCATION '{{hdfs_location}}'
{tblproperties}
""".format(
    external=external,
    row_format=row_format_stmt,
    columns=',\n'.join(columns.split('\n')),
    primary_keys=primary_keys_clause,
    partitioned_by=partitioned_by,
    tblproperties=tblproperties_clause,
    file_format_string=file_format_string
    ).strip()

  # Remove empty lines from the stmt string.  There is an empty line for
  # each of the sections that didn't have anything (e.g. partitioned_by)
  stmt = os.linesep.join([s for s in stmt.splitlines() if s])
  stmt += ';'
  return stmt

def build_hbase_create_stmt_in_hive(columns, partition_columns, table_name):
  # The hbase create statement differs sufficiently from the generic create to justify a
  # separate method. Specifically, STORED AS becomes STORED BY. There is section called
  # serdeproperties, the partition colmns have to be appended to columns in the schema.
  columns = columns.split('\n')
  # partition columns have to be appended to the columns in the schema.
  # PARTITIONED BY is not supported and does not make sense for HBase.
  if partition_columns:
    columns.extend(partition_columns.split('\n'))
  # stringids is a special case. It still points to functional_hbase.alltypesagg
  if 'stringids' not in table_name:
    tbl_properties = ('TBLPROPERTIES("hbase.table.name" = '
                      '"{db_name}{db_suffix}.{table_name}")')
  else:
    tbl_properties = ('TBLPROPERTIES("hbase.table.name" = '
                      '"{db_name}{db_suffix}.alltypesagg")')
  # build hbase column mapping, the first column is implicitly the primary key
  # which has a diffrerent representation [:key]
  hbase_column_mapping = ["d:%s" % c.split(' ')[0] for c in columns[1:]]
  hbase_column_mapping = ":key," + ','.join(hbase_column_mapping)
  stmt = HBASE_CREATE_STATEMENT.format(
    columns=',\n'.join(columns),
    hbase_column_mapping=hbase_column_mapping,
    tbl_properties=tbl_properties,
    ).strip()
  return stmt + ';'

def avro_schema(columns):
  record = {
    "name": "a", # doesn't matter
    "type": "record",
    "fields": list()
    }
  for column_spec in columns.strip().split('\n'):
    # column_spec looks something like "col_name col_type COMMENT comment"
    # (comment may be omitted, we don't use it)
    name = column_spec.split()[0]

    if "DECIMAL" in column_spec.upper():
      if column_spec.split()[1].upper() == "DECIMAL":
        # No scale and precision specified, use defaults
        scale = 0
        precision = 9
      else:
        # Parse out scale and precision from decimal type
        m = re.search("DECIMAL\((?P<precision>.*),(?P<scale>.*)\)", column_spec.upper())
        assert m, "Could not parse decimal column spec: " + column_spec
        scale = int(m.group('scale'))
        precision = int(m.group('precision'))
      type = {"type": "bytes", "logicalType": "decimal", "precision": precision,
              "scale": scale}
    else:
      hive_type = column_spec.split()[1].upper()
      if hive_type.startswith('CHAR(') or hive_type.startswith('VARCHAR('):
        type = 'string'
      elif hive_type == 'DATE':
        type = {"type": "int", "logicalType": "date"}
      else:
        type = HIVE_TO_AVRO_TYPE_MAP[hive_type]

    record["fields"].append(
      {'name': name,
       'type': [type, "null"]}) # all columns nullable

  return json.dumps(record)

def build_compression_codec_statement(codec, compression_type, file_format):
  codec = AVRO_COMPRESSION_MAP[codec] if file_format == 'avro' else COMPRESSION_MAP[codec]
  if not codec:
    return str()
  return (AVRO_COMPRESSION_CODEC % codec) if file_format == 'avro' else (
    COMPRESSION_TYPE % compression_type.upper() + '\n' + COMPRESSION_CODEC % codec)

def build_codec_enabled_statement(codec):
  compression_enabled = 'false' if codec == 'none' else 'true'
  return COMPRESSION_ENABLED % compression_enabled

def build_insert_into_statement(insert, db_name, db_suffix, table_name, file_format,
                                hdfs_path, for_impala=False):
  insert_hint = "/* +shuffle, clustered */" \
    if for_impala and file_format == 'parquet' else ""
  insert_statement = insert.format(db_name=db_name,
                                   db_suffix=db_suffix,
                                   table_name=table_name,
                                   hdfs_location=hdfs_path,
                                   impala_home=os.getenv("IMPALA_HOME"),
                                   hint=insert_hint)

  # Kudu tables are managed and don't support OVERWRITE, so we replace OVERWRITE
  # with INTO to make this a regular INSERT.
  if file_format == 'kudu':
    insert_statement = insert_statement.replace("OVERWRITE", "INTO")

  if for_impala:
    return insert_statement

  statement = SET_PARTITION_MODE_NONSTRICT_STATEMENT + "\n"
  statement += SET_DYNAMIC_PARTITION_STATEMENT + "\n"
  statement += SET_MAX_DYNAMIC_PARTITIONS_STATEMENT + "\n"
  statement += "set hive.auto.convert.join=true;\n"

  # For some reason (hive bug?) we need to have the CombineHiveInputFormat set
  # for cases where we are compressing in bzip or lzo on certain tables that
  # have multiple files.
  if 'multi' in table_name and ('bzip' in db_suffix or 'lzo' in db_suffix):
    statement += SET_HIVE_INPUT_FORMAT % "CombineHiveInputFormat"
  else:
    statement += SET_HIVE_INPUT_FORMAT % "HiveInputFormat"
  return statement + insert_statement

def build_hbase_insert(db_name, db_suffix, table_name):
  hbase_insert = SET_HIVE_HBASE_BULK_LOAD + ';\n'
  hbase_insert += ("INSERT OVERWRITE TABLE {db_name}{db_suffix}.{table_name}"
                   " SELECT * FROM {db_name}.{table_name};\n").\
                   format(db_name=db_name, db_suffix=db_suffix,table_name=table_name)
  return hbase_insert

def build_insert(insert, db_name, db_suffix, file_format,
                 codec, compression_type, table_name, hdfs_path, create_hive=False):
  # HBASE inserts don't need the hive options to be set, and don't require and HDFS
  # file location, so they're handled separately.
  if file_format == 'hbase' and not create_hive:
    return build_hbase_insert(db_name, db_suffix, table_name)
  output = build_codec_enabled_statement(codec) + "\n"
  output += build_compression_codec_statement(codec, compression_type, file_format) + "\n"
  output += build_insert_into_statement(insert, db_name, db_suffix,
                                        table_name, file_format, hdfs_path) + "\n"
  return output

def build_load_statement(load_template, db_name, db_suffix, table_name):
  # hbase does not need the hdfs path.
  if table_name.startswith('hbase'):
    load_template = load_template.format(table_name=table_name,
                                         db_name=db_name,
                                         db_suffix=db_suffix)
  else:
    base_load_dir = os.getenv("REMOTE_LOAD", os.getenv("IMPALA_HOME"))
    load_template = load_template.format(table_name=table_name,
                                         db_name=db_name,
                                         db_suffix=db_suffix,
                                         impala_home = base_load_dir)
  return load_template

def build_hbase_create_stmt(db_name, table_name, column_families, region_splits):
  hbase_table_name = "{db_name}_hbase.{table_name}".format(db_name=db_name,
                                                           table_name=table_name)
  create_stmts = list()
  create_stmts.append("disable '%s'" % hbase_table_name)
  create_stmts.append("drop '%s'" % hbase_table_name)
  column_families = ','.join(["'{0}'".format(cf) for cf in column_families.splitlines()])
  create_statement = "create '%s', %s" % (hbase_table_name, column_families)
  if (region_splits):
    create_statement += ", {SPLITS => [" + region_splits.strip() + "]}"

  create_stmts.append(create_statement)
  return create_stmts

# Does a hdfs directory listing and returns array with all the subdir names.
def get_hdfs_subdirs_with_data(path):
  tmp_file = tempfile.TemporaryFile("w+")
  cmd = "hadoop fs -du %s | grep -v '^0' | awk '{print $3}'" % path
  subprocess.call([cmd], shell = True, stderr = open('/dev/null'), stdout = tmp_file)
  tmp_file.seek(0)

  # Results look like:
  # <acls> -  <user> <group> <date> /directory/subdirectory
  # So to get subdirectory names just return everything after the last '/'
  return [line[line.rfind('/') + 1:].strip() for line in tmp_file.readlines()]

class Statements(object):
  """Simple container object for storing SQL statements to be output to a
  file. Useful for ordering the statements correctly."""
  def __init__(self):
    self.create = list()
    self.load = list()
    self.load_base = list()

  def write_to_file(self, filename):
    # If there is no content to write, skip
    if not self: return
    output = self.create + self.load_base + self.load
    with open(filename, 'w') as f:
      f.write('\n\n'.join(output))

  def __nonzero__(self):
    return bool(self.create or self.load or self.load_base)

def eval_section(section_str):
  """section_str should be the contents of a section (i.e. a string). If section_str
  starts with `, evaluates section_str as a shell command and returns the
  output. Otherwise returns section_str."""
  if not section_str.startswith('`'): return section_str
  cmd = section_str[1:]
  # Use bash explicitly instead of setting shell=True so we get more advanced shell
  # features (e.g. "for i in {1..n}")
  p = subprocess.Popen(['/bin/bash', '-c', cmd], stdout=subprocess.PIPE)
  stdout, stderr = p.communicate()
  if stderr: print stderr
  assert p.returncode == 0
  return stdout.strip()

def generate_statements(output_name, test_vectors, sections,
                        schema_include_constraints, schema_exclude_constraints,
                        schema_only_constraints):
  # TODO: This method has become very unwieldy. It has to be re-factored sooner than
  # later.
  # Parquet statements to be executed separately by Impala
  hbase_output = Statements()
  hbase_post_load = Statements()
  impala_invalidate = Statements()

  table_names = None
  if options.table_names:
    table_names = [name.lower() for name in options.table_names.split(',')]
  existing_tables = get_hdfs_subdirs_with_data(options.hive_warehouse_dir)
  for row in test_vectors:
    impala_create = Statements()
    hive_output = Statements()
    impala_load = Statements()
    file_format, data_set, codec, compression_type =\
        [row.file_format, row.dataset, row.compression_codec, row.compression_type]
    table_format = '%s/%s/%s' % (file_format, codec, compression_type)
    db_suffix = row.db_suffix()
    db_name = '{0}{1}'.format(data_set, options.scale_factor)
    db = '{0}{1}'.format(db_name, db_suffix)
    create_db_stmt = 'CREATE DATABASE IF NOT EXISTS {0};\n'.format(db)
    impala_create.create.append(create_db_stmt)
    for section in sections:
      table_name = section['BASE_TABLE_NAME'].strip()

      if table_names and (table_name.lower() not in table_names):
        print 'Skipping table: %s.%s, table is not in specified table list' % (db, table_name)
        continue

      # Check Hive version requirement, if present.
      if section['HIVE_MAJOR_VERSION'] and \
         section['HIVE_MAJOR_VERSION'].strip() != \
         os.environ['IMPALA_HIVE_MAJOR_VERSION'].strip():
        print "Skipping table '{0}.{1}': wrong Hive major version".format(db, table_name)
        continue

      if table_format in schema_only_constraints and \
         table_name.lower() not in schema_only_constraints[table_format]:
        print ('Skipping table: %s.%s, \'only\' constraint for format did not '
              'include this table.') % (db, table_name)
        continue

      if schema_include_constraints[table_name.lower()] and \
         table_format not in schema_include_constraints[table_name.lower()]:
        print 'Skipping \'%s.%s\' due to include constraint match.' % (db, table_name)
        continue

      if schema_exclude_constraints[table_name.lower()] and\
         table_format in schema_exclude_constraints[table_name.lower()]:
        print 'Skipping \'%s.%s\' due to exclude constraint match.' % (db, table_name)
        continue

      alter = section.get('ALTER')
      create = section['CREATE']
      create_hive = section['CREATE_HIVE']
      assert not (create and create_hive), "Can't set both CREATE and CREATE_HIVE"

      table_properties = section['TABLE_PROPERTIES']
      insert = eval_section(section['DEPENDENT_LOAD'])
      insert_hive = eval_section(section['DEPENDENT_LOAD_HIVE'])
      assert not (insert and insert_hive),\
          "Can't set both DEPENDENT_LOAD and DEPENDENT_LOAD_HIVE"
      load = eval_section(section['LOAD'])

      if file_format == 'kudu':
        create_kudu = section["CREATE_KUDU"]
        if section['DEPENDENT_LOAD_KUDU']:
          insert = eval_section(section['DEPENDENT_LOAD_KUDU'])
      else:
        create_kudu = None

      if file_format == 'orc' and section["DEPENDENT_LOAD_ACID"]:
        insert = None
        insert_hive = eval_section(section["DEPENDENT_LOAD_ACID"])

      columns = eval_section(section['COLUMNS']).strip()
      partition_columns = section['PARTITION_COLUMNS'].strip()
      row_format = section['ROW_FORMAT'].strip()

      # Force reloading of the table if the user specified the --force option or
      # if the table is partitioned and there was no ALTER section specified. This is to
      # ensure the partition metadata is always properly created. The ALTER section is
      # used to create partitions, so if that section exists there is no need to force
      # reload.
      # IMPALA-6579: Also force reload all Kudu tables. The Kudu entity referenced
      # by the table may or may not exist, so requiring a force reload guarantees
      # that the Kudu entity is always created correctly.
      # TODO: Rename the ALTER section to ALTER_TABLE_ADD_PARTITION
      force_reload = options.force_reload or (partition_columns and not alter) or \
          file_format == 'kudu'

      # Empty tables (tables with no "LOAD" sections) are assumed to be used for insert
      # testing. Since Impala currently only supports inserting into TEXT, PARQUET and
      # HBASE we need to create these tables with a supported insert format.
      create_file_format = file_format
      create_codec = codec
      if not (section['LOAD'] or section['DEPENDENT_LOAD']
              or section['DEPENDENT_LOAD_HIVE'] or section['DEPENDENT_LOAD_ACID']):
        create_codec = 'none'
        create_file_format = file_format
        if file_format not in IMPALA_SUPPORTED_INSERT_FORMATS:
          create_file_format = 'text'

      tblproperties = parse_table_properties(create_file_format, table_properties)
      # ORC tables are full ACID by default.
      if (HIVE_MAJOR_VERSION == 3 and
          create_file_format == 'orc' and
          'transactional' not in tblproperties):
        tblproperties['transactional'] = 'true'

      hdfs_location = '{0}.{1}{2}'.format(db_name, table_name, db_suffix)
      # hdfs file names for functional datasets are stored
      # directly under /test-warehouse
      # TODO: We should not need to specify the hdfs file path in the schema file.
      # This needs to be done programmatically.
      if data_set == 'functional':
        hdfs_location = hdfs_location.split('.')[-1]
      # Transactional tables need to be put under the 'managed' directory.
      if is_transactional(tblproperties):
        hdfs_location = os.path.join('managed', hdfs_location)
      data_path = os.path.join(options.hive_warehouse_dir, hdfs_location)

      output = impala_create
      if create_hive or file_format == 'hbase':
        output = hive_output
      elif codec == 'lzo':
        # Impala CREATE TABLE doesn't allow INPUTFORMAT.
        output = hive_output

      # TODO: Currently, Kudu does not support partitioned tables via Impala.
      # If a CREATE_KUDU section was provided, assume it handles the partition columns
      if file_format == 'kudu' and partition_columns != '' and not create_kudu:
        print "Ignore partitions on Kudu table: %s.%s" % (db_name, table_name)
        continue

      # If a CREATE section is provided, use that. Otherwise a COLUMNS section
      # must be provided (and optionally PARTITION_COLUMNS and ROW_FORMAT
      # sections), which is used to generate the create table statement.
      if create_hive:
        table_template = create_hive
      elif create_kudu:
        table_template = create_kudu
      elif create:
        table_template = create
        if file_format in ['avro', 'hbase', 'kudu']:
          # We don't know how to generalize CREATE sections to Avro and hbase.
          print ("CREATE section not supported with %s, "
                 "skipping: '%s'" % (file_format, table_name))
          continue
      elif columns:
        avro_schema_dir = "%s/%s" % (AVRO_SCHEMA_DIR, data_set)
        table_template = build_table_template(
          create_file_format, columns, partition_columns,
          row_format, avro_schema_dir, table_name, tblproperties)
        # Write Avro schema to local file
        if file_format == 'avro':
          if not os.path.exists(avro_schema_dir):
            os.makedirs(avro_schema_dir)
          with open("%s/%s.json" % (avro_schema_dir, table_name),"w") as f:
            f.write(avro_schema(columns))
      else:
        table_template = None

      if table_template:
        output.create.append(build_create_statement(table_template, table_name, db_name,
            db_suffix, create_file_format, create_codec, data_path, force_reload))
      # HBASE create table
      if file_format == 'hbase':
        # If the HBASE_COLUMN_FAMILIES section does not exist, default to 'd'
        column_families = section.get('HBASE_COLUMN_FAMILIES', 'd')
        region_splits = section.get('HBASE_REGION_SPLITS', None)
        hbase_output.create.extend(build_hbase_create_stmt(db_name, table_name,
            column_families, region_splits))
        hbase_post_load.load.append("flush '%s_hbase.%s'\n" % (db_name, table_name))

      # Need to make sure that tables created and/or data loaded in Hive is seen
      # in Impala. We only need to do a full invalidate if the table was created in Hive
      # and Impala doesn't know about it. Otherwise, do a refresh.
      if output == hive_output:
        invalidate_table_stmt = "INVALIDATE METADATA {0}.{1};\n".format(db, table_name)
      else:
        invalidate_table_stmt = "REFRESH {0}.{1};\n".format(db, table_name)
      impala_invalidate.create.append(invalidate_table_stmt)

      # The ALTER statement in hive does not accept fully qualified table names so
      # insert a use statement. The ALTER statement is skipped for HBASE as it's
      # used for adding partitions.
      # TODO: Consider splitting the ALTER subsection into specific components. At the
      # moment, it assumes we're only using ALTER for partitioning the table.
      if alter and file_format not in ("hbase", "kudu"):
        use_db = 'USE {db_name};\n'.format(db_name=db)
        if output == hive_output and codec == 'lzo':
          # Hive ALTER TABLE ADD PARTITION doesn't handle null partitions, so
          # we can't run the ALTER section in this case.
          if options.force_reload:
            # IMPALA-2278: Hive INSERT OVERWRITE won't clear out partition directories
            # that weren't already added to the table. So, for force reload, manually
            # delete the partition directories.
            output.create.append(("DFS -rm -R {data_path};").format(
              data_path=data_path))
          else:
            # If this is not a force reload use msck repair to add the partitions
            # into the table.
            output.create.append(use_db + 'msck repair table %s;' % (table_name))
        else:
          output.create.append(use_db + alter.format(table_name=table_name))

      # If the directory already exists in HDFS, assume that data files already exist
      # and skip loading the data. Otherwise, the data is generated using either an
      # INSERT INTO statement or a LOAD statement.
      if not force_reload and hdfs_location in existing_tables:
        print 'HDFS path:', data_path, 'contains data. Data loading can be skipped.'
      else:
        print 'HDFS path:', data_path, 'does not exists or is empty. Data will be loaded.'
        if not db_suffix:
          if load:
            hive_output.load_base.append(build_load_statement(load, db_name,
                                                              db_suffix, table_name))
          else:
            print 'Empty base table load for %s. Skipping load generation' % table_name
        elif file_format in ['kudu', 'parquet']:
          if insert_hive:
            hive_output.load.append(build_insert(insert_hive, db_name, db_suffix,
                file_format, codec, compression_type, table_name, data_path))
          elif insert:
            impala_load.load.append(build_insert_into_statement(insert, db_name,
                db_suffix, table_name, file_format, data_path, for_impala=True))
          else:
            print 'Empty parquet/kudu load for table %s. Skipping insert generation' \
              % table_name
        else:
          if insert_hive:
            insert = insert_hive
          if insert:
            hive_output.load.append(build_insert(insert, db_name, db_suffix, file_format,
                codec, compression_type, table_name, data_path, create_hive=create_hive))
          else:
            print 'Empty insert for table %s. Skipping insert generation' % table_name

    impala_create.write_to_file("create-%s-impala-generated-%s-%s-%s.sql" %
        (output_name, file_format, codec, compression_type))
    hive_output.write_to_file("load-%s-hive-generated-%s-%s-%s.sql" %
        (output_name, file_format, codec, compression_type))
    impala_load.write_to_file("load-%s-impala-generated-%s-%s-%s.sql" %
        (output_name, file_format, codec, compression_type))

  if hbase_output:
    hbase_output.create.append("exit")
    hbase_output.write_to_file('load-' + output_name + '-hbase-generated.create')
  if hbase_post_load:
    hbase_post_load.load.append("exit")
    hbase_post_load.write_to_file('post-load-' + output_name + '-hbase-generated.sql')
  impala_invalidate.write_to_file("invalidate-" + output_name + "-impala-generated.sql")


def is_transactional(table_properties):
  return table_properties.get('transactional', "").lower() == 'true'


def parse_schema_template_file(file_name):
  VALID_SECTION_NAMES = ['DATASET', 'BASE_TABLE_NAME', 'COLUMNS', 'PARTITION_COLUMNS',
                         'ROW_FORMAT', 'CREATE', 'CREATE_HIVE', 'CREATE_KUDU',
                         'DEPENDENT_LOAD', 'DEPENDENT_LOAD_KUDU', 'DEPENDENT_LOAD_HIVE',
                         'DEPENDENT_LOAD_ACID', 'LOAD', 'ALTER', 'HBASE_COLUMN_FAMILIES',
                         'TABLE_PROPERTIES', 'HBASE_REGION_SPLITS', 'HIVE_MAJOR_VERSION']
  return parse_test_file(file_name, VALID_SECTION_NAMES, skip_unknown_sections=False)

if __name__ == "__main__":
  if options.table_formats is None:
    if options.exploration_strategy not in KNOWN_EXPLORATION_STRATEGIES:
      print 'Invalid exploration strategy:', options.exploration_strategy
      print 'Valid values:', ', '.join(KNOWN_EXPLORATION_STRATEGIES)
      sys.exit(1)

    test_vectors = [vector.value for vector in\
        load_table_info_dimension(options.workload, options.exploration_strategy)]
  else:
    table_formats = options.table_formats.split(',')
    dataset = get_dataset_from_workload(options.workload)
    test_vectors =\
        [TableFormatInfo.create_from_string(dataset, tf) for tf in table_formats]

  target_dataset = test_vectors[0].dataset
  print 'Target Dataset: ' + target_dataset
  dataset_load_dir = os.path.join(SQL_OUTPUT_DIR, target_dataset)
  # If the directory containing the sql files does not exist, create it. Else nuke all the
  # files corresponding to the current workload.
  try:
    os.makedirs(dataset_load_dir)
  except OSError:
    # Directory already exists, remove it.
    shutil.rmtree(dataset_load_dir)
    # Recreate the workload dir
    os.makedirs(dataset_load_dir)
  finally:
    # Make sure that the directory was created and is empty.
    assert os.path.isdir(dataset_load_dir)
    assert len(os.listdir(dataset_load_dir)) == 0
    # Make the dataset dir the current working directory
    os.chdir(dataset_load_dir)

  schema_template_file = os.path.join(DATASET_DIR, target_dataset,
                                      '%s_schema_template.sql' % target_dataset)

  if not os.path.isfile(schema_template_file):
    print 'Schema file not found: ' + schema_template_file
    sys.exit(1)

  constraints_file = os.path.join(DATASET_DIR, target_dataset, 'schema_constraints.csv')
  include_constraints, exclude_constraints, only_constraints = \
      parse_table_constraints(constraints_file)
  sections = parse_schema_template_file(schema_template_file)
  generate_statements('%s-%s' % (options.workload, options.exploration_strategy),
      test_vectors, sections, include_constraints, exclude_constraints, only_constraints)
