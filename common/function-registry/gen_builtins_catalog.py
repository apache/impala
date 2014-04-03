#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script generates the FE calls to populate the builtins.
# To add a builtin, add an entry to impala_functions.py.

import sys
import os
from string import Template
import impala_functions

java_registry_preamble = '\
//  Copyright 2012 Cloudera Inc.\n\
// \n\
//  Licensed under the Apache License, Version 2.0 (the "License");\n\
//  you may not use this file except in compliance with the License.\n\
//  You may obtain a copy of the License at\n\
// \n\
//  http://www.apache.org/licenses/LICENSE-2.0\n\
// \n\
//  Unless required by applicable law or agreed to in writing, software\n\
//  distributed under the License is distributed on an "AS IS" BASIS,\n\
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
//  See the License for the specific language governing permissions and\n\
//  limitations under the License.\n\
\n\
// This is a generated file, DO NOT EDIT.\n\
// To add new functions, see impala/common/function-registry/gen_opcodes.py\n\
\n\
package com.cloudera.impala.builtins;\n\
\n\
import com.cloudera.impala.catalog.ColumnType;\n\
import com.cloudera.impala.catalog.Db;\n\
\n\
public class ScalarBuiltins { \n\
  public static void initBuiltins(Db db) { \
\n'

java_registry_epilogue = '\
  }\n\
}\n'

FE_PATH = "../../fe/generated-sources/gen-java/com/cloudera/impala/builtins/"

# This contains all the metadata to describe all the builtins.
# Each meta data entry is itself a map to store all the meta data
#   - fn_name, ret_type, args, symbol, sql_names
meta_data_entries = []

# Read in the function and add it to the meta_data_entries map
def add_function(fn_meta_data, udf_interface):
  entry = {}
  entry["sql_names"] = fn_meta_data[0]
  entry["ret_type"] = fn_meta_data[1]
  entry["args"] = fn_meta_data[2]
  entry["symbol"] = fn_meta_data[3]
  entry["udf_interface"] = udf_interface
  if len(fn_meta_data) >= 6:
    entry["prepare"] = fn_meta_data[4]
    entry["close"] = fn_meta_data[5]
  meta_data_entries.append(entry)

def generate_fe_entry(entry, name):
  java_output = ""
  if entry["udf_interface"]:
    java_output += "true"
  else:
    java_output += "false"
  java_output += ", \"" + name + "\""
  java_output += ", \"" + entry["symbol"] + "\""

  if 'prepare' in entry:
    assert 'close' in entry
    java_output += ', "%s"' % entry["prepare"]
    java_output += ', "%s"' % entry["close"]

  # Check the last entry for varargs indicator.
  if entry["args"] and entry["args"][-1] == "...":
    entry["args"].pop()
    java_output += ", true"
  else:
    java_output += ", false"

  java_output += ", ColumnType." + entry["ret_type"]
  for arg in entry["args"]:
    java_output += ", ColumnType." + arg
  return java_output

# Generates the FE builtins init file that registers all the builtins.
def generate_fe_registry_init(filename):
  java_registry_file = open(filename, "w")
  java_registry_file.write(java_registry_preamble)

  for entry in meta_data_entries:
    for name in entry["sql_names"]:
      java_output = generate_fe_entry(entry, name)
      java_registry_file.write("    db.addScalarBuiltin(%s);\n" % java_output)

  java_registry_file.write("\n")
  java_registry_file.write(java_registry_epilogue)
  java_registry_file.close()

# Read the function metadata inputs
for function in impala_functions.functions:
  if len(function) != 4 and len(function) != 6:
    print "Invalid function entry in impala_functions.py:\n\t" + repr(function)
    sys.exit(1)
  add_function(function, False)

for function in impala_functions.udf_functions:
  assert len(function) == 4 or len(function) == 6, \
         "Invalid function entry in impala_functions.py:\n\t" + repr(function)
  add_function(function, True)

if not os.path.exists(FE_PATH):
  os.makedirs(FE_PATH)

generate_fe_registry_init(FE_PATH + "ScalarBuiltins.java")

