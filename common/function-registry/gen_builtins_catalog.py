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
# under the License.

# This script generates the FE calls to populate the builtins.
# To add a builtin, add an entry to impala_functions.py.

from __future__ import absolute_import, division, print_function
import os
import impala_functions

java_registry_preamble = '\
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
// To add new functions, see the generator at\n\
// common/function-registry/gen_builtins_catalog.py or the function list at\n\
// common/function-registry/impala_functions.py.\n\
\n\
package org.apache.impala.builtins;\n\
\n\
import org.apache.impala.catalog.Type;\n\
import org.apache.impala.catalog.Db;\n\
\n\
public class ScalarBuiltins { \n\
  public static void initBuiltins(Db db) { \
\n'

java_registry_epilogue = '\
  }\n\
}\n'

FE_PATH = os.path.expandvars(
  "$IMPALA_HOME/fe/generated-sources/gen-java/org/apache/impala/builtins/")

# This contains all the metadata to describe all the builtins.
# Each meta data entry is itself a map to store all the meta data
#   - fn_name, ret_type, args, symbol, sql_names
meta_data_entries = []


# Read in the function and add it to the meta_data_entries map
def add_function(fn_meta_data, user_visible):
  assert 4 <= len(fn_meta_data) <= 6, \
         "Invalid function entry in impala_functions.py:\n\t" + repr(fn_meta_data)
  entry = {}
  entry["sql_names"] = fn_meta_data[0]
  entry["ret_type"] = fn_meta_data[1]
  entry["args"] = fn_meta_data[2]
  entry["symbol"] = fn_meta_data[3]
  if len(fn_meta_data) >= 5:
    entry["prepare"] = fn_meta_data[4]
  if len(fn_meta_data) >= 6:
    entry["close"] = fn_meta_data[5]
  entry["user_visible"] = user_visible
  meta_data_entries.append(entry)


def generate_fe_entry(entry, name):
  java_output = ""
  java_output += "\"" + name + "\""
  java_output += ", \"" + entry["symbol"] + "\""
  if entry["user_visible"]:
    java_output += ", true"
  else:
    java_output += ", false"

  if 'prepare' in entry:
    java_output += ', "%s"' % entry["prepare"]
    if 'close' in entry:
      java_output += ', "%s"' % entry["close"]
    else:
      java_output += ', null'

  # Check the last entry for varargs indicator.
  if entry["args"] and entry["args"][-1] == "...":
    entry["args"].pop()
    java_output += ", true"
  else:
    java_output += ", false"

  java_output += ", Type." + entry["ret_type"]
  for arg in entry["args"]:
    java_output += ", Type." + arg
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


if __name__ == "__main__":
  # Read the function metadata inputs
  for function in impala_functions.visible_functions:
    add_function(function, True)
  for function in impala_functions.invisible_functions:
    add_function(function, False)

  if not os.path.exists(FE_PATH):
    os.makedirs(FE_PATH)

  generate_fe_registry_init(FE_PATH + "ScalarBuiltins.java")
