#!/usr/bin/env python
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


# Java wrapper class generator for Hive ESRI varargs UDFs to bridge the varargs support
# in Impala. A generated class is extending the original UDF and adding wrapper
# 'evaluate' methods projecting the varargs method as an n parameter method.

from __future__ import absolute_import, division, print_function
import os
from gen_builtins_catalog import FE_PATH

LICENSE = """
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This is a generated file, DO NOT EDIT.
// To add new functions, see the generator at
// common/function-registry/gen_geospatial_builtins_wrappers.py"""


ARGUMENT_EXCEPTION = "UDFArgumentException"
ARGUMENT_LENGTH_EXCEPTION = "UDFArgumentLengthException"
UDF_PACKAGE = "org.apache.hadoop.hive.ql.udf.esri"
DOUBLE_TYPE = "org.apache.hadoop.hive.serde2.io.DoubleWritable"
BYTE_TYPE = "org.apache.hadoop.io.BytesWritable"


class Wrapper():

  METHOD_FORMAT = ("""  public {return_type} evaluate({parameter_list}) {exception_clause}{{
    return super.evaluate({argument_list});
  }}""")

  CLASS_FORMAT = """public class {class_name} extends {base_class} {{
{methods}
}}"""

  FILE_FORMAT = """{license}

package {package};

{wrapper_class}"""

  EXCEPTION_CLAUSE_FORMAT = "throws org.apache.hadoop.hive.ql.exec.%s"

  def __init__(self, original_class, parameter_type, parameter_range, throws=None):
    self.original_class = original_class
    self.parameter_type = parameter_type
    self.throws = throws
    self.parameter_range = parameter_range
    # Return type is always BytesWritable with the current cases
    self.return_type = BYTE_TYPE

  def generate_parameter(self, num):
    return "{parameter_type} arg{num}".format(
      parameter_type=self.parameter_type, num=num
    )

  def generate_argument(self, num):
    return "arg%d" % num

  def generate_argument_list(self, num):
    arguments = list()
    for i in range(num):
      arguments.append(self.generate_argument(i))
    return ", ".join(arguments)

  def generate_parameter_list(self, num):
    parameters = list()
    for i in range(num):
      parameters.append(self.generate_parameter(i))
    return ", ".join(parameters)

  def generate_method(self, num):
    exception_clause = ""
    if self.throws:
      exception_clause = self.EXCEPTION_CLAUSE_FORMAT % self.throws
    return self.METHOD_FORMAT.format(
      return_type=self.return_type,
      parameter_list=self.generate_parameter_list(num),
      exception_clause=exception_clause,
      argument_list=self.generate_argument_list(num),
    )

  def generate_methods(self):
    methods = list()

    for i in self.parameter_range:
      methods.append(self.generate_method(i))

    return "\n\n".join(methods)

  def generate_wrapper_class(self):
    return self.CLASS_FORMAT.format(
      class_name=self.generate_wrapper_name(),
      base_class=self.original_class,
      methods=self.generate_methods()
    )

  def generate_file(self):
    return self.FILE_FORMAT.format(
      license=LICENSE,
      package="org.apache.impala.builtins",
      wrapper_class=self.generate_wrapper_class()
    )

  def generate_wrapper_name(self):
    return "%s_Wrapper" % self.original_class.split('.').pop()

  def get_filename(self):
    return "%s.java" % self.generate_wrapper_name()


WRAPPERS = [Wrapper("%s.ST_ConvexHull" % UDF_PACKAGE, BYTE_TYPE, list(range(2, 9, 1))),
            Wrapper("%s.ST_LineString" % UDF_PACKAGE, DOUBLE_TYPE, list(range(2, 15, 2)),
              ARGUMENT_EXCEPTION),
            Wrapper("%s.ST_MultiPoint" % UDF_PACKAGE, DOUBLE_TYPE, list(range(2, 15, 2)),
              ARGUMENT_LENGTH_EXCEPTION),
            Wrapper("%s.ST_Polygon" % UDF_PACKAGE, DOUBLE_TYPE, list(range(6, 15, 2)),
              ARGUMENT_LENGTH_EXCEPTION),
            Wrapper("%s.ST_Union" % UDF_PACKAGE, BYTE_TYPE, list(range(2, 15, 1)))]

if __name__ == "__main__":
  if not os.path.exists(FE_PATH):
    os.makedirs(FE_PATH)

  for wrapper_config in WRAPPERS:
    path = os.path.join(FE_PATH, wrapper_config.get_filename())
    wrapper_class_file = open(path, "w")
    wrapper_class_file.write(wrapper_config.generate_file())
    wrapper_class_file.close()
