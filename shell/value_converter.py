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

from TCLIService.TCLIService import TTypeId

import sys


class ValueConverter(object):

  def get_converter(value):
      pass

  def override_floating_point_converter(format_specification):
      pass


class HS2ValueConverter(ValueConverter):

  def __get_binary_converter(self):
      if sys.version_info.major < 3:
          return str
      return lambda s: s.decode(errors='replace')

  def __init__(self):
      self.value_converters = {
          TTypeId.BOOLEAN_TYPE: lambda b: 'true' if b else 'false',
          TTypeId.TINYINT_TYPE: str,
          TTypeId.SMALLINT_TYPE: str,
          TTypeId.INT_TYPE: str,
          TTypeId.BIGINT_TYPE: str,
          TTypeId.BINARY_TYPE: self.__get_binary_converter(),
          TTypeId.FLOAT_TYPE: str,
          TTypeId.DOUBLE_TYPE: str
      }

  def get_converter(self, value):
      return self.value_converters.get(value, None)

  def override_floating_point_converter(self, format_specification):
      def convert(value):
          return ('{:%s}' % format_specification).format(value)
      self.value_converters[TTypeId.FLOAT_TYPE] = convert
      self.value_converters[TTypeId.DOUBLE_TYPE] = convert
