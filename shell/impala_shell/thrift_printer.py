#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

import re
from sys import stdout


class ThriftPrettyPrinter(object):
  """Implements a pretty printer for Thrift objects.
  Generates string representations.  Does not output
  to stdout, stderr, or any other file handles."""

  # Inputs:
  #   redacted_fields - list of names of object attributes whose
  #                     values will not be printed out
  #   indent          - string containing only spaces, used as the
  #                     base indentation where all other indentations
  #                     will be multiples of this string
  #   objects_to_skip - list of names of objects attributes that
  #                     will not be printed out, useful to ignore
  #                     duplicate information such as query results
  def __init__(self,
               redacted_fields=("secret", "password"),
               indent="  ",
               objects_to_skip=("TRowSet", "TGetRuntimeProfileResp")):
    if redacted_fields is not None:
      assert type(redacted_fields) is list or \
             type(redacted_fields) is tuple, \
             "redacted_fields must be either a list or a tuple"
    self.base_indent = indent
    self.redacted_fields = redacted_fields
    self.objects_to_skip = objects_to_skip

    self._objname_re = re.compile("^.*?'(.*?)'.*?$")

  def print_obj(self, thrift_obj, file_handle=stdout):
    """Prints the provided 'thrift_obj' to the provided
    file handle.  If no file handle is specified, then
    stdout will be used.  The 'file_handle' object must
    have a write(string) method.

    While this class is specifically targeted to printing
    Thrift objects, there is no technical limitation preventing
    any other type of object from being printed.  However, the
    output of non-Thrift objects may not be as nicely formatted."""

    # Inputs:
    #   thrift_obj  - the object to print out, its attributes will
    #                 be walked recursively through the entire
    #                 object structure and printed out
    #   file_handle - where the object will be written, defaults to stdout
    #                 but can be any object with a write(str) method
    self._internal_print(thrift_obj, self.base_indent, file_handle)

  def _internal_print(self, thrift_obj, indent, file_handle):
    """Recursive function that does the work of walking and printing
    an object."""
    # parse out the type name of the thrift object
    obj_name = self._objname_re.match(str(type(thrift_obj))) \
               .group(1).split(".")[-1]
    file_handle.write("<{0}>".format(obj_name))

    if self.objects_to_skip.count(obj_name):
      file_handle.write(" - <skipping>\n")
      return

    indent = "{0}{1}".format(indent, self.base_indent)
    file_handle.write("\n")

    if obj_name == "list" or obj_name == "tuple":
      # lists and tuples have to be handled differently
      # because the vars function does not operate on them
      for attr_val in thrift_obj:
        file_handle.write(indent)
        self._internal_print(attr_val, indent, file_handle)
    else:
      # print out simple types first before printing out objects
      # this ensures the simple types are easier to see
      child_simple_attrs = {}
      child_objs = {}
      for attr_name in vars(thrift_obj):
        attr_val = getattr(thrift_obj, attr_name)
        if (hasattr(attr_val, '__dict__')
            or attr_val is list
            or attr_val is tuple):
          child_objs[attr_name] = attr_val
        else:
          child_simple_attrs[attr_name] = attr_val

      # print out child attributes in alphabetical order
      for child_attr_name in sorted(child_simple_attrs):
        self._print_attr(child_attr_name,
                         child_simple_attrs[child_attr_name],
                         indent,
                         file_handle)

      # print out complex types objects, lists, or tuples
      # in alphabetical order
      for attr_name in sorted(child_objs):
        self._print_attr(attr_name,
                         child_objs[attr_name],
                         indent,
                         file_handle)

  def _print_attr(self, attr_name, attr_val, indent, file_handle):
    """Handles a single object attribute by either printing out
    its name/value (for simple types) or recursing down into the
    object (for objects/lists/tuples)."""
    file_handle.write(indent)

    if attr_val is not None and self.redacted_fields.count(attr_name) > 0:
      file_handle.write("- {0}: *******\n".format(attr_name))
    elif attr_val is None:
      file_handle.write("- {0}: <None>\n".format(attr_name))
    elif type(attr_val) is list or type(attr_val) is tuple:
      file_handle.write("[")
      self._internal_print(attr_val, indent, file_handle)
      file_handle.write("{0}]\n".format(indent))
    elif hasattr(attr_val, '__dict__'):
      indent += "{0:{1}}  {2}".format("", len(attr_name), self.base_indent)
      file_handle.write("- {0}: ".format(attr_name))
      self._internal_print(attr_val, indent, file_handle)
    else:
      file_handle.write("- {0}: ".format(attr_name))
      try:
        str(attr_val).decode("ascii")
        file_handle.write("{0}".format(attr_val))
      except UnicodeDecodeError:
        # python2 - string contains binary data
        file_handle.write("<binary data>")
      except AttributeError:
        # python3 - does not require decoding strings and thus falls into this code
        if isinstance(attr_val, bytes):
          file_handle.write("<binary data>")
        else:
          file_handle.write("{0}".format(attr_val))
      file_handle.write("\n")
