#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from __future__ import print_function, unicode_literals

import csv
import re
import sys

try:
  from cStringIO import StringIO  # python 2
except ImportError:
  from io import StringIO  # python 3


class PrettyOutputFormatter(object):
  def __init__(self, prettytable):
    self.prettytable = prettytable

  def format(self, rows):
    """Returns string containing representation of the table data."""

    def decode_if_needed(row):
      if sys.version_info.major >= 3:
        return row
      # prettytable will decode with 'strict' if the string is not already unicode,
      # we should do it here to handle invalid characters.
      return [entry.decode('UTF-8', 'replace') if isinstance(entry, str) else entry
         for entry in row]

    # Clear rows that already exist in the table.
    self.prettytable.clear_rows()
    try:
      for row in rows:
        self.prettytable.add_row(decode_if_needed(row))
      return self.prettytable.get_string()
    except Exception as e:
      # beeswax returns each row as a tab separated string. If a string column
      # value in a row has tabs, it will break the row split. Default to displaying
      # raw results. This will change with a move to hiveserver2. Reference: IMPALA-116
      error_msg = ("Prettytable cannot resolve string columns values that have "
                   "embedded tabs. Reverting to tab delimited text output")
      print(error_msg, file=sys.stderr)
      print('{0}: {1}'.format(type(e), str(e)), file=sys.stderr)

      return '\n'.join(['\t'.join(decode_if_needed(row)) for row in rows])


class DelimitedOutputFormatter(object):
  def __init__(self, field_delim="\t"):
    if field_delim:
      if sys.version_info.major > 2:
        # strings do not have a 'decode' method in python 3
        field_delim_bytes = bytearray(field_delim, 'utf-8')
        self.field_delim = field_delim_bytes.decode('unicode_escape')
      else:
        self.field_delim = field_delim.decode('unicode_escape')
      # IMPALA-8652, the delimiter should be a 1-character string and verified already
      assert len(self.field_delim) == 1

  def format(self, rows):
    """Returns string containing UTF-8-encoded representation of the table data."""
    # csv.writer expects a file handle to the input.
    temp_buffer = StringIO()
    if sys.version_info.major == 2:
      # csv.writer in python2 requires an ascii string delimiter
      delim = self.field_delim.encode('ascii', 'ignore')
    else:
      delim = self.field_delim
    writer = csv.writer(temp_buffer, delimiter=delim,
                        lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    for row in rows:
      if sys.version_info.major == 2:
        row = [val.encode('utf-8', 'replace') if isinstance(val, unicode) else val
            for val in row]
      writer.writerow(row)
    rows = temp_buffer.getvalue().rstrip()
    temp_buffer.close()
    return rows


class OutputStream(object):
  def __init__(self, formatter, filename=None):
    """Helper class for writing query output.

    User should invoke the `write(data)` method of this object.
    `data` is a list of lists.
    """
    self.formatter = formatter
    self.filename = filename

  def write(self, data):
    formatted_data = self.formatter.format(data)
    if self.filename is not None:
      try:
        with open(self.filename, 'ab') as out_file:
          # Note that instances of this class do not persist, so it's fine to
          # close the we close the file handle after each write.
          out_file.write(formatted_data.encode('utf-8'))  # file opened in binary mode
          out_file.write(b'\n')
      except IOError as err:
        file_err_msg = "Error opening file %s: %s" % (self.filename, str(err))
        print('{0} (falling back to stderr)'.format(file_err_msg), file=sys.stderr)
        print(formatted_data, file=sys.stderr)
    else:
      # If filename is None, then just print to stdout
      print(formatted_data)


class OverwritingStdErrOutputStream(object):
  """This class is used to write output to stderr and overwrite the previous text as
  soon as new content needs to be written."""

  # ANSI Escape code for up.
  UP = "\x1b[A"

  def __init__(self):
    self.last_line_count = 0
    self.last_clean_text = ""

  def _clean_before(self):
    sys.stderr.write(self.UP * self.last_line_count)
    sys.stderr.write(self.last_clean_text)

  def write(self, data):
    """This method will erase the previously printed text on screen by going
    up as many new lines as the old text had and overwriting it with whitespace.
    Afterwards, the new text will be printed."""
    self._clean_before()
    new_line_count = data.count("\n")
    sys.stderr.write(self.UP * min(new_line_count, self.last_line_count))
    sys.stderr.write(data)

    # Cache the line count and the old text where all text was replaced by
    # whitespace.
    self.last_line_count = new_line_count
    self.last_clean_text = re.sub(r"[^\s]", " ", data)

  def clear(self):
    sys.stderr.write(self.UP * self.last_line_count)
    sys.stderr.write(self.last_clean_text)
    sys.stderr.write(self.UP * self.last_line_count)
    self.last_line_count = 0
    self.last_clean_text = ""
