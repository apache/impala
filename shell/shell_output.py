#!/usr/bin/env ambari-python-wrap
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


def match_string_type(str_to_convert, reference_str):
  """ Returns 'str_to_convert' converted to the same type as 'reference_str'.
      Can handle only str and unicode. NOOP in Python 3.
  """
  if sys.version_info.major >= 3:
    assert isinstance(reference_str, str)
    assert isinstance(str_to_convert, str)
    return str_to_convert

  if type(str_to_convert) == type(reference_str):
    return str_to_convert

  if isinstance(reference_str, str):
    assert isinstance(str_to_convert, unicode)
    return str_to_convert.encode('UTF-8')
  else:
    assert isinstance(reference_str, str)
    return str_to_convert.decode('UTF-8')


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
        # csv.writer in python2 requires an ascii string delimiter
        self.field_delim = field_delim.decode('unicode_escape').encode('ascii', 'ignore')
      # IMPALA-8652, the delimiter should be a 1-character string and verified already
      assert len(self.field_delim) == 1

  def format(self, rows):
    """Returns string containing UTF-8-encoded representation of the table data."""
    # csv.writer expects a file handle to the input.
    temp_buffer = StringIO()
    writer = csv.writer(temp_buffer, delimiter=self.field_delim,
                        lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    for row in rows:
      if sys.version_info.major == 2:
        row = [val.encode('utf-8', 'replace') if isinstance(val, unicode) else val
            for val in row]
      writer.writerow(row)
    # The CSV writer produces an extra newline. Strip that extra newline (and
    # only that extra newline). csv wraps newlines for data values in quotes,
    # so rstrip will be limited to the extra newline.
    if sys.version_info.major == 2:
      # Python 2 is in encoded Unicode bytes, so this needs to be a bytes \n.
      rows = temp_buffer.getvalue().rstrip(b'\n')
    else:
      rows = temp_buffer.getvalue().rstrip('\n')
    temp_buffer.close()
    return rows


class VerticalOutputFormatter(DelimitedOutputFormatter):
  def __init__(self, column_names):
    DelimitedOutputFormatter.__init__(self, field_delim="\n")
    self.column_names = column_names
    self.column_name_max_len = max([len(s) for s in column_names])

  def format(self, rows):
    """Returns string containing UTF-8-encoded representation of the table data."""
    # csv.writer expects a file handle to the input.
    temp_buffer = StringIO()
    writer = csv.writer(temp_buffer, delimiter=self.field_delim,
                        lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    for r, row in enumerate(rows):
      if sys.version_info.major == 2:
        row = [val.encode('utf-8', 'replace') if isinstance(val, unicode) else val
            for val in row]
      writer.writerow(["************************************** " +
        str(r + 1) + ".row **************************************"])
      for c, val in enumerate(row):
        row[c] = self.column_names[c].rjust(self.column_name_max_len) + ": " + val
      writer.writerow(row)
    # The CSV writer produces an extra newline. Strip that extra newline (and
    # only that extra newline). csv wraps newlines for data values in quotes,
    # so rstrip will be limited to the extra newline.
    if sys.version_info.major == 2:
      # Python 2 is in encoded Unicode bytes, so this needs to be a bytes \n.
      rows = temp_buffer.getvalue().rstrip(b'\n')
    else:
      rows = temp_buffer.getvalue().rstrip('\n')
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
          # The file is opened in binary mode. Python 2 returns Unicode bytes
          # that can be written directly. Python 3 returns a string, which
          # we need to encode before writing.
          # TODO: Reexamine the contract of the format() function and see if
          # we can remove this.
          if sys.version_info.major == 2 and isinstance(formatted_data, str):
            out_file.write(formatted_data)
          else:
            out_file.write(formatted_data.encode('utf-8'))
          out_file.write(b'\n')
      except IOError as err:
        file_err_msg = "Error opening file %s: %s" % (self.filename, str(err))
        print('{0} (falling back to stderr)'.format(file_err_msg), file=sys.stderr)
        print(formatted_data, file=sys.stderr)
    else:
      # If filename is None, then just print to stdout
      print(formatted_data)

  def flush(self):
    # When outputing to a file, the file is currently closed with each write,
    # so the flush doesn't need to do anything.
    if self.filename is None:
      sys.stdout.flush()


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
