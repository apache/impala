#!/usr/bin/env ambari-python-wrap
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

# This script is to be called by Cloudera Manager to collect Breakpad minidump files up to
# a specified date/time. A compressed tarball is created in the user specified location.
# We try to fit as many files as possible into the tarball until a size limit is reached.
# Example invokation by CM to:
#     ./collect_minidumps.py --conf_dir=/var/run/.../5555-impala-STATESTORE/impala-conf \
#       --role_name=statestored --max_output_size=50000000 --end_time=1463033495000 \
#       --output_file_path=/tmp/minidump_package.tar.gz

from __future__ import absolute_import, division, print_function
import os
import re
import sys
import tarfile

from contextlib import closing
from optparse import OptionParser

class FileArchiver(object):
  '''This is a generic class that makes a tarball out of files in the source_dir.  We
  assume that source_dir contains only files. The resulting file will be compressed with
  gzip and placed into output_file_path. If a file with that name already exists, it will
  be deleted and re-created. Max_result_size is the maximum allowed size of the resulting
  tarball. If all files in the source_dir can't fit into the allowed size, most recent
  files will be preferred. start_time and end_time paramenters (in milliseconds UTC) allow
  us to specify an interval of time for which to consider the files.
  '''

  def __init__(self,
      source_dir,
      output_file_path,
      max_output_size,
      start_time=None,
      end_time=None):
    self.source_dir = source_dir
    self.max_output_size = max_output_size
    self.start_time = start_time
    self.end_time = end_time
    self.output_file_path = output_file_path
    # Maps the number of files in the tarball to the resulting size (in bytes).
    self.resulting_sizes = {}
    self.file_list = []

  def _remove_output_file(self):
    try:
      os.remove(self.output_file_path)
    except OSError:
      pass

  def _tar_files(self, num_files=None):
    '''Make a tarball with num_files most recent files in the file_list. Record the
    resulting size into resulting_sizes map and return it.
    '''
    num_files = num_files or len(self.file_list)
    self._remove_output_file()
    if num_files == 0:
      size = 0
    else:
      with closing(tarfile.open(self.output_file_path, mode='w:gz')) as out:
        for i in range(num_files):
          out.add(self.file_list[i])
      size = os.stat(self.output_file_path).st_size
    self.resulting_sizes[num_files] = size
    return size

  def _compute_file_list(self):
    '''Computes a sorted list of eligible files in the source directory by filtering out
    files with modified date not in the desired time range. Directories and other
    non-files are ignored.
    '''
    file_list = []
    for f in os.listdir(self.source_dir):
      full_path = os.path.join(self.source_dir, f)
      if not os.path.isfile(full_path):
        continue
      # st_mtime is in seconds UTC, so we need to multiply by 1000 to get milliseconds.
      time_modified = os.stat(full_path).st_mtime * 1000
      if self.start_time and self.start_time > time_modified:
        continue
      if self.end_time and self.end_time < time_modified:
        continue
      file_list.append(full_path)
    self.file_list = sorted(file_list, key=lambda f: os.stat(f).st_mtime, reverse=True)

  def _binary_search(self):
    '''Calculates the maximum number of files that can be collected, such that the tarball
    size is less than max_output_size.
    '''
    min_num = 0
    max_num = len(self.file_list)
    while max_num - min_num > 1:
      mid = (min_num + max_num) // 2
      if self._tar_files(mid) <= self.max_output_size:
        min_num = mid
      else:
        max_num = mid
    return min_num

  def make_tarball(self):
    '''Make a tarball with the maximum number of files such that the size of the tarball
    is less than or equal to max_output_size. Returns a pair (status (int), message
    (str)). status represents the result of the operation and follows the unix convention
    where 0 equals success. message provides additional information. A status of 1 is
    returned if source_dir is not empty and no files were able to fit into the tarball.
    '''
    self._compute_file_list()
    if len(self.file_list) == 0:
      status = 0
      msg = 'No files found in "{0}".'
      return status, msg.format(self.source_dir)
    output_size = self._tar_files()
    if output_size <= self.max_output_size:
      status = 0
      msg = 'Success, archived all {0} files in "{1}".'
      return status, msg.format(len(self.file_list), self.source_dir)
    else:
      max_num_files = self._binary_search()
      if max_num_files == 0:
        self._remove_output_file()
        status = 1
        msg = ('Unable to archive any files in "{0}". '
            'Increase max_output_size to at least {1} bytes.')
        # If max_num_files is 0, we are guaranteed that the binary search tried making a
        # tarball with 1 file.
        return status, msg.format(self.source_dir, self.resulting_sizes[1])
      else:
        self._tar_files(max_num_files)
        status = 0
        msg = 'Success. Archived {0} out of {1} files in "{2}".'
        return status, msg.format(max_num_files, len(self.file_list), self.source_dir)

def get_config_parameter_value(conf_dir, role_name, config_parameter_name):
  '''Extract a single config parameter from the configuration file of a particular
  daemon.
  '''
  ROLE_FLAGFILE_MAP = {
      'impalad': 'impalad_flags',
      'statestored': 'state_store_flags',
      'catalogd': 'catalogserver_flags'}
  config_parameter_value = None
  try:
    file_path = os.path.join(conf_dir, ROLE_FLAGFILE_MAP[role_name])
    with open(file_path, 'r') as f:
      for line in f:
        m = re.match('-{0}=(.*)'.format(config_parameter_name), line)
        if m:
          config_parameter_value = m.group(1)
  except IOError as e:
    print('Error: Unable to open "{0}".'.format(file_path), file=sys.stderr)
    sys.exit(1)
  return config_parameter_value

def get_minidump_dir(conf_dir, role_name):
  '''Extracts the minidump directory path for a given role from the configuration file.
  The directory defaults to 'minidumps', relative paths are prepended with log_dir, which
  defaults to '/tmp'.
  '''
  minidump_path = get_config_parameter_value(
    conf_dir, role_name, 'minidump_path') or 'minidumps'
  if not os.path.isabs(minidump_path):
    log_dir = get_config_parameter_value(conf_dir, role_name, 'log_dir') or '/tmp'
    minidump_path = os.path.join(log_dir, minidump_path)
  result = os.path.join(minidump_path, role_name)
  if not os.path.isdir(result):
    msg = 'Error: minidump directory does not exist.'
    print(msg, file=sys.stderr)
    sys.exit(1)
  return result

def main():
  parser = OptionParser()
  parser.add_option('--conf_dir',
      help='Directory in which to look for the config file with startup flags')
  parser.add_option('--role_name', type='choice',
      choices=['impalad', 'statestored', 'catalogd'], default='impalad',
      help='For which role to collect the minidumps.')
  parser.add_option('--max_output_size', default=40*1024*1024, type='int',
      help='The maximum file size of the result tarball to be written given in bytes. '
           'If the total size exceeds this value, most recent files will be preferred')
  parser.add_option('--start_time', default=None, type='int',
      help='Interval start time (in epoch milliseconds UTC).')
  parser.add_option('--end_time', default=None, type='int',
      help='Interval end time, until when to collect the minidump files '
           '(in epoch milliseconds UTC).')
  parser.add_option('--output_file_path', help='The full path of the output file.')
  options, args = parser.parse_args()
  if not options.conf_dir:
    msg = 'Error: conf_dir is not specified.'
    print(msg, file=sys.stderr)
    sys.exit(1)
  if not options.output_file_path:
    msg = 'Error: output_file_path is not specified.'
    print(msg, file=sys.stderr)
    sys.exit(1)

  minidump_dir = get_minidump_dir(options.conf_dir, options.role_name)
  file_archiver = FileArchiver(source_dir=minidump_dir,
      max_output_size=options.max_output_size,
      start_time=options.start_time,
      end_time=options.end_time,
      output_file_path=options.output_file_path)
  status, msg = file_archiver.make_tarball()
  print(msg, file=sys.stderr)
  sys.exit(status)

if __name__ == '__main__':
  main()
