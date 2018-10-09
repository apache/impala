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
# ABFS access utilities
#
# This file uses the Hadoop CLI to provide simple functions to the Impala test
# suite to whatever the default filesystem is

import re
import subprocess
import tempfile

from tests.util.filesystem_base import BaseFilesystem


class ABFSClient(BaseFilesystem):

  def _hadoop_fs_shell(self, command):
    hadoop_command = ['hadoop', 'fs'] + command
    process = subprocess.Popen(hadoop_command,
          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    status = process.returncode
    return (status, stdout, stderr)

  def create_file(self, path, file_data, overwrite=True):
    fixed_path = self._normalize_path(path)
    if not overwrite and self.exists(fixed_path): return False
    f = tempfile.NamedTemporaryFile(delete=False)
    tmp_path = f.name
    f.write(file_data)
    f.close()
    (status, stdout, stderr) = \
        self._hadoop_fs_shell(['-put', tmp_path, fixed_path])
    return status == 0

  def make_dir(self, path, permission=None):
    fixed_path = self._normalize_path(path)
    self._hadoop_fs_shell(['-mkdir', '-p', fixed_path])
    return True

  def copy(self, src, dst):
    fixed_src = self._normalize_path(src)
    fixed_dst = self._normalize_path(dst)
    (status, stdout, stderr) = \
        self._hadoop_fs_shell(['-cp', fixed_src, fixed_dst])
    assert status == 0, \
        'ABFS copy failed: ' + stderr + "; " + stdout
    assert self.exists(dst), \
        'ABFS copy failed: Destination file {dst} does not exist'\
            .format(dst=dst)

  def _inner_ls(self, path):
    fixed_path = self._normalize_path(path)
    (status, stdout, stderr) = self._hadoop_fs_shell(['-ls', fixed_path])
    # Trim the "Found X items" line and trailing new-line
    entries = stdout.split("\n")[1:-1]
    files = []
    for entry in entries:
      fields = re.split(" +", entry)
      files.append({
        'name': fields[7],
        'length': int(fields[4]),
        'mode': fields[0]
      })
    return files

  def ls(self, path):
    fixed_path = self._normalize_path(path)
    files = []
    for f in self._inner_ls(fixed_path):
      fname = f['name'].split("/")[-1]
      if not fname == '':
        files += [fname]
    return files

  def exists(self, path):
    fixed_path = self._normalize_path(path)
    (status, stdout, stderr) = self._hadoop_fs_shell(['-test', '-e', fixed_path])
    return status == 0

  def delete_file_dir(self, path, recursive=False):
    fixed_path = self._normalize_path(path)
    rm_command = ['-rm', fixed_path]
    if recursive:
      rm_command = ['-rm', '-r', fixed_path]
    (status, stdout, stderr) = self._hadoop_fs_shell(rm_command)
    return status == 0

  def get_all_file_sizes(self, path):
    """Returns a list of integers which are all the file sizes of files found
    under 'path'."""
    fixed_path = self._normalize_path(path)
    return [f['length'] for f in
        self._inner_ls(fixed_path) if f['mode'][0] == "-"]

  def _normalize_path(self, path):
    # Paths passed in may lack a leading slash
    return path if path.startswith('/') else '/' + path
