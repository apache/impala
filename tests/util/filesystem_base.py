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
# Filsystem access abstraction

from __future__ import absolute_import, division, print_function
from abc import ABCMeta, abstractmethod
from future.utils import with_metaclass


class BaseFilesystem(with_metaclass(ABCMeta, object)):

  @abstractmethod
  def create_file(self, path, file_data, overwrite):
    """Create a file in 'path' and populate with the string 'file_data'. If overwrite is
    True, the file is overwritten. Returns True if successful, False if the file already
    exists and throws an exception otherwise"""
    pass

  @abstractmethod
  def make_dir(self, path, permission):
    """Create a directory in 'path' with octal umask 'permission'.
    Returns True if successful and throws an exception otherwise"""
    pass

  @abstractmethod
  def copy(self, src, dst, overwrite):
    """Copy a file from 'src' to 'dst'. Throws an exception if unsuccessful."""
    pass

  @abstractmethod
  def copy_from_local(self, src, dst):
    """Copies a file from 'src' file on the local filesystem to the 'dst', which can be
    on any HDFS compatible filesystem. Fails if the src file is not on the local
    filesystem. Throws an exception if unsuccessful."""
    pass

  @abstractmethod
  def ls(self, path):
    """Return a list of all files/dirs/keys in path. Throws an exception if path
    is invalid."""
    pass

  @abstractmethod
  def exists(self, path):
    """Returns True if a particular path exists, else it returns False."""
    pass

  @abstractmethod
  def delete_file_dir(self, path, recursive):
    """Delete all files/dirs/keys in a path. Returns True if successful or if the file
    does not exist. Throws an exception otherwise."""
    pass

  @abstractmethod
  def get_all_file_sizes(self, path):
    """Returns a list of integers which are all the file sizes of files found under
    'path'."""
    pass

  @abstractmethod
  def touch(self, paths):
    """Updates the access and modification times of the files specified by 'paths' to
    the current time. If the files don't exist, zero length files will be created with
    current time as the timestamp of them."""
    pass
