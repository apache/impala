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
# Hdfs access utilities

import getpass
import httplib
import os.path
import re
import requests
import subprocess
import tempfile
from os import environ
from pywebhdfs.webhdfs import PyWebHdfsClient, errors, _raise_pywebhdfs_exception
from xml.etree.ElementTree import parse

from tests.util.filesystem_base import BaseFilesystem
from tests.util.filesystem_utils import FILESYSTEM_PREFIX


class HdfsConfig(object):
  """Reads an XML configuration file (produced by a mini-cluster) into a dictionary
  accessible via get()"""
  def __init__(self, *filename):
    self.conf = {}
    for arg in filename:
      tree = parse(arg)
      for property in tree.getroot().getiterator('property'):
        self.conf[property.find('name').text] = property.find('value').text

  def get(self, key):
    return self.conf.get(key)


# Configuration object for the configuration that the minicluster will use.
CORE_CONF = HdfsConfig(os.path.join(environ['HADOOP_CONF_DIR'], "core-site.xml"))
# NAMENODE is the path prefix that should be used in results, since paths that come
# out of Impala have been qualified.  When running against the default filesystem,
# this will be the same as fs.defaultFS.  When running against a secondary filesystem,
# this will be the same as FILESYSTEM_PREFIX.
NAMENODE = FILESYSTEM_PREFIX or CORE_CONF.get('fs.defaultFS')


class PyWebHdfsClientWithChmod(PyWebHdfsClient, BaseFilesystem):
  def chmod(self, path, permission):
    """Set the permission of 'path' to 'permission' (specified as an octal string, e.g.
    '775'"""
    uri = self._create_uri(path, "SETPERMISSION", permission=permission)
    response = requests.put(uri, allow_redirects=True)
    if not response.status_code == httplib.OK:
      _raise_pywebhdfs_exception(response.status_code, response.text)
    return True

  def chown(self, path, user, group):
    """Sets the owner and the group of 'path to 'user' / 'group'"""
    uri = self._create_uri(path, "SETOWNER", owner=user, group=group)
    response = requests.put(uri, allow_redirects=True)
    if not response.status_code == httplib.OK:
      _raise_pywebhdfs_exception(response.status_code, response.text)
    return True

  def setacl(self, path, acls):
    uri = self._create_uri(path, "SETACL", aclspec=acls)
    response = requests.put(uri, allow_redirects=True)
    if not response.status_code == httplib.OK:
      _raise_pywebhdfs_exception(response.status_code, response.text)
    return True

  def getacl(self, path):
    uri = self._create_uri(path, "GETACLSTATUS")
    response = requests.get(uri, allow_redirects=True)
    if not response.status_code == httplib.OK:
      _raise_pywebhdfs_exception(response.status_code, response.text)
    return response.json()

  def delete_file_dir(self, path, recursive=False):
    """Deletes a file or a dir if it exists.

    Overrides the superclass's method by providing delete if exists semantics. This takes
    the burden of stat'ing the file away from the caller.
    """
    if not self.exists(path):
      return True
    return super(PyWebHdfsClientWithChmod, self).delete_file_dir(path,
        recursive=recursive)

  def get_file_dir_status(self, path):
    """Stats a file or a directoty in hdfs

    The superclass expects paths without the leading '/'. This method strips it from the
    path to make usage transparent to the caller.
    """
    path = path.lstrip('/')
    return super(PyWebHdfsClientWithChmod, self).get_file_dir_status(path)

  def get_all_file_sizes(self, path):
    """Returns a list of all file sizes in the path"""
    sizes = []
    for status in self.list_dir(path).get('FileStatuses').get('FileStatus'):
      if status['type'] == 'FILE':
        sizes += [status['length']]
    return sizes

  def copy(self, src, dest):
    """Copies a file in hdfs from src to destination

    Simulates hdfs dfs (or hadoop fs) -cp <src> <dst>. Does not resolve all the files if
    the source or destination is a directory. Files need to be explicitly copied.
    TODO: Infer whether the source or destination is a directory and do this implicitly.
    TODO: Take care of larger files by always reading/writing them in small chunks.
    """
    assert self.get_file_dir_status(src)
    # Get the data
    data = self.read_file(src)
    # Copy the data
    self.create_file(dest, data)
    assert self.get_file_dir_status(dest)
    assert self.read_file(dest) == data

  def ls(self, path):
    """Returns a list of all file and directory names in 'path'"""
    # list_dir() returns a dictionary of file statues. This function picks out the
    # file and directory names and just returns a list of the names.
    file_infos = self.list_dir(path).get('FileStatuses').get('FileStatus')
    return [info.get('pathSuffix') for info in file_infos]

  def exists(self, path):
    """Checks if a particular path exists"""
    try:
      self.get_file_dir_status(path)
    except errors.FileNotFound:
      return False
    return True


class HadoopFsCommandLineClient(BaseFilesystem):
  """This client is a wrapper around the hadoop fs command line. This is useful for
  filesystems that rely on the logic in the Hadoop connector. For example, S3 with
  S3Guard needs all accesses to go through the S3 connector. This is also useful for
  filesystems that are fully served by this limited set of functionality (ABFS uses
  this).
  """

  def __init__(self, filesystem_type="HDFS"):
    # The filesystem_type is used only for providing more specific error messages.
    self.filesystem_type = filesystem_type
    super(HadoopFsCommandLineClient, self).__init__()

  def _hadoop_fs_shell(self, command):
    """Helper function wrapper around 'hadoop fs' takes in the arguments as a list."""
    hadoop_command = ['hadoop', 'fs'] + command
    process = subprocess.Popen(hadoop_command,
          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    status = process.returncode
    return (status, stdout, stderr)

  def create_file(self, path, file_data, overwrite=True):
    """Creates a temporary file with the specified file_data on the local filesystem,
    then puts it into the specified path."""
    fixed_path = self._normalize_path(path)
    if not overwrite and self.exists(fixed_path): return False
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
      tmp_file.write(file_data)
    (status, stdout, stderr) = \
        self._hadoop_fs_shell(['-put', tmp_file.name, fixed_path])
    return status == 0

  def make_dir(self, path, permission=None):
    """Create a directory at the specified path. Permissions are not supported."""
    fixed_path = self._normalize_path(path)
    (status, stdout, stderr) = self._hadoop_fs_shell(['-mkdir', '-p', fixed_path])
    return status == 0

  def copy(self, src, dst):
    """Copy the source file to the destination."""
    fixed_src = self._normalize_path(src)
    fixed_dst = self._normalize_path(dst)
    (status, stdout, stderr) = \
        self._hadoop_fs_shell(['-cp', fixed_src, fixed_dst])
    assert status == 0, \
        '{0} copy failed: '.format(self.filesystem_type) + stderr + "; " + stdout
    assert self.exists(dst), \
        '{fs_type} copy failed: Destination file {dst} does not exist'\
            .format(fs_type=self.filesystem_type, dst=dst)

  def _inner_ls(self, path):
    """List names, lengths, and mode for files/directories under the specified path."""
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
    """Returns a list of all file and directory names in 'path'"""
    fixed_path = self._normalize_path(path)
    files = []
    for f in self._inner_ls(fixed_path):
      fname = os.path.basename(f['name'])
      if not fname == '':
        files += [fname]
    return files

  def exists(self, path):
    """Checks if a particular path exists"""
    fixed_path = self._normalize_path(path)
    (status, stdout, stderr) = self._hadoop_fs_shell(['-test', '-e', fixed_path])
    return status == 0

  def delete_file_dir(self, path, recursive=False):
    """Delete the file or directory given by the specified path. Recursive must be true
    for directories."""
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
    """Paths passed in may lack a leading slash. This adds a leading slash if it is
    missing."""
    return path if path.startswith('/') else '/' + path


def get_hdfs_client_from_conf(conf):
  """Returns a new HTTP client for an HDFS cluster using an HdfsConfig object"""
  hostport = conf.get('dfs.namenode.http-address')
  if hostport is None:
    raise Exception("dfs.namenode.http-address not found in config")
  host, port = hostport.split(":")
  return get_hdfs_client(host=host, port=port)


def get_hdfs_client(host, port, user_name=getpass.getuser()):
  """Returns a new HTTP client for an HDFS cluster using an explict host:port pair"""
  return PyWebHdfsClientWithChmod(host=host, port=port, user_name=user_name)


def get_default_hdfs_config():
  core_site_path = os.path.join(environ.get('HADOOP_CONF_DIR'), 'core-site.xml')
  hdfs_site_path = os.path.join(environ.get('HADOOP_CONF_DIR'), 'hdfs-site.xml')
  return HdfsConfig(core_site_path, hdfs_site_path)


def create_default_hdfs_client():
  return get_hdfs_client_from_conf(get_default_hdfs_config())
