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
# ADLS access utilities
#
# This file uses the azure-data-lake-store-python client and provides simple
# functions to the Impala test suite to access Azure Data Lake Store.

from __future__ import absolute_import, division, print_function
from azure.datalake.store import core, lib, multithread, exceptions
from tests.util.filesystem_base import BaseFilesystem
from tests.util.filesystem_utils import ADLS_CLIENT_ID, ADLS_TENANT_ID, ADLS_CLIENT_SECRET
from tests.util.hdfs_util import HadoopFsCommandLineClient

class ADLSClient(BaseFilesystem):

  def __init__(self, store):
    self.token = lib.auth(tenant_id = ADLS_TENANT_ID,
                          client_secret = ADLS_CLIENT_SECRET,
                          client_id = ADLS_CLIENT_ID)
    self.adlsclient = core.AzureDLFileSystem(self.token, store_name=store)
    self.adls_cli_client = HadoopFsCommandLineClient("ADLS")

  def create_file(self, path, file_data, overwrite=True):
    if not overwrite and self.exists(path): return False
    with self.adlsclient.open(path, 'wb') as f:
      num_bytes = f.write(file_data)
      assert num_bytes == len(file_data), "ADLS write failed."
    return True

  def make_dir(self, path, permission=None):
    self.adlsclient.mkdir(path)
    return True

  def copy(self, src, dst, overwrite=True):
    self.adls_cli_client.copy(src, dst, overwrite)

  def copy_from_local(self, src, dst):
    self.adls_cli_client.copy_from_local(src, dst)

  def ls(self, path):
    file_paths = self.adlsclient.ls(path)
    files= []
    for f in file_paths:
      fname = f.split("/")[-1]
      if not fname == '':
        files += [fname]
    return files

  def exists(self, path):
    return self.adlsclient.exists(path)

  def delete_file_dir(self, path, recursive=False):
    try:
      self.adlsclient.rm(path, recursive)
    except exceptions.FileNotFoundError as e:
      return False
    return True

  def get_all_file_sizes(self, path):
    """Returns a list of integers which are all the file sizes of files found under
    'path'."""
    return [self.adlsclient.info(f)['length'] for f in self.adlsclient.ls(path) \
        if self.adlsclient.info(f)['type'] == 'FILE']
