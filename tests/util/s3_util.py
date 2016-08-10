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
# S3 access utilities
#
# This file uses the boto3 client and provides simple functions to the Impala test suite
# to access Amazon S3.

import boto3
from tests.util.filesystem_base import BaseFilesystem

class S3Client(BaseFilesystem):

  @classmethod
  def __init__(self, bucket):
    self.bucketname = bucket
    self.s3 = boto3.resource('s3')
    self.bucket = self.s3.Bucket(self.bucketname)
    self.s3client = boto3.client('s3')

  def create_file(self, path, file_data, overwrite=True):
    if not overwrite and self.exists(path): return False
    self.s3client.put_object(Bucket=self.bucketname, Key=path, Body=file_data)
    return True

  def make_dir(self, path, permission=None):
    # This function is a no-op. S3 is a key-value store and does not have a directory
    # structure. We can use a non existant path as though it already exists.
    pass

  def copy(self, src, dst):
    self.s3client.copy_object(Bucket=self.bucketname,
                              CopySource={'Bucket':self.bucketname, 'Key':src}, Key=dst)
    assert self.exists(dst), \
        'S3 copy failed: Destination file {dst} does not exist'.format(dst=dst)

  # Since S3 is a key-value store, it does not have a command like 'ls' for a directory
  # structured filesystem. It lists everything under a path recursively.
  # We have to manipulate its response to get an 'ls' like output.
  def ls(self, path):
    if not path.endswith('/'):
      path += '/'
    # Use '/' as a delimiter so that we don't get all keys under a path recursively.
    response = self.s3client.list_objects(
        Bucket=self.bucketname, Prefix=path, Delimiter='/')
    dirs = []
    # Non-keys or "directories" will be listed as 'Prefix' under 'CommonPrefixes'.
    if 'CommonPrefixes' in response:
      dirs = [t['Prefix'] for t in response['CommonPrefixes']]
    files = []
    # Keys or "files" will be listed as 'Key' under 'Contents'.
    if 'Contents' in response:
      files = [t['Key'] for t in response['Contents']]
    files_and_dirs = []
    files_and_dirs.extend([d.split('/')[-2] for d in dirs])
    for f in files:
      key = f.split("/")[-1]
      if not key == '':
        files_and_dirs += [key]
    return files_and_dirs

  def get_all_file_sizes(self, path):
    if not path.endswith('/'):
      path += '/'
    # Use '/' as a delimiter so that we don't get all keys under a path recursively.
    response = self.s3client.list_objects(
        Bucket=self.bucketname, Prefix=path, Delimiter='/')
    if 'Contents' in response:
      return [t['Size'] for t in response['Contents']]
    return []

  def exists(self, path):
    response = self.s3client.list_objects(Bucket=self.bucketname,Prefix=path)
    return response.get('Contents') is not None

  # Helper function which lists keys in a path. Should not be used by the tests directly.
  def _list_keys(self, path):
    if not self.exists(path):
      return False
    response = self.s3client.list_objects(Bucket=self.bucketname, Prefix=path)
    contents = response.get('Contents')
    return [c['Key'] for c in contents]

  def delete_file_dir(self, path, recursive=False):
    if not self.exists(path):
      return True
    objects = [{'Key': k} for k in self._list_keys(path)] if recursive else path
    self.s3client.delete_objects(Bucket=self.bucketname, Delete={'Objects':objects})
    return True
