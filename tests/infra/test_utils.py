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

# This module contains tests for some of the tests/util code.

from tests.util.filesystem_utils import prepend_with_fs

def test_filesystem_utils():
  # Verify that empty FS prefix gives back the same path.
  path = "/fake-warehouse"
  assert prepend_with_fs("", path) == path

  # Verify that prepend_with_fs() is idempotent.
  fs = "fakeFs://bucket"
  path = "/fake-warehouse"
  assert prepend_with_fs(fs, path) == fs + path
  assert prepend_with_fs(fs, prepend_with_fs(fs, path)) == fs + path
