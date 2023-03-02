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

# Fixture parametrizations should go here, not in conftest.py.

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.patterns import is_valid_impala_identifier


class UniqueDatabase(object):

  @staticmethod
  def parametrize(name_prefix=None, sync_ddl=False, num_dbs=1):
    named_params = {}
    if name_prefix is not None:
      name_prefix = str(name_prefix)
      if not is_valid_impala_identifier(name_prefix):
        raise ValueError('name_prefix "{0}" is not a valid Impala identifier; check '
                         'value for long length or invalid '
                         'characters.'.format(name_prefix))
      named_params["name_prefix"] = name_prefix
    if not isinstance(sync_ddl, bool):
      raise ValueError('value {0} of sync_ddl is be a boolean'.format(sync_ddl))
    named_params["sync_ddl"] = sync_ddl
    if not isinstance(num_dbs, int) or num_dbs <= 0:
      raise ValueError("num_dbs must be an integer >= 1 but '{0}' given".format(num_dbs))
    named_params["num_dbs"] = num_dbs
    return pytest.mark.parametrize('unique_database', [named_params], indirect=True)
