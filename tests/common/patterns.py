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

# Common patterns that ought to be the same throughout the framework should be placed
# here.

from __future__ import absolute_import, division, print_function
import re

# http://www.cloudera.com/content/www/en-us/documentation/enterprise/latest/topics/impala_identifiers.html
VALID_IMPALA_IDENTIFIER_REGEX = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]{,127}$')


def is_valid_impala_identifier(identifier):
  """Return True if identifier is a valid Impala identifier, False otherwise."""
  return VALID_IMPALA_IDENTIFIER_REGEX.match(identifier) is not None
