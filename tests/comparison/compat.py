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


# The purpose of this module is to unify a place to settle DB differences in addition to
# differences based on DB API2 libraries (like Impyla vs. psycopg2). Putting the
# handling in a single place will make it easier to track what workarounds exist.

from __future__ import absolute_import, division, print_function
from tests.comparison import db_connection


def setup_ref_database(ref_conn):
  """Set up reference database at query generator runtime.

  Because data_generator.py runs are expensive operations, and reference databases tend
  to persist across runs, this method should support lightweight options to configure
  reference databases as needed, regardless of whether the data is loaded or not. A good
  example is loading optional PostgreSQL modules to support comparison with functions
  added to Impala.

  ref_conn: Open db_connection.DbConnection object. Method assumes no open cursors on
  open connection object. Method will close any cursors opened, and leave connection
  open.
  """

  if ref_conn.db_type == db_connection.POSTGRESQL:
    # IMPALA-7759: The Levenshtein function needs the fuzzystrmatch extension.
    # References:
    # https://www.postgresql.org/docs/9.5/fuzzystrmatch.html
    # https://www.postgresql.org/docs/9.5/sql-createextension.html
    # On Ubuntu 16, postgresql-contrib-9.5 provides fuzzystrmatch and is installed as
    # part of Impala's bin/bootstrap_system.sh.
    with ref_conn.cursor() as cursor:
      cursor.execute('CREATE EXTENSION IF NOT EXISTS fuzzystrmatch')
