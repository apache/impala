#!/usr/bin/env impala-python

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

# This is a script that allows a tester to test his Oracle setup and cx_Oracle
# installation within his impala-python environment. It's meant to be super simple.  The
# emphasis here is on a cx_Oracle connection and cursor with no other distractions, even
# command line option parsing/handling. Modify the constants below and run:
#
#  $ ./verify-oracle-connection.py
#
# If you get an exception, something is wrong. If cx_Oracle was able to make a
# connection to Oracle and run a simple query, you'll get a success message.
#
# Refer to ORACLE.txt for help.

# Importing the whole module instead of doing selective import seems to help find linker
# errors.
from __future__ import absolute_import, division, print_function
import cx_Oracle

# Host on which Oracle Database lies.
HOST = '127.0.0.1'

# The values below are default for the version of Oracle Database Express Edition
# tested.  You may need to change these as needed:
PORT = 1521
SID = 'XE'
USER = 'system'
PASSWORD = 'oracle'


def main():
  TEST_QUERY = 'SELECT 1 FROM DUAL'
  EXPECTED_RESULT = [(1,)]
  dsn = cx_Oracle.makedsn(HOST, PORT, SID)
  with cx_Oracle.connect(user=USER, password=PASSWORD, dsn=dsn) as conn:
    try:
      cursor = conn.cursor()
      query = cursor.execute(TEST_QUERY)
      rows = query.fetchall()
    finally:
      cursor.close()
    assert rows == EXPECTED_RESULT
  print('success')


if '__main__' == __name__:
  main()
