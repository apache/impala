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

# This script is a thin wrapper to execute a query using Impyla against an HTTP endpoint.
# It can be used by other tests (e.g. LdapImpylaHttpTest.java) that start a cluster with
# an LDAP server to validate Impyla's functionality.

from __future__ import absolute_import, division, print_function
import argparse
import logging

from impala import dbapi as impyla

logging.basicConfig(level=logging.INFO)


def run_query(query, args):
  """Runs a query using Impyla. Args must contain various options that are forwarded to
  Impyla's connect() method."""
  auth_mechanism = 'NOSASL'
  if args.user:
    auth_mechanism = 'LDAP'
  conn = impyla.connect(host=args.host, port=args.port, user=args.user,
                        password=args.password, auth_mechanism=auth_mechanism,
                        use_http_transport=True, http_path=args.http_path,
                        http_cookie_names=args.http_cookie_names)
  cursor = conn.cursor()
  cursor.execute(query)
  result = cursor.fetchall()
  # Print the result so that the caller can validate it.
  print(str(result))


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--host", default="localhost")
  parser.add_argument("--port", type=int, default=28000)
  parser.add_argument("--http_path", default="")
  parser.add_argument("--user")
  parser.add_argument("--password")
  parser.add_argument("--http_cookie_names", default=['impala.auth', 'impala.session.id'])
  parser.add_argument("--query", default="select 42")
  args = parser.parse_args()
  run_query(args.query, args)


if __name__ == "__main__":
  main()
