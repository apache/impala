#!/usr/bin/env python
#
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

# defaults for OptionParser options stored in dict

import getpass
import os
import socket

impala_shell_defaults = {
            'impalad': socket.getfqdn() + ':21000',
            'query': None,
            'query_file': None,
            'use_kerberos': False,
            'output_file': None,
            'write_delimited': False,
            'print_header': False,
            'output_delimiter': '\\t',
            'kerberos_service_name': 'impala',
            'verbose': True,
            'show_profiles': False,
            'version': False,
            'ignore_query_failure': False,
            'refresh_after_connect': False,
            'default_db': None,
            'use_ldap': False,
            'user': getpass.getuser(),
            'ssl': False,
            'ca_cert': None,
            'config_file': os.path.expanduser("~/.impalarc"),
            'print_progress' : False,
            'print_summary' : False
            }
