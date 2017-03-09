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
            'ca_cert': None,
            'config_file': os.path.expanduser("~/.impalarc"),
            'default_db': None,
            'history_max': 1000,
            'ignore_query_failure': False,
            'impalad': socket.getfqdn() + ':21000',
            'kerberos_service_name': 'impala',
            'output_delimiter': '\\t',
            'output_file': None,
            'print_header': False,
            'print_progress' : False,
            'print_summary' : False,
            'query': None,
            'query_file': None,
            'refresh_after_connect': False,
            'show_profiles': False,
            'ssl': False,
            'use_kerberos': False,
            'use_ldap': False,
            'user': getpass.getuser(),
            'verbose': True,
            'version': False,
            'write_delimited': False,
            }
