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
from os.path import dirname, abspath
import sys

# When installing the python shell as a standalone package, this __init__ is
# used to workaround the issues stemming from IMPALA-6808. Because of the way
# the Impala python environment has been somewhat haphazardly constructed in
# a deployed cluster, it ends up being "polluted" with top-level modules that
# should really be sub-modules. One of the principal places this occurs is with
# the various modules required by the Impala shell. This isn't a concern when
# the shell is invoked via a specially installed version of python that belongs
# to Impala, but it does become an issue when the shell is being run using the
# system python.
#
# If we want to install the shell as a standalone package, we need to construct
# it in such a way that all of the internal modules are contained within a
# top-level impala_shell namespace. However, this then breaks various imports
# throughout the Impala shell code. The way this file corrects that is to add
# the impala_shell directory to PYTHONPATH only when the shell is invoked. As
# far as I can tell, there's no cleaner way to address this without fully
# resolving IMPALA-6808.
impala_shell_dir = dirname(abspath(__file__))
sys.path.append(impala_shell_dir)
