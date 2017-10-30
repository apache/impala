// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file includes constant strings that are used in multiple places in the codebase.

#ifndef IMPALA_COMMON_CONSTANT_STRINGS_H_
#define IMPALA_COMMON_CONSTANT_STRINGS_H_

// Template for a description of the ways to specify bytes. strings::Substitute() must
// used to fill in the blanks.
#define MEM_UNITS_HELP_MSG "Specified as number of bytes ('<int>[bB]?'), " \
                          "megabytes ('<float>[mM]'), " \
                          "gigabytes ('<float>[gG]'), " \
                          "or percentage of $0 ('<int>%'). " \
                          "Defaults to bytes if no unit is given."

#endif
