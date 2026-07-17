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

set(CUP_OUTPUT_DIRECTORY
  ${CMAKE_SOURCE_DIR}/fe/generated-sources/cup/org/apache/impala/analysis)

add_custom_command(
  OUTPUT
    ${CUP_OUTPUT_DIRECTORY}/SqlParser.java
    ${CUP_OUTPUT_DIRECTORY}/SqlParserSymbols.java
    ${CUP_OUTPUT_DIRECTORY}/action_table.dat
  DEPENDS sql-parser.cup
  COMMAND ${CMAKE_SOURCE_DIR}/bin/cmake_aux/generate-cup-sources.sh
)

add_custom_target(cup-deps
  DEPENDS
    ${CUP_OUTPUT_DIRECTORY}/SqlParser.java
    ${CUP_OUTPUT_DIRECTORY}/SqlParserSymbols.java
    ${CUP_OUTPUT_DIRECTORY}/action_table.dat
)
