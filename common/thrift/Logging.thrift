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

namespace cpp impala
namespace java org.apache.impala.thrift

// Types to support Impala log forwarding.

// Convenience type to map between log4j levels and glog severity
enum TLogLevel {
  VLOG_3 = 0
  VLOG_2 = 1
  VLOG = 2
  INFO = 3
  WARN = 4
  ERROR = 5
  FATAL = 6
}

// Helper structs for GetJavaLogLevels(), SetJavaLogLevel() methods.
// These are used as:
// - input param to set the logging level of a particular Java class
// at runtime using GlogAppender.setLogLevel()
// - return value as a list of Java classes with corresponding
// logging levels set before using GlogAppender.getLogLevels()
struct TSetJavaLogLevelParams {
  1: required string class_name
  2: required string log_level
}

struct TGetJavaLogLevelsResult {
  1: required list<string> log_levels
}
