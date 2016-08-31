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
//
/// "Redaction" is about preventing sensitive data from showing up in undesired locations,
/// such as log files or a web ui. For example, this library could be used to log credit
/// card numbers as XXXX-...-XXXX instead of actual numbers.
//
/// The java original is https://github.com/cloudera/logredactor .

#ifndef IMPALA_UTIL_REDACTOR_H
#define IMPALA_UTIL_REDACTOR_H

#include <string>

namespace impala {

/// Load redaction rules stored in a file. Returns an error message if problems were
/// encountered while constructing the redaction rules. If no rules were given the
/// message will be empty. This is not thread-safe and should not be called with
/// concurrent calls to Redact().
//
/// The rule format is JSON with the following structure:
/// {
///   "version": 1,      # required int
///   "rules": [         # required array, rules are executed in order
///     {
///       "description": "",     # optional, completely ignored
///       "caseSensitive": true, # optional boolean, default = true, applies to both the
///                              #   'trigger' and 'search'.
///       "trigger": "find",     # optional string, default = "", rule only goes into
///                              #   effect if the string to be redacted contains this
///                              #   value. An empty string matches everything.
///       "search": "\\d",       # required string, regex to search for
///       "replace": "#"         # required string, the replacement value for 'search'
///     },
///     ...
///   ]
/// }
//
/// Rules are set globally. Having multiple sets of rules in effect at the same time is
/// not supported.
std::string SetRedactionRulesFromFile(const std::string& rules_file_path);

/// Applies the redaction rules to the given string in-place. If no redaction rules
/// are set, nothing will be done. This is thread-safe as long as multiple calls are
/// not made with the same input string. If the 'changed' argument is not null, it will
/// be set to true if redaction modified 'string'.
void Redact(std::string* string, bool* changed = NULL);

/// Utility function to redacted a string without modifying the original.
inline std::string RedactCopy(const std::string& original) {
  std::string temp(original);
  Redact(&temp);
  return temp;
}

}

#endif
