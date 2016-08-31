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

#ifndef IMPALA_UTIL_REDACTOR_DETAIL_H_
#define IMPALA_UTIL_REDACTOR_DETAIL_H_

#include <string>
#include <vector>

#include <re2/re2.h>

namespace impala {
struct Rule {
  // Factory constructor. The factory pattern is used because constructing a
  // case-insensitive Regex requires multiple lines and a Rule should be immutable so
  // the Regex should be const. Const members must be initialized in the initialization
  // list but multi-line statements cannot be used there. Keeping the Rule class
  // immutable was preferred over having a direct constructor, though either should be
  // fine.
  static Rule Create(const std::string& trigger, const std::string& search_regex,
      const std::string& replacement, bool case_sensitive) {
    re2::RE2::Options options;
    options.set_case_sensitive(case_sensitive);
    re2::RE2 re(search_regex, options);
    return Rule(trigger, re, replacement);
  }

  // For use with vector.
  Rule(const Rule& other)
      : trigger(other.trigger),
        search_pattern(other.search_pattern.pattern(), other.search_pattern.options()),
        replacement(other.replacement) {}

  const std::string trigger;
  const re2::RE2 search_pattern;
  const std::string replacement;

  bool case_sensitive() const { return search_pattern.options().case_sensitive(); }

 private:
  // For use with the factory constructor. The case-sensitivity option in
  // 'regex_options' also applies to 'trigger'.
  Rule(const std::string& trigger, const re2::RE2& search_pattern, const std::string& replacement)
      : trigger(trigger),
        search_pattern(search_pattern.pattern(), search_pattern.options()),
        replacement(replacement) {}
};

typedef std::vector<Rule> Rules;

}

#endif // IMPALA_UTIL_REDACTOR_DETAIL_H_
