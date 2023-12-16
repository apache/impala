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

#include "redactor.h"
#include "redactor.detail.h"

#include <cerrno>
#include <cstring>  // strcmp, strcasestr
#include <ostream>
#include <sstream>
#include <sys/stat.h>
#include <vector>

#include <gutil/strings/substitute.h>
#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/error/en.h>
#include <re2/re2.h>
#include <re2/stringpiece.h>

#include "common/logging.h"

#include "common/names.h"

namespace impala {

using rapidjson::Document;
using rapidjson::Value;

typedef re2::RE2 Regex;


// The actual rules in effect, if any.
Rules* g_rules = NULL;

string NameOfTypeOfJsonValue(const Value& value) {
  switch (value.GetType()) {
    case rapidjson::kNullType:
      return "Null";
    case rapidjson::kFalseType:
    case rapidjson::kTrueType:
      return "Bool";
    case rapidjson::kObjectType:
      return "Object";
    case rapidjson::kArrayType:
      return "Array";
    case rapidjson::kStringType:
      return "String";
    case rapidjson::kNumberType:
      if (value.IsInt()) return "Integer";
      if (value.IsDouble()) return "Float";
      [[fallthrough]];
    default:
      DCHECK(false);
      return "Unknown";
  }
}

// This class will parse a version 1 rule file and populate g_rules.
class RulesParser {
 public:
  // Perform the parsing and populate g_rules. Any errors encountered will be written
  // to error_message.
  string Parse(const Document& rules_doc) {
    error_message_.str("");
    bool found_rules = false;
    for (Value::ConstMemberIterator member = rules_doc.MemberBegin();
        member != rules_doc.MemberEnd(); ++member) {
      if (strcmp("rules", member->name.GetString()) == 0) {
        found_rules = true;
        ParseRules(member->value);
      } else if (strcmp("version", member->name.GetString()) == 0) {
        // Ignore
      } else {
        AddDocParseError()
            << "unexpected property '" << member->name.GetString() << "' must be removed";
      }
    }
    if (!found_rules) {
      AddDocParseError() << "an array of rules is required";
    }
    return error_message_.str();
  }

 private:
  ostringstream error_message_;

  // The current index of the rule being parsed. 'SizeType' avoids an ambiguity between
  // json_value[int] (array) and json_value[const char*] (object), otherwise 0 could be
  // a null pointer.
  rapidjson::SizeType rule_idx_;

  // Parse an array of rules.
  void ParseRules(const Value& rules) {
    if (!rules.IsArray()) {
      AddDocParseError() << "'rules' must be of type Array but is a "
                         << NameOfTypeOfJsonValue(rules);
      return;
    }
    for (rule_idx_ = 0; rule_idx_ < rules.Size(); ++rule_idx_) {
      const Value& rule = rules[rule_idx_];
      if (!rule.IsObject()) {
        AddRuleParseError() << "rule should be a JSON Object but is a "
                            << NameOfTypeOfJsonValue(rule);
        continue;
      }
      ParseRule(rule);
    }
  }

  // Parse a rule and populate g_rules.
  void ParseRule(const Value& json_rule) {
    bool found_replace = false;
    bool case_sensitive = true;
    string search_text, replace, trigger;
    for (Value::ConstMemberIterator member = json_rule.MemberBegin();
        member != json_rule.MemberEnd(); ++member) {
      if (strcmp("search", member->name.GetString()) == 0) {
        if (!ReadRuleProperty("search", json_rule, &search_text)) return;
        if (search_text.empty()) {
          AddRuleParseError() << "search property must be a non-empty regex";
          return;
        }
      } else if (strcmp("replace", member->name.GetString()) == 0) {
        found_replace = true;
        if (!ReadRuleProperty("replace", json_rule, &replace)) return;
      } else if (strcmp("trigger", member->name.GetString()) == 0) {
        if (!ReadRuleProperty("trigger", json_rule, &trigger, /*required*/ false)) return;
      } else if (strcmp("caseSensitive", member->name.GetString()) == 0) {
        if (!ReadRuleProperty("caseSensitive", json_rule, &case_sensitive, false)) return;
      } else if (strcmp("description", member->name.GetString()) == 0) {
        // Ignore, this property is for user documentation.
      } else {
        // Future properties may change the meaning of current properties so ignoring
        // unknown properties is not safe.
        AddRuleParseError() << "unexpected property '" << member->name.GetString()
                            << "' must be removed";
        return;
      }
    }
    if (search_text.empty()) {  // Only empty if not found
      AddRuleParseError() << "a 'search' property is required";
      return;
    } else if (!found_replace) {
      AddRuleParseError() << "a 'replace' property is required";
      return;
    }
    const Rule& rule = Rule::Create(trigger, search_text, replace, case_sensitive);
    if (!rule.search_pattern.ok()) {
      AddRuleParseError() << "search regex is invalid; " << rule.search_pattern.error();
      return;
    }
    (*g_rules).push_back(rule);
  }

  // Reads a rule property of the given name and assigns the property value to the out
  // parameter. A true return value indicates success.
  template<typename T>
  bool ReadRuleProperty(const string& name, const Value& rule, T* value,
      bool required = true) {
    const Value& json_value = rule[name.c_str()];
    if (json_value.IsNull()) {
      if (required) {
        AddRuleParseError() << name << " property is required and cannot be null";
        return false;
      }
      return true;
    }
    return ValidateTypeAndExtractValue(name, json_value, value);
  }

// Extract a value stored in a rapidjson::Value and assign it to the out parameter.
// The type will be validated before extraction. A true return value indicates success.
// The name parameter is only used to generate an error message upon failure.
#define EXTRACT_VALUE(json_type, cpp_type) \
  bool ValidateTypeAndExtractValue(const string& name, const Value& json_value, \
      cpp_type* value) { \
    if (!json_value.Is ## json_type()) { \
      AddRuleParseError() << name << " property must be of type " #json_type \
                          << " but is a " << NameOfTypeOfJsonValue(json_value); \
      return false; \
    } \
    *value = json_value.Get ## json_type(); \
    return true; \
  }
EXTRACT_VALUE(String, string)
EXTRACT_VALUE(Bool, bool)

  ostream& AddDocParseError() {
    if (error_message_.tellp()) error_message_ << endl;
    error_message_ << "Error parsing redaction rules; ";
    return error_message_;
  }

  ostream& AddRuleParseError() {
    if (error_message_.tellp()) error_message_ << endl;
    error_message_ << "Error parsing redaction rule #" << (rule_idx_ + 1) <<  "; ";
    return error_message_;
  }
};

string SetRedactionRulesFromFile(const string& rules_file_path) {
  if (g_rules == NULL) g_rules = new Rules();
  g_rules->clear();

  // Read the file.
  FILE* rules_file = fopen(rules_file_path.c_str(), "r");
  if (rules_file == NULL) {
    return Substitute("Could not open redaction rules file '$0'; $1",
        rules_file_path, strerror(errno));
  }
  // Check for an empty file and ignore it. This is done to play nice with automated
  // cluster configuration tools that will generate empty files when no rules are in
  // effect. Without this the JSON parser would produce an error.
  struct stat rules_file_stats;
  if (fstat(fileno(rules_file), &rules_file_stats)) {
    fclose(rules_file);
    return Substitute("Error reading redaction rules file; $0", strerror(errno));
  }
  if (rules_file_stats.st_size == 0) {
    fclose(rules_file);
    return "";
  }

  char readBuffer[65536];
  rapidjson::FileReadStream stream(rules_file, readBuffer, sizeof(readBuffer));
  Document rules_doc;
  rules_doc.ParseStream(stream);
  fclose(rules_file);
  if (rules_doc.HasParseError()) {
    return Substitute("Error parsing redaction rules; $0",
        GetParseError_En(rules_doc.GetParseError()));
  }
  if (!rules_doc.IsObject()) {
    return "Error parsing redaction rules; root element must be a JSON Object.";
  }
  if (!rules_doc.HasMember("version")) {
    return "Error parsing redaction rules; a document version is required.";
  }
  const Value& version = rules_doc["version"];
  if (version.IsNull()) {
    return "Error parsing redaction rules; a document version is required.";
  }
  if (!version.IsInt()) {
    return Substitute("Error parsing redaction rules; version must be an Integer but "
        "is a $0", NameOfTypeOfJsonValue(version));
  }
  if (version.GetInt() != 1) {
    return "Error parsing redaction rules; only version 1 is supported.";
  }

  RulesParser rules_parser;
  return rules_parser.Parse(rules_doc);
}

void Redact(string* value, bool* changed) {
  DCHECK(value != NULL);
  if (g_rules == NULL || g_rules->empty()) return;
  for (Rules::const_iterator rule = g_rules->begin(); rule != g_rules->end(); ++rule) {
    if (rule->case_sensitive()) {
      if (value->find(rule->trigger) == string::npos) continue;
    } else {
      if (strcasestr(value->c_str(), rule->trigger.c_str()) == NULL) continue;
    }
    int replacement_count = re2::RE2::GlobalReplace(
        value, rule->search_pattern, rule->replacement);
    if (changed != NULL && !*changed) *changed = replacement_count;
  }
}

}
