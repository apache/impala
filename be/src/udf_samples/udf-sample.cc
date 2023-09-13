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

#include "udf-sample.h"

#include <string>

// In this sample we are declaring a UDF that adds two ints and returns an int.
IMPALA_UDF_EXPORT
IntVal AddUdf(FunctionContext* context, const IntVal& arg1, const IntVal& arg2) {
  if (arg1.is_null || arg2.is_null) return IntVal::null();
  return IntVal(arg1.val + arg2.val);
}

// Multiple UDFs can be defined in the same file

// Classify input customer reviews.
IMPALA_UDF_EXPORT
StringVal ClassifyReviewsDefault(FunctionContext* context, const StringVal& input) {
  std::string request =
      std::string("Classify the following review as positive, neutral, or negative")
      + std::string(" and only include the uncapitalized category in the response: ")
      + std::string(reinterpret_cast<const char*>(input.ptr), input.len);
  StringVal prompt(request.c_str());
  return context->Functions()->ai_generate_text_default(context, prompt);
}

// Classify input customer reviews.
IMPALA_UDF_EXPORT
StringVal ClassifyReviews(FunctionContext* context, const StringVal& input) {
  std::string request =
      std::string("Classify the following review as positive, neutral, or negative")
      + std::string(" and only include the uncapitalized category in the response: ")
      + std::string(reinterpret_cast<const char*>(input.ptr), input.len);
  StringVal prompt(request.c_str());
  const StringVal endpoint("https://api.openai.com/v1/chat/completions");
  const StringVal model("gpt-3.5-turbo");
  const StringVal api_key_jceks_secret("open-ai-key");
  const StringVal params("{\"temperature\": 0.9, \"model\": \"gpt-4\"}");
  return context->Functions()->ai_generate_text(
      context, endpoint, prompt, model, api_key_jceks_secret, params);
}
