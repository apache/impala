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

const globals = require("globals");

module.exports = {
  parserOptions : {
    ecmaVersion : 2021,
    sourceType : "module"
  },
  globals : globals.browser,
  rules : {
    // Warn for unused variables
    "no-unused-vars" : "warn",
    // Enforce strict equality (=== and !==)
    "eqeqeq" : "error",
    // Require curly braces for all control statements (if, while, etc.)
    "curly" : ["error", "multi-line"],
    // Enforce semicolons at the end of statements
    "semi" : ["error", "always"],
    // Enforce double quotes for strings
    "quotes" : ["error", "double", {"allowTemplateLiterals" : true}],
    // Set maximum line length to 90
    "max-len" : ["error", {"code" : 90}],
    // Disallow `var`, use `let` or `const`
    "no-var" : "error",
    // Prefer `const` where possible
    "prefer-const" : "error",
    // Disallow multiple empty lines
    "no-multiple-empty-lines" : ["error",{max : 1}],
    // Enforce spacing around infix operators (eg. +, =)
    "space-infix-ops" : "error",
    // Require parentheses around arrow function arguments
    "arrow-parens" : ["error", "as-needed"],
    // Require a space before if, function and other blocks
    "space-before-blocks" : "error",
    // Enforce consistent spacing inside braces
    "object-curly-spacing" : ["error", "never"],
    // Disallow shadowing variables declared in the outer scope
    "no-shadow" : "error",
    // Disallow constant conditions in if statements, loops, etc
    "no-constant-condition" : "error",
    // Disallow unnecessary parentheses in expressions
    "no-extra-parens" : "error",
    // Disallow duplicate arguments in function definitions
    "no-dupe-args" : "error",
    // Disallow duplicate keys in object literals
    "no-dupe-keys" : "error",
    // Disallow unreachable code after return, throw, continue, etc
    "no-unreachable" : "error",
    // Disallow reassigning function parameters
    "no-param-reassign" : "error",
    // Require functions to always return a value or to not return anything at all
    "consistent-return" : "error",
    // Enforce consistent use of dot notation wherever possible
    "dot-notation" : "error",
    // Enforces spacing around the colon in object literal properties
    "key-spacing" : ["error", {"beforeColon" : true}],
    // Disallow optional chaining, where undefined values are not allowed
    "no-unsafe-optional-chaining" : "error"
  },
  overrides: [
    {
      files : ["**/*.cjs"],
      parserOptions: {
        sourceType: "script"
      }
    }
  ]
}