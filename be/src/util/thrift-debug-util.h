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

#pragma once
#include <string>
#include <thrift/protocol/TDebugProtocol.h>

#include "gutil/strings/substitute.h"
#include "util/debug-util.h"

DECLARE_string(debug_actions);

namespace { // unnamed namespace

constexpr char SECRET[] = "<HIDDEN>";

template <typename...>
using void_t = void;

template <typename...>
struct AlwaysFalse { static constexpr bool value = false; };

/// HasSessionHandle is a metafunction that helps to decide whether the given type has
/// a member 'sessionHandle'.
/// I.e. HasSessionHandle<T>::values is true iff T::sessionHandle exists.
template <typename, typename = void>
struct HasSessionHandle : std::false_type {};

template <typename T>
struct HasSessionHandle<T, void_t<decltype(T::sessionHandle)>> :
    std::is_same<decltype(T::sessionHandle),
                 apache::hive::service::cli::thrift::TSessionHandle>
{};

/// HasOperationHandle is a metafunction that helps to decide whether the given type has
/// a member 'operationHandle'.
/// I.e. HasOperationHandle<T>::value is true iff T::operationHandle exists.
template <typename, typename = void>
struct HasOperationHandle : std::false_type {};

template <typename T>
struct HasOperationHandle<T, void_t<decltype(T::operationHandle)>> :
    std::is_same<decltype(T::operationHandle),
                 apache::hive::service::cli::thrift::TOperationHandle>
{};

template <typename T>
struct HasSecret {
  static constexpr bool value =
      HasSessionHandle<T>::value ||
      HasOperationHandle<T>::value ||
      std::is_same<T, apache::hive::service::cli::thrift::THandleIdentifier>::value ||
      std::is_same<T, impala::TExecutePlannedStatementReq>::value;
};

template <typename T,
          std::enable_if_t<!HasSessionHandle<T>::value, T>* = nullptr>
inline void RemoveSessionHandleSecret(T& t)
{}

template <typename T,
          std::enable_if_t<HasSessionHandle<T>::value, T>* = nullptr>
inline void RemoveSessionHandleSecret(T& t) {
  t.sessionHandle.sessionId.secret = SECRET;
}

template <typename T,
          std::enable_if_t<!HasOperationHandle<T>::value, T>* = nullptr>
inline void RemoveOperationHandleSecret(T& t)
{}

template <typename T,
          std::enable_if_t<HasOperationHandle<T>::value, T>* = nullptr>
inline void RemoveOperationHandleSecret(T& t) {
  t.operationHandle.operationId.secret = SECRET;
}

template <typename T>
inline void RemoveSecret(T& t) {}

inline void RemoveSecret(apache::hive::service::cli::thrift::THandleIdentifier& t) {
  t.secret = SECRET;
}

template <typename T>
inline void RemoveAnySecret(T& t) {
  RemoveSessionHandleSecret(t);
  RemoveOperationHandleSecret(t);
  RemoveSecret(t);
}

} // unnamed namespace

namespace impala {

/// Delegate to apache::thrift::ThriftDebugString() when T doesn't have a session secret.
template <typename T,
          std::enable_if_t<!HasSecret<T>::value>* = nullptr>
inline std::string ThriftDebugString(const T& t) {
  return apache::thrift::ThriftDebugString(t);
}

/// Raise compile-time error when ThriftDebugString() is used on an object that has
/// a session secret.
template <typename T,
          std::enable_if_t<HasSecret<T>::value, T>* = nullptr>
inline std::string ThriftDebugString(const T& t) {
  static_assert(AlwaysFalse<T>::value,
      "Don't use ThriftDebugString() to output this object. "
      "Use RedactedDebugString() instead.");
  return "";
}

/// Creates a copy of 't', overwrites the session secret of the copy, then returns
/// a human-readable string.
template <typename T,
          std::enable_if_t<HasSecret<T>::value, T>* = nullptr>
inline std::string RedactedDebugString(const T& t) {
  T copy = t;
  RemoveAnySecret(copy);
  return apache::thrift::ThriftDebugString(copy);
}

inline std::string RedactedDebugString(const TExecutePlannedStatementReq& req) {
  static const char* ret_pattern =
      "TExecutePlannedStatementReq {\n"
      "  01: statementReq (struct) = $0,\n"
      "  *** OTHER FIELDS ARE OMITTED ***\n"
      "}";
  return strings::Substitute(ret_pattern, RedactedDebugString(req.statementReq));
}

/// Raise compile-time error when ThriftDebugStringNoThrow() is used on an object that has
/// a session secret.
template <typename T,
          std::enable_if_t<HasSecret<T>::value, T>* = nullptr>
inline std::string ThriftDebugStringNoThrow(const T& t) {
  static_assert(AlwaysFalse<T>::value,
      "Don't use ThriftDebugStringNoThrow() to output this object. "
      "Use RedactedDebugStringNoThrow() instead.");
  return "";
}

/// This method is a wrapper over Thrift's ThriftDebugString. It catches any exception
/// that might be thrown by the underlying Thrift method. Typically, we use this method
/// when we suspect the ThriftStruct could be a large object like a HdfsTable whose
/// string representation could have significantly larger memory requirements than the
/// binary representation.
template <typename ThriftStruct,
          std::enable_if_t<!HasSecret<ThriftStruct>::value>* = nullptr>
std::string ThriftDebugStringNoThrow(ThriftStruct ts) noexcept {
  try {
    Status status = DebugAction(FLAGS_debug_actions, "THRIFT_DEBUG_STRING");
    return apache::thrift::ThriftDebugString(ts);
  } catch (std::exception& e) {
    return "Unexpected exception received: " + std::string(e.what());
  }
}

/// Same as ThriftDebugStringNoThrow(), but hides the session/operation secret.
template <typename T,
          std::enable_if_t<HasSecret<T>::value, T>* = nullptr>
inline std::string RedactedDebugStringNoThrow(const T& t) {
  try {
    T copy = t;
    RemoveAnySecret(copy);
    Status status = DebugAction(FLAGS_debug_actions, "THRIFT_DEBUG_STRING");
    return apache::thrift::ThriftDebugString(copy);
  } catch (std::exception& e) {
    return "Unexpected exception received: " + std::string(e.what());
  }
}

} // namespace impala
