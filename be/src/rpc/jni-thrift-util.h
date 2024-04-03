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

#ifndef IMPALA_RPC_JNI_THRIFT_UTIL_H
#define IMPALA_RPC_JNI_THRIFT_UTIL_H

#include "rpc/thrift-util.h"
#include "util/jni-util.h"

/// Utility functions that depend on both JNI and Thrift, kept separate to reduce
/// unnecessary dependencies where we just want to use one or the other.

namespace impala {

template <class T>
Status SerializeThriftMsg(JNIEnv* env, T* msg, jbyteArray* serialized_msg) {
  int buffer_size = 100 * 1024;  // start out with 100KB
  ThriftSerializer serializer(false, buffer_size);

  uint8_t* buffer = NULL;
  uint32_t size = 0;
  RETURN_IF_ERROR(serializer.SerializeToBuffer(msg, &size, &buffer));

  // Make sure that 'size' is within the limit of INT_MAX as the use of
  // 'size' below takes int.
  if (size > INT_MAX) {
    return Status(strings::Substitute(
        "The length of the serialization buffer ($0 bytes) exceeds the limit of $1 bytes",
        size, INT_MAX));
  }

  /// create jbyteArray given buffer
  *serialized_msg = env->NewByteArray(size);
  RETURN_ERROR_IF_EXC(env);
  if (*serialized_msg == NULL) return Status("couldn't construct jbyteArray");
  env->SetByteArrayRegion(*serialized_msg, 0, size, reinterpret_cast<jbyte*>(buffer));
  RETURN_ERROR_IF_EXC(env);
  return Status::OK();
}

template <class T>
Status DeserializeThriftMsg(JNIEnv* env, jbyteArray serialized_msg, T* deserialized_msg) {
  JniByteArrayGuard guard;
  RETURN_IF_ERROR(JniByteArrayGuard::create(env, serialized_msg, &guard));
  uint32_t buf_size = guard.get_size();
  return DeserializeThriftMsg(reinterpret_cast<const uint8_t*>(guard.get()), &buf_size,
      false, deserialized_msg);
}

}

#endif
