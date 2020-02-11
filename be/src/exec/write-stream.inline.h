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


#ifndef IMPALA_EXEC_OUTPUT_BUFFER_INLINE_H
#define IMPALA_EXEC_OUTPUT_BUFFER_INLINE_H

#include "exec/write-stream.h"
#include "exec/read-write-util.h"

#include <stdlib.h>

namespace impala {

inline int WriteStream::WriteByte(uint8_t val) {
  return WriteByte(static_cast<char>(val));
}

inline int WriteStream::WriteByte(char val) {
  return WriteBytes(1, &val);
}

inline int WriteStream::WriteVLong(int64_t val) {
  uint8_t buf[ReadWriteUtil::MAX_VINT_LEN];
  int size = ReadWriteUtil::PutVLong(val, buf);
  return WriteBytes(size, buf);
}

inline int WriteStream::WriteVInt(int32_t val) {
  uint8_t buf[ReadWriteUtil::MAX_VINT_LEN];
  int size = ReadWriteUtil::PutVInt(val, buf);
  return WriteBytes(size, buf);
}

inline int WriteStream::WriteInt(uint32_t val) {
  uint8_t buf[sizeof(int32_t)];
  ReadWriteUtil::PutInt(static_cast<uint8_t*>(buf), val);
  return WriteBytes(sizeof(int32_t), buf);
}

inline int WriteStream::WriteZInt(int32_t val) {
  uint8_t buf[ReadWriteUtil::MAX_ZINT_LEN];
  int len = ReadWriteUtil::PutZInt(val, buf);
  return WriteBytes(len, buf);
}

inline int WriteStream::WriteZLong(int64_t val) {
  uint8_t buf[ReadWriteUtil::MAX_ZLONG_LEN];
  int len = ReadWriteUtil::PutZLong(val, buf);
  return WriteBytes(len, buf);
}

inline int WriteStream::WriteBytes(int length, const uint8_t* buf) {
  return WriteBytes(length, reinterpret_cast<const char*>(buf));
}

inline int WriteStream::WriteBytes(int length, const char* buf) {
  len_ += length;
  buffer_.write(buf, length);
  return length;
}

inline int WriteStream::WriteText(int length, const uint8_t* buf) {
  int l = length;
  l += WriteVInt(length);
  if (length > 0) WriteBytes(length, buf);
  return l;
}

inline int WriteStream::WriteEmptyText() {
  return WriteVInt(0);
}

inline int WriteStream::WriteBoolean(bool b) {
  uint8_t val = b ? 1 : 0;
  return WriteBytes(1, &val);
}

inline std::string WriteStream::String() {
  return buffer_.str();
}

inline size_t WriteStream::Size() {
  return len_;
}

inline void WriteStream::Clear() {
  len_ = 0;
  buffer_.str("");
}

} // namespace impala
#endif
