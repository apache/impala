// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "exec/read-write-util.h"
#include "exec/byte-stream.h"

using namespace std;
using namespace impala;

Status ReadWriteUtil::ReadZLong(ByteStream* byte_stream, int64_t* value) {
  uint8_t buf[MAX_ZLONG_LEN];
  int64_t nread;

  RETURN_IF_ERROR(byte_stream->Read(buf, MAX_ZLONG_LEN, &nread));

  int len = GetZLong(buf, value);
  if (len > nread) {
    return Status("Bad integer format");
  } else if (len < nread) {
    RETURN_IF_ERROR(byte_stream->SeekRelative(len - nread));
  }
  
  return Status::OK;
}

Status ReadWriteUtil::ReadZInt(ByteStream* byte_stream, int32_t* integer) {
  uint8_t buf[MAX_ZINT_LEN];
  int64_t nread;

  RETURN_IF_ERROR(byte_stream->Read(buf, MAX_ZINT_LEN, &nread));

  int len = GetZInt(buf, integer);
  if (len > nread) {
    return Status("Bad integer format");
  } else if (len < nread) {
    RETURN_IF_ERROR(byte_stream->SeekRelative(len - nread));
  }
  
  return Status::OK;
}

Status ReadWriteUtil::ReadString(ByteStream* byte_stream, string* str) {
  int64_t len;
  RETURN_IF_ERROR(ReadZLong(byte_stream, &len));
  str->reserve(len);
  str->resize(len);
  int64_t bytes_read;
  RETURN_IF_ERROR(byte_stream->Read(
      reinterpret_cast<uint8_t*>(&(*str)[0]), len, &bytes_read));
  if (len != bytes_read) {
    return Status("Short read in ReadString");
  }
  return Status::OK;
}

Status ReadWriteUtil::ReadBytes(ByteStream* byte_stream, vector<uint8_t>* buf) {
  int64_t len;
  RETURN_IF_ERROR(ReadZLong(byte_stream, &len));
  buf->reserve(len);
  buf->resize(len);
  int64_t bytes_read;
  RETURN_IF_ERROR(byte_stream->Read(&(*buf)[0], len, &bytes_read));
  if (len != bytes_read) {
    return Status("Short read in ReadBytes");
  }
  return Status::OK;
}

Status ReadWriteUtil::SkipBytes(ByteStream* byte_stream) {
  int64_t len;
  RETURN_IF_ERROR(ReadZLong(byte_stream, &len));
  return byte_stream->SeekRelative(len);
}

int ReadWriteUtil::PutZInt(int32_t integer, uint8_t* buf) {
  integer = (integer << 1) ^ (integer >> 31);
  const int mask = 0x7f;
  const int cont = 0x80;
  buf[0] = integer & mask;
  int len = 1;
  while ((integer >>= 7) != 0) {
    // Set the continuation bit.
    buf[len - 1] |= cont;
    buf[len] = integer & mask;
    len++;
  }

  return len;
}

int ReadWriteUtil::PutZLong(int64_t longint, uint8_t* buf) {
  longint = (longint << 1) ^ (longint >> 31);
  const int mask = 0x7f;
  const int cont = 0x80;
  buf[0] = longint & mask;
  int len = 1;
  while ((longint >>= 7) != 0) {
    // Set the continuation bit.
    buf[len - 1] |= cont;
    buf[len] = longint & mask;
    len++;
  }

  return len;
}

