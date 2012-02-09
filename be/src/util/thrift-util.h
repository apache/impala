// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_THRIFT_UTIL_H
#define IMPALA_UTIL_THRIFT_UTIL_H

#include <sstream>
#include <jni.h>
#include <boost/shared_ptr.hpp>
#include <TApplicationException.h>

#include "util/jni-util.h"

namespace impala {

template <class T>
Status SerializeThriftMsg(JNIEnv* env, T* msg, jbyteArray* serialized_msg) {
  int buffer_size = 100 * 1024;  // start out with 1MB

  // Serialize msg into java bytearray using memory transport.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(buffer_size));
  apache::thrift::protocol::
    TBinaryProtocolFactoryT<apache::thrift::transport::TMemoryBuffer> tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(tmem_transport);
  try {
    msg->write(tproto.get());
  } catch (apache::thrift::TApplicationException& e) {
    std::stringstream msg;
    msg << "couldn't serialize thrift msg:\n" << e.what();
    return Status(msg.str());
  }

  // create jbyteArray given buffer
  uint8_t* buffer;
  uint32_t size;
  tmem_transport->getBuffer(&buffer, &size);
  *serialized_msg = env->NewByteArray(size);
  if (*serialized_msg == NULL) return Status("couldn't construct jbyteArray");
  env->SetByteArrayRegion(*serialized_msg, 0, size, reinterpret_cast<jbyte*>(buffer));
  RETURN_ERROR_IF_EXC(env, JniUtil::throwable_to_string_id());
  return Status::OK;
}

template <class T>
void DeserializeThriftMsg(JNIEnv* env, jbyteArray serialized_msg, T* deserialized_msg) {
  // TODO: Find out why using serialized_msg directly does not work.
  // Copy java byte array into native byte array.
  jboolean is_copy = false;
  int buf_size = env->GetArrayLength(serialized_msg);
  jbyte* buf = env->GetByteArrayElements(serialized_msg, &is_copy);
  uint8_t native_bytes[buf_size];
  for (int i = 0; i < buf_size; i++) {
    native_bytes[i] = buf[i];
  }

  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TTransport> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(native_bytes, buf_size));
  apache::thrift::protocol::
    TBinaryProtocolFactoryT<apache::thrift::transport::TMemoryBuffer> tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(tmem_transport);
  deserialized_msg->read(tproto.get());
}

}

#endif
