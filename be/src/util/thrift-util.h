// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_THRIFT_UTIL_H
#define IMPALA_UTIL_THRIFT_UTIL_H

#include <boost/shared_ptr.hpp>
#include <protocol/TBinaryProtocol.h>
#include <sstream>
#include <TApplicationException.h>
#include <transport/TBufferTransports.h>

#include "common/status.h"
#include "util/jni-util.h"

namespace impala {

class THostPort;
class ThriftServer;

template <class T>
Status SerializeThriftMsg(JNIEnv* env, T* msg, jbyteArray* serialized_msg) {
  int buffer_size = 100 * 1024;  // start out with 100KB

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
Status DeserializeThriftMsg(JNIEnv* env, jbyteArray serialized_msg, T* deserialized_msg) {
  jboolean is_copy = false;
  int buf_size = env->GetArrayLength(serialized_msg);
  jbyte* buf = env->GetByteArrayElements(serialized_msg, &is_copy);
  
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TTransport> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(
          reinterpret_cast<uint8_t*>(buf), buf_size));
  apache::thrift::protocol::
    TBinaryProtocolFactoryT<apache::thrift::transport::TMemoryBuffer> tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(tmem_transport);
  try {
    deserialized_msg->read(tproto.get());
  } catch (apache::thrift::protocol::TProtocolException& e) {
    std::stringstream msg;
    msg << "couldn't deserialize thrift msg:\n" << e.what();
    return Status(msg.str());
  }
  // Return buffer back. JNI_ABORT indicates to not copy contents back to java
  // side.
  env->ReleaseByteArrayElements(serialized_msg, buf, JNI_ABORT);
  return Status::OK;
}

// Redirects all Thrift logging to VLOG(1)
void InitThriftLogging();

// Wait for a server that is running locally to start accepting
// connections, up to a maximum timeout
Status WaitForLocalServer(const ThriftServer& server, int num_retries,
   int retry_interval_ms);

// Wait for a server to start accepting connections, up to a maximum timeout
Status WaitForServer(const std::string& host, int port, int num_retries,
   int retry_interval_ms);

// Utility method to print address as address:port
void THostPortToString(const THostPort& address, std::string* out);

// Prints a hostport as ipaddress:port
std::ostream& operator<<(std::ostream& out, const THostPort& hostport);

}

#endif
