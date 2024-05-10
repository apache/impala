// This file will be removed when the code is accepted into the Thrift library.
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "config.h"
#ifdef HAVE_SASL_SASL_H
#include <stdint.h>
#include <mutex>
#include <sstream>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include "rpc/thrift-server.h"
#include "transport/TSaslTransport.h"
#include "transport/TSaslServerTransport.h"

#include "common/logging.h"

#include "common/names.h"

DEFINE_int32(sasl_connect_tcp_timeout_ms, 300000, "(Advanced) The underlying TSocket "
    "send/recv timeout in milliseconds for the initial SASL handeshake.");

using namespace sasl;

namespace apache { namespace thrift { namespace transport {
TSaslServerTransport::TSaslServerTransport(std::shared_ptr<TTransport> transport)
   : TSaslTransport(transport) {
}

TSaslServerTransport::TSaslServerTransport(const string& mechanism,
                                           const string& protocol,
                                           const string& serverName,
                                           const string& realm,
                                           unsigned flags,
                                           const map<string, string>& props,
                                           const vector<struct sasl_callback>& callbacks,
                                           std::shared_ptr<TTransport> transport)
     : TSaslTransport(transport) {
  addServerDefinition(mechanism, protocol, serverName, realm, flags,
      props, callbacks);
}

TSaslServerTransport:: TSaslServerTransport(
    const std::map<std::string, TSaslServerDefinition*>& serverMap,
    std::shared_ptr<TTransport> transport)
    : TSaslTransport(transport) {
  serverDefinitionMap_.insert(serverMap.begin(), serverMap.end());
}

/**
 * Set the server for this transport
 */
void TSaslServerTransport::setSaslServer(sasl::TSasl* saslServer) {
  if (isClient_) {
    throw TTransportException(
        TTransportException::INTERNAL_ERROR, "Setting server in client transport");
  }
  sasl_.reset(saslServer);
}

void TSaslServerTransport::setupSaslNegotiationState() {
  // Do nothing, as explained in header comment.
}

void TSaslServerTransport::resetSaslNegotiationState() {
  // Sometimes we may fail negotiation before creating the TSaslServer negotitation
  // state if the client's first message is invalid. So we don't assume that TSaslServer
  // will have been created.
  if (sasl_) sasl_->resetSaslContext();
}

void TSaslServerTransport::handleSaslStartMessage() {
  uint32_t resLength;
  NegotiationStatus status;

  uint8_t* message = receiveSaslMessage(&status, &resLength);
  // Message is a non-null terminated string; to use it like a
  // C-string we have to copy it into a null-terminated buffer.
  string message_str(reinterpret_cast<char*>(message), resLength);

  if (status != TSASL_START) {
    stringstream ss;
    ss << "Expecting START status, received " << status;
    sendSaslMessage(TSASL_ERROR,
                    reinterpret_cast<const uint8_t*>(ss.str().c_str()), ss.str().size());
    throw TTransportException(ss.str());
  }
  map<string, TSaslServerDefinition*>::iterator defn =
      TSaslServerTransport::serverDefinitionMap_.find(message_str);
  if (defn == TSaslServerTransport::serverDefinitionMap_.end()) {
    stringstream ss;
    ss << "Unsupported mechanism type " << message_str;
    sendSaslMessage(TSASL_BAD,
                    reinterpret_cast<const uint8_t*>(ss.str().c_str()), ss.str().size());
    throw TTransportException(TTransportException::BAD_ARGS, ss.str());
  }

  TSaslServerDefinition* serverDefinition = defn->second;
  sasl_.reset(new TSaslServer(serverDefinition->protocol_,
                              serverDefinition->serverName_,
                              serverDefinition->realm_,
                              serverDefinition->flags_,
                              &serverDefinition->callbacks_[0]));
  sasl_->setupSaslContext();
  // First argument is interpreted as C-string
  sasl_->evaluateChallengeOrResponse(
      reinterpret_cast<const uint8_t*>(message_str.c_str()), resLength, &resLength);

}

std::shared_ptr<TTransport> TSaslServerTransport::Factory::getTransport(
    std::shared_ptr<TTransport> trans) {
  // Thrift servers use both an input and an output transport to communicate with
  // clients. In principal, these can be different, but for SASL clients we require them
  // to be the same so that the authentication state is identical for communication in
  // both directions. In order to do this, we share the same TTransport object for both
  // input and output set in TAcceptQueueServer::SetupConnection.
  std::shared_ptr<TBufferedTransport> ret_transport;
  std::shared_ptr<TTransport> wrapped(
      new TSaslServerTransport(serverDefinitionMap_, trans));
  // Verify the max message size is inherited properly
  impala::VerifyMaxMessageSizeInheritance(trans.get(), wrapped.get());
  // Set socket timeouts to prevent TSaslServerTransport->open from blocking the server
  // from accepting new connections if a read/write blocks during the handshake
  TSocket* socket = static_cast<TSocket*>(trans.get());
  socket->setRecvTimeout(FLAGS_sasl_connect_tcp_timeout_ms);
  socket->setSendTimeout(FLAGS_sasl_connect_tcp_timeout_ms);
  ret_transport.reset(new TBufferedTransport(wrapped,
      impala::ThriftServer::BufferedTransportFactory::DEFAULT_BUFFER_SIZE_BYTES,
          wrapped->getConfiguration()));
  impala::VerifyMaxMessageSizeInheritance(wrapped.get(), ret_transport.get());
  ret_transport.get()->open();
  // Reset socket timeout back to zero, so idle clients do not timeout
  socket->setRecvTimeout(0);
  socket->setSendTimeout(0);
  return ret_transport;
}

}}}
#endif
