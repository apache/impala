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
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/thread.hpp>

#include <thrift/transport/TBufferTransports.h>
#include "rpc/thrift-server.h"
#include "transport/TSaslTransport.h"
#include "transport/TSaslServerTransport.h"

#include "common/logging.h"

#include "common/names.h"

using namespace sasl;

namespace apache { namespace thrift { namespace transport {
TSaslServerTransport::TSaslServerTransport(shared_ptr<TTransport> transport)
   : TSaslTransport(transport) {
}

TSaslServerTransport::TSaslServerTransport(const string& mechanism,
                                           const string& protocol,
                                           const string& serverName,
                                           const string& realm,
                                           unsigned flags,
                                           const map<string, string>& props,
                                           const vector<struct sasl_callback>& callbacks,
                                           shared_ptr<TTransport> transport)
     : TSaslTransport(transport) {
  addServerDefinition(mechanism, protocol, serverName, realm, flags,
      props, callbacks);
}

TSaslServerTransport:: TSaslServerTransport(
    const std::map<std::string, TSaslServerDefinition*>& serverMap,
    boost::shared_ptr<TTransport> transport)
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
  // First argument is interpreted as C-string
  sasl_->evaluateChallengeOrResponse(
      reinterpret_cast<const uint8_t*>(message_str.c_str()), resLength, &resLength);

}

shared_ptr<TTransport> TSaslServerTransport::Factory::getTransport(
    shared_ptr<TTransport> trans) {
  lock_guard<mutex> l(transportMap_mutex_);
  // Thrift servers use both an input and an output transport to communicate with
  // clients. In principal, these can be different, but for SASL clients we require them
  // to be the same so that the authentication state is identical for communication in
  // both directions. In order to do this, we cache the transport that we return in a map
  // keyed by the transport argument to this method. Then if there are two successive
  // calls to getTransport() with the same transport, we are sure to return the same
  // wrapped transport both times.
  //
  // However, the cache map would retain references to all the transports it ever
  // created. Instead, we remove an entry in the map after it has been found for the first
  // time, that is, after the second call to getTransport() with the same argument. That
  // matches the calling pattern in TThreadedServer and TThreadPoolServer, which both call
  // getTransport() twice in succession when a connection is established, and then never
  // again. This is obviously brittle (what if for some reason getTransport() is called a
  // third time?) but for our usage of Thrift it's a tolerable band-aid.
  //
  // An alternative approach is to use the 'custom deleter' feature of shared_ptr to
  // ensure that when ret_transport is eventually deleted, its corresponding map entry is
  // removed. That is likely to be error prone given the locking involved; for now we go
  // with the simple solution.
  map<shared_ptr<TTransport>, shared_ptr<TBufferedTransport> >::iterator trans_map =
      transportMap_.find(trans);
  VLOG_EVERY_N(2, 100) << "getTransport(): transportMap_ size is: "
                       << transportMap_.size();
  shared_ptr<TBufferedTransport> ret_transport;
  if (trans_map == transportMap_.end()) {
    shared_ptr<TTransport> wrapped(new TSaslServerTransport(serverDefinitionMap_, trans));
    ret_transport.reset(new TBufferedTransport(wrapped,
            impala::ThriftServer::BufferedTransportFactory::DEFAULT_BUFFER_SIZE_BYTES));
    ret_transport.get()->open();
    transportMap_[trans] = ret_transport;
  } else {
    ret_transport = trans_map->second;
    transportMap_.erase(trans_map);
  }
  return ret_transport;
}

}}}
#endif
