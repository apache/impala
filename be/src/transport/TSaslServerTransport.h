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

#pragma once

#include <pthread.h>
#include <mutex>
#include <string>

#include <boost/shared_ptr.hpp>
#include <thrift/transport/TTransport.h>
#include "transport/TSasl.h"
#include "transport/TSaslTransport.h"

namespace apache { namespace thrift { namespace transport {

/**
 *
 * This transport implements the Simple Authentication and Security Layer (SASL).
 * see: http://www.ietf.org/rfc/rfc2222.txt.  It is based on and depends
 * on the presence of the cyrus-sasl library.
 *
 */
class TSaslServerTransport : public TSaslTransport {
 private:
  /* Set up the Sasl server state for a connection.
   * The Server negotiation state is setup in handleSaslStartMessage() after
   * agreeing upon the client's chosen mechanism. So we do nothing here.
   * The client always sends the first message in the protocol, so there isn't
   * a case where the TSaslServer state needs to be setup before receiving the
   * first message (see TSaslClient::hasInitialResponse()).
   */
  virtual void setupSaslNegotiationState();

  /* Reset the Sasl server state. The negotiation will have to start from scratch
   * after this is called.
   */
  virtual void resetSaslNegotiationState();

  /* Handle the initial message from the client. */
  virtual void handleSaslStartMessage();

  /* Structure that defines a Sasl Server. */
  struct TSaslServerDefinition {
    /* Mechanism implemented */
    std::string mechanism_;

    /* Protocol */
    std::string protocol_;

    /* Name of the server node. */
    std::string serverName_;

    /* Realm - for Kerberos */
    std::string realm_;

    /* Sasl Flags: see sasl.h */
    unsigned flags_;

    /* Properties of this server. */
    std::map<std::string, std::string> props_;

    /* Callbacks to the application program. */
    std::vector<struct sasl_callback> callbacks_;

    TSaslServerDefinition(const std::string& mechanism, const std::string& protocol,
                          const std::string& serverName, const std::string& realm,
                          unsigned flags, const std::map<std::string, std::string>& props,
                          const std::vector<struct sasl_callback>& callbacks)
        : mechanism_(mechanism),
          protocol_(protocol),
          serverName_(serverName),
          realm_(realm),
          flags_(flags),
          props_(props),
          callbacks_(callbacks) {
    }
  };

  /* Map from a mechanism to a server definition. */
  std::map<std::string, TSaslServerDefinition*> serverDefinitionMap_;

  /* Wrap the passed transport in a transport for the defined server. */
  TSaslServerTransport(const std::map<std::string, TSaslServerDefinition*>& serverMap,
                       std::shared_ptr<TTransport> transport);

 public:

  /**
   * Constructs a new TSaslTransport to act as a server.
   * transport: the underlying transport used to read and write data.
   *
   */
  TSaslServerTransport(std::shared_ptr<TTransport> transport);

  /**
   * Construct a new TSaslTrasnport, passing in the components of the definition.
   */
  TSaslServerTransport(const std::string& mechanism,
                       const std::string& protocol,
                       const std::string& serverName,
                       const std::string& realm,
                       unsigned flags,
                       const std::map<std::string, std::string>& props,
                       const std::vector<struct sasl_callback>& callbacks,
                       std::shared_ptr<TTransport> transport);

  /* Add a definition to a server transport */
  void addServerDefinition(const std::string& mechanism,
                           const std::string& protocol,
                           const std::string& serverName,
                           const std::string& realm,
                           unsigned int flags,
                           std::map<std::string, std::string> props,
                           std::vector<struct sasl_callback> callbacks) {
    serverDefinitionMap_.insert(std::pair<std::string,
                                          TSaslServerDefinition*>(mechanism,
        new TSaslServerDefinition(mechanism,
            protocol, serverName, realm, flags, props, callbacks)));
  }

  /* Set the server */
  void setSaslServer(sasl::TSasl* saslServer);

  /**
   * Factory to ensure that a given underlying TTransport instance receives
   * the same TSaslServerTransport for both send and receive.
   */
  class Factory : public TTransportFactory {
   public:
    Factory() {
    }
    /**
     * Create a new Factor for a server definition.
     * Provides a single definition for the server, others may be added.
     */
    Factory(const std::string& mechanism, const std::string& protocol,
            const std::string& serverName, const std::string& realm,
            unsigned flags, std::map<std::string, std::string> props,
            std::vector<struct sasl_callback> callbacks)
        : TTransportFactory() {
      addServerDefinition(mechanism, protocol, serverName, realm, flags,
          props, callbacks);
    }

    virtual ~Factory() {}

    /**
     * Wraps the transport with the Sasl protocol.
     */
    virtual std::shared_ptr<TTransport> getTransport(
        std::shared_ptr<TTransport> trans);

    /* Add a definition to a server transport factory */
    void addServerDefinition(const std::string& mechanism,
                             const std::string& protocol,
                             const std::string& serverName,
                             const std::string& realm,
                             unsigned int flags,
                             std::map<std::string, std::string> props,
                             std::vector<struct sasl_callback> callbacks) {
      serverDefinitionMap_.insert(
          std::pair<std::string, TSaslServerDefinition*>(mechanism,
          new TSaslServerDefinition(mechanism, protocol,
              serverName, realm, flags, props, callbacks)));
    }
   private:
    /* Map for holding and returning server definitions. */
    std::map<std::string, TSaslServerDefinition*> serverDefinitionMap_;
  };

};

}}} // apache::thrift::transport
