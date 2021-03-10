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

#ifndef IMPALA_TRANSPORT_TSSLCLIENTTRANSPORT_H
#define IMPALA_TRANSPORT_TSSLCLIENTTRANSPORT_H

#include <string>

#include <boost/shared_ptr.hpp>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TVirtualTransport.h>
#include "transport/TSaslTransport.h"
#include "transport/TSasl.h"

namespace apache { namespace thrift { namespace transport {

/**
 * This transport implements the Simple Authentication and Security Layer (SASL).
 * see: http://www.ietf.org/rfc/rfc2222.txt.  It is based on and depends
 * on the presence of the cyrus-sasl library.  This is the client side.
 */
class TSaslClientTransport : public TSaslTransport {
 public:

  /**
   * Constructs a new TSaslTransport to act as a client.
   * saslClient: the sasl object implimenting the underlying authentication handshake
   * transport: the transport to read and write data.
   */
  TSaslClientTransport(std::shared_ptr<sasl::TSasl> saslClient,
                       std::shared_ptr<TTransport> transport);

 protected:
  /* Set up the Sasl server state for a connection. */
  virtual void setupSaslNegotiationState();

  /* Reset the Sasl client state. The negotiation will have to start from scratch
   * after this is called.
   */
  virtual void resetSaslNegotiationState();

  /// Handle any startup messages.
  virtual void handleSaslStartMessage();
};

}}} // apache::thrift::transport

#endif // #ifndef IMPALA_TRANSPORT_TSSLCLIENTTRANSPORT_H
