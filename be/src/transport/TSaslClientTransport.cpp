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
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

#include <thrift/transport/TBufferTransports.h>
#include "transport/TSaslTransport.h"
#include "transport/TSaslClientTransport.h"

using namespace sasl;

namespace apache { namespace thrift { namespace transport {

TSaslClientTransport::TSaslClientTransport(std::shared_ptr<sasl::TSasl> saslClient,
                                           std::shared_ptr<TTransport> transport)
   : TSaslTransport(saslClient, transport) {
}

void TSaslClientTransport::setupSaslNegotiationState() {
  if (!sasl_) {
    throw SaslClientImplException(
        "Invalid state: setupSaslNegotiationState() failed. TSaslClient not created");
  }
  sasl_->setupSaslContext();
}

void TSaslClientTransport::resetSaslNegotiationState() {
  if (!sasl_) {
    throw SaslClientImplException(
        "Invalid state: resetSaslNegotiationState() failed. TSaslClient not created");
  }
  sasl_->resetSaslContext();
}

void TSaslClientTransport::handleSaslStartMessage() {

  uint32_t resLength = 0;
  uint8_t dummy = 0;
  uint8_t *initialResponse = &dummy;

  /* Get data to send to the server if the client goes first. */
  if (sasl_->hasInitialResponse()) {
    initialResponse = sasl_->evaluateChallengeOrResponse(NULL, 0, &resLength);
  }

  /* These two calls comprise a single message in the thrift-sasl protocol. */
  sendSaslMessage(TSASL_START, const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(
                                   sasl_->getMechanismName().c_str())),
      sasl_->getMechanismName().length(), false);
  sendSaslMessage(TSASL_OK, initialResponse, resLength);

  transport_->flush();
}
}}}

#endif
