// This file will be removed when the code is accepted into the Thrift library.
/****************************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License") { you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/
#include "config.h"
#ifdef HAVE_SASL_SASL_H

#include <transport/TSasl.h>

using namespace std;

namespace sasl {
uint8_t* TSasl::unwrap(const uint8_t* incoming,
                       const int offset, const uint32_t len, uint32_t* outLen) {
  uint32_t outputlen;
  uint8_t* output;
  int result;

  result = sasl_decode(conn,
                      (const char*)incoming, len, (const char**)&output, &outputlen);
  if (result != SASL_OK) {
    throw SaslException(sasl_errdetail(conn));
  }
  *outLen = outputlen;
  return output;
}

uint8_t* TSasl::wrap(const uint8_t* outgoing,
                     const int offset, const uint32_t len, uint32_t* outLen) {
  uint32_t outputlen;
  uint8_t* output;
  int result;

  result = sasl_encode(conn, (const char*)outgoing+offset,
                       len, (const char**)&output, &outputlen);
  if (result != SASL_OK) {
    throw SaslException(sasl_errdetail(conn));
  }
  *outLen = outputlen;
  return output;
}

TSaslClient::TSaslClient(const string& mechanisms, const string& authenticationId,
    const string& protocol, const string& serverName, const map<string,string>& props, 
    sasl_callback_t* callbacks) {
  conn = NULL;
  if (!props.empty()) {
    throw SaslServerImplException("Properties not yet supported");
  }
  int result = sasl_client_new(protocol.c_str(), serverName.c_str(),
			   NULL, NULL, callbacks, 0, &conn);
  if (result != SASL_OK) {
    if (conn) {
      throw SaslServerImplException(sasl_errdetail(conn));
    } else {
      throw SaslServerImplException(sasl_errstring(result, NULL, NULL));
    }
  }
  
  if (!authenticationId.empty()) {
    /* TODO: setup security property */
    /*
    sasl_security_properties_t secprops;
    // populate  secprops
    result = sasl_setprop(conn, SASL_AUTH_EXTERNAL, authenticationId.c_str());
    */
  }

  chosenMech = mechList = mechanisms;
  authComplete = false;
  clientStarted = false;
}

TSaslClient::~TSaslClient() {
  sasl_dispose(&conn);
}


/* Evaluates the challenge data and generates a response. */
uint8_t* TSaslClient::evaluateChallengeOrResponse(
    const uint8_t* challenge, const uint32_t len, uint32_t *resLen) {
  sasl_interact_t* client_interact=NULL;
  uint8_t* out=NULL;
  uint32_t outlen=0;
  uint32_t result;
  char* mechUsing;

  if (!clientStarted) {
    result=sasl_client_start(conn,
          mechList.c_str(),
          &client_interact, /* filled in if an interaction is needed */
          (const char**)&out,      /* filled in on success */
          &outlen,   /* filled in on success */
          (const char**)&mechUsing);
    clientStarted = true;
    chosenMech = mechUsing;
  } else {
    if (len  > 0) {
      result=sasl_client_step(conn,  /* our context */
          (const char*)challenge,    /* the data from the server */
          len, /* its length */
          &client_interact,  /* this should be unallocated and NULL */
          (const char**)&out,     /* filled in on success */
          &outlen); /* filled in on success */
    } else {
      result = SASL_CONTINUE;
    }
  }

  if (result == SASL_OK) {
    authComplete = true;
  } else if (result != SASL_CONTINUE) {
    throw SaslClientImplException(sasl_errdetail(conn));
  }
  *resLen = outlen;
  return (uint8_t*)out;
}

/* Returns the IANA-registered mechanism name of this SASL client. */
string TSaslClient::getMechanismName() {
  return chosenMech;
}

/* Retrieves the negotiated property */
string	TSaslClient::getNegotiatedProperty(const string& propName) {
  return NULL;
}

/* Determines whether this mechanism has an optional initial response. */
bool TSaslClient::hasInitialResponse() {
  // TODO: Need to return a value based on the mechanism.
  return true;
}

TSaslServer::TSaslServer(const string& service, const string& serverFQDN,
                         const string& userRealm,
                         unsigned flags, sasl_callback_t* callbacks) {
  conn = NULL;
  int result = sasl_server_new(service.c_str(),
      serverFQDN.size() == 0 ? NULL : serverFQDN.c_str(),
      userRealm.size() == 0 ? NULL :userRealm.c_str(),
      NULL, NULL, callbacks, flags, &conn);
  if (result != SASL_OK) {
    if (conn) {
      throw SaslServerImplException(sasl_errdetail(conn));
    } else {
      throw SaslServerImplException(sasl_errstring(result, NULL, NULL));
    }
  }

  authComplete = false;
  serverStarted = false;
}

uint8_t* TSaslServer::evaluateChallengeOrResponse(const uint8_t* response,
                                                  const uint32_t len, uint32_t* resLen) {
  uint8_t* out = NULL;
  uint32_t outlen = 0;
  uint32_t result;

  if (!serverStarted) {
    result = sasl_server_start(conn,
        (const char *)response, NULL, 0, (const char **)&out, &outlen);
  } else {
    result = sasl_server_step(conn,
        (const char*)response, len, (const char**)&out, &outlen);
  }

  if (result == SASL_OK) {
    authComplete = true;
  } else if (result != SASL_CONTINUE) {
    throw SaslServerImplException(sasl_errdetail(conn));
  }
  serverStarted = true;

  *resLen = outlen;
  return out;
}

TSaslServer::~TSaslServer() {
  sasl_dispose(&conn);
}
};
#endif
