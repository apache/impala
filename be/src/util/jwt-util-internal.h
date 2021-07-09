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

#ifndef IMPALA_JWT_UTIL_INTERNAL_H
#define IMPALA_JWT_UTIL_INTERNAL_H

#include <string>
#include <unordered_map>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#pragma clang diagnostic ignored "-Wunused-private-field"
// picojson/picojson.h which is included by jwt-cpp/jwt.h defines __STDC_FORMAT_MACROS
// without checking if it's already defined so un-define the micro here to avoid
// re-definition error. Also need to hide warning "macro name is a reserved identifier".
#ifdef __STDC_FORMAT_MACROS
#undef __STDC_FORMAT_MACROS
#endif
#include <jwt-cpp/jwt.h>
#pragma clang diagnostic pop

#include "common/logging.h"
#include "common/status.h"

namespace impala {

using DecodedJWT = jwt::decoded_jwt<jwt::picojson_traits>;
using JWTVerifier = jwt::verifier<jwt::default_clock, jwt::picojson_traits>;

/// Key-Value map for parsing Json keys.
typedef std::unordered_map<std::string, std::string> JsonKVMap;

class JsonWebKeySet;

/// JWTPublicKey:
/// This class represent cryptographic public key for JSON Web Token (JWT) verification.
class JWTPublicKey {
 public:
  JWTPublicKey(std::string algorithm, std::string pub_key)
    : verifier_(jwt::verify()), algorithm_(algorithm), public_key_(pub_key) {}

  /// Verify the given decoded token.
  Status Verify(const DecodedJWT& decoded_jwt, const std::string& algorithm) const;

  const std::string& get_algorithm() const { return algorithm_; }
  const std::string& get_key() const { return public_key_; }

 protected:
  /// JWT Verifier.
  JWTVerifier verifier_;

 private:
  /// Signing Algorithm:
  /// Currently support following JSON Web Algorithms (JWA):
  /// HS256, HS384, HS512, RS256, RS384, and RS512.
  const std::string algorithm_;
  /// Public key value:
  /// For EC and RSA families of algorithms, it's the public key converted in PEM-encoded
  /// format since jwt-cpp APIs only accept EC/RSA public keys in PEM-encoded format.
  /// For HMAC-SHA2, it's Octet Sequence key representing secret key.
  const std::string public_key_;
};

/// JWT Public Key for HS256.
/// HS256: HMAC using SHA-256.
class HS256JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  HS256JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::hs256(pub_key));
  }
};

/// JWT Public Key for HS384.
/// HS384: HMAC using SHA-384.
class HS384JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw exception if failed to initialize the JWT verifier.
  HS384JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::hs384(pub_key));
  }
};

/// JWT Public Key for HS512.
/// HS512: HMAC using SHA-512.
class HS512JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  HS512JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::hs512(pub_key));
  }
};

/// JWT Public Key for RS256.
/// RS256: RSASSA-PKCS1-v1_5 using SHA-256.
class RS256JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  RS256JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::rs256(pub_key, "", "", ""));
  }
};

/// JWT Public Key for RS384.
/// RS384: RSASSA-PKCS1-v1_5 using SHA-384.
class RS384JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw exception if failed to initialize the JWT verifier.
  RS384JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::rs384(pub_key, "", "", ""));
  }
};

/// JWT Public Key for RS512.
/// RS512: RSASSA-PKCS1-v1_5 using SHA-512.
class RS512JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  RS512JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::rs512(pub_key, "", "", ""));
  }
};

/// JWT Public Key for PS256.
/// PS256: RSASSA-PSS using SHA-256 and MGF1 with SHA-256.
/// RSASSA-PSS is the probabilistic version of RSA.
class PS256JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  PS256JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::ps256(pub_key, "", "", ""));
  }
};

/// JWT Public Key for PS384.
/// PS384: RSASSA-PSS using SHA-384 and MGF1 with SHA-384.
class PS384JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw exception if failed to initialize the JWT verifier.
  PS384JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::ps384(pub_key, "", "", ""));
  }
};

/// JWT Public Key for PS512.
/// PS512: RSASSA-PSS using SHA-512 and MGF1 with SHA-512.
class PS512JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  PS512JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::ps512(pub_key, "", "", ""));
  }
};

/// JWT Public Key for ES256.
/// ES256: ECDSA using P-256 and SHA-256.
class ES256JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  ES256JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::es256(pub_key, "", "", ""));
  }
};

/// JWT Public Key for ES384.
/// ES384: ECDSA using P-384 and SHA-384.
class ES384JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw exception if failed to initialize the JWT verifier.
  ES384JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::es384(pub_key, "", "", ""));
  }
};

/// JWT Public Key for ES512.
/// ES512: ECDSA using P-521 and SHA-512.
class ES512JWTPublicKey : public JWTPublicKey {
 public:
  /// Throw JWT exception if failed to initialize the verifier.
  ES512JWTPublicKey(std::string algorithm, std::string pub_key)
    : JWTPublicKey(algorithm, pub_key) {
    verifier_.allow_algorithm(jwt::algorithm::es512(pub_key, "", "", ""));
  }
};

/// Construct a JWKPublicKey of HS from the JWK.
class HSJWTPublicKeyBuilder {
 public:
  static Status CreateJWKPublicKey(JsonKVMap& kv_map, JWTPublicKey** pub_key_out);
};

/// Construct a JWKPublicKey of RSA from the JWK.
class RSAJWTPublicKeyBuilder {
 public:
  static Status CreateJWKPublicKey(JsonKVMap& kv_map, JWTPublicKey** pub_key_out);

 private:
  /// Convert public key of RSA from JWK format to PEM encoded format by using OpenSSL
  /// APIs.
  static bool ConvertJwkToPem(
      const std::string& base64_n, const std::string& base64_e, std::string& pub_key);
};

/// Construct a JWKPublicKey of EC from the JWK.
class ECJWTPublicKeyBuilder {
 public:
  static Status CreateJWKPublicKey(JsonKVMap& kv_map, JWTPublicKey** pub_key_out);

 private:
  /// Convert public key of EC from JWK format to PEM encoded format by using OpenSSL
  /// APIs.
  static bool ConvertJwkToPem(int eccgrp, const std::string& base64_x,
      const std::string& base64_y, std::string& pub_key);
};

/// JSON Web Key Set (JWKS) conveys the public keys used by the signing party to the
/// clients that need to validate signatures. It represents a cryptographic key set in
/// JSON data structure.
/// This class works as JWT provider, which load the JWKS from file, store keys in an
/// internal maps for each family of algorithms, and provides API to retrieve key by
/// key-id.
/// Init() should be called during the initialization of the daemon. There is no
/// more modification for the instance after Init() return. The class is thread safe.
class JsonWebKeySet {
 public:
  explicit JsonWebKeySet() {}

  /// Map from a key ID (kid) to a JWTPublicKey.
  typedef std::unordered_map<std::string, std::unique_ptr<JWTPublicKey>> JWTPublicKeyMap;

  /// Load JWKS stored in a JSON file. Returns an error if problems were encountered
  /// while parsing/constructing the Json Web keys. If no keys were given in the file,
  /// the internal maps will be empty.
  Status Init(const std::string& jwks_file_path);

  /// Look up the key ID in the internal key maps and returns the key if the lookup was
  /// successful, otherwise return nullptr.
  const JWTPublicKey* LookupRSAPublicKey(const std::string& kid) const;
  const JWTPublicKey* LookupHSKey(const std::string& kid) const;
  const JWTPublicKey* LookupECPublicKey(const std::string& kid) const;

  /// Return number of keys for each family of algorithms.
  int GetHSKeyNum() const { return hs_key_map_.size(); }
  /// Return number of keys for RSA.
  int GetRSAPublicKeyNum() const { return rsa_pub_key_map_.size(); }
  /// Return number of keys for EC.
  int GetECPublicKeyNum() const { return ec_pub_key_map_.size(); }

  /// Return all keys for HS.
  const JWTPublicKeyMap* GetAllHSKeys() const { return &hs_key_map_; }
  /// Return all keys for RSA.
  const JWTPublicKeyMap* GetAllRSAPublicKeys() const { return &rsa_pub_key_map_; }
  /// Return all keys for EC.
  const JWTPublicKeyMap* GetAllECPublicKeys() const { return &ec_pub_key_map_; }

  /// Return TRUE if there is no key.
  bool IsEmpty() const {
    return hs_key_map_.empty() && rsa_pub_key_map_.empty() && ec_pub_key_map_.empty();
  }

 private:
  friend class JWKSetParser;

  /// Following two functions are called inside Init().
  /// Add a RSA public key.
  void AddRSAPublicKey(std::string key_id, JWTPublicKey* jwk_pub_key);
  /// Add a HS key.
  void AddHSKey(std::string key_id, JWTPublicKey* jwk_pub_key);
  /// Add a EC public key.
  void AddECPublicKey(std::string key_id, JWTPublicKey* jwk_pub_key);

  /// Note: According to section 4.5 of RFC 7517 (JSON Web Key), different keys might use
  /// the same "kid" value is if they have different "kty" (key type) values but are
  /// considered to be equivalent alternatives by the application using them. So keys
  /// for each "kty" are saved in different maps.

  /// Octet Sequence keys for HS256 (HMAC using SHA-256), HS384 and HS512.
  /// kty (key type): oct.
  JWTPublicKeyMap hs_key_map_;
  /// Public keys for RSA family of algorithms: RS256, RS384, RS512, PS256, PS384, PS512.
  /// kty (key type): RSA.
  JWTPublicKeyMap rsa_pub_key_map_;
  /// Public keys for EC family of algorithms: ES256, ES384, ES512.
  /// kty (key type): EC.
  JWTPublicKeyMap ec_pub_key_map_;
};

} // namespace impala

#endif
