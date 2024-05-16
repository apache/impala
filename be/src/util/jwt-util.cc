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

#include <string.h>
#include <cerrno>
#include <ostream>
#include <unordered_map>
#include <vector>
#include <sys/stat.h>

#include <boost/algorithm/string.hpp>
#include <gutil/strings/escaping.h>
#include <gutil/strings/substitute.h>
#include <openssl/bio.h>
#include <openssl/ec.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rand.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/filereadstream.h>

#include "common/names.h"
#include "hash-util.h"
#include "jwt-util-internal.h"
#include "jwt-util.h"
#include "kudu/util/curl_util.h"
#include "kudu/util/faststring.h"
#include "kudu/util/monotime.h"
#include "util/kudu-status-util.h"
#include "util/test-info.h"

DECLARE_int32(jwks_update_frequency_s);
DECLARE_int32(jwks_pulling_timeout_s);

// Support only a single x5c certificate.
// Update MAX_X5C_CERTIFICATES when we can
// support more than one.
#define MAX_X5C_CERTIFICATES 1
static const char* ARRAY_TYPE = "Array";

namespace impala {

using rapidjson::Document;
using rapidjson::Value;

// JWK Set (JSON Web Key Set) is JSON data structure that represents a set of JWKs.
// This class parses JWKS file.
class JWKSetParser {
 public:
  JWKSetParser(JWKSSnapshot* jwks) : jwks_(jwks) {}

  // Perform the parsing and populate JWKS's internal map. Return error status if
  // encountering any error.
  Status Parse(const Document& rules_doc) {
    bool found_keys = false;
    for (Value::ConstMemberIterator member = rules_doc.MemberBegin();
         member != rules_doc.MemberEnd(); ++member) {
      if (strcmp("keys", member->name.GetString()) == 0) {
        found_keys = true;
        RETURN_IF_ERROR(ParseKeys(member->value));
      } else {
        return Status(TErrorCode::JWKS_PARSE_ERROR,
            Substitute(
                "Unexpected property '$0' must be removed", member->name.GetString()));
      }
    }
    if (!found_keys) {
      return Status(TErrorCode::JWKS_PARSE_ERROR, "An array of keys is required");
    }
    return Status::OK();
  }

 private:
  JWKSSnapshot* jwks_;

  string NameOfTypeOfJsonValue(const Value& value) {
    switch (value.GetType()) {
      case rapidjson::kNullType:
        return "Null";
      case rapidjson::kFalseType:
      case rapidjson::kTrueType:
        return "Bool";
      case rapidjson::kObjectType:
        return "Object";
      case rapidjson::kArrayType:
        return "Array";
      case rapidjson::kStringType:
        return "String";
      case rapidjson::kNumberType:
        if (value.IsInt()) return "Integer";
        if (value.IsDouble()) return "Float";
        [[fallthrough]];
      default:
        DCHECK(false);
        return "Unknown";
    }
  }

  // Parse an array of keys.
  Status ParseKeys(const Value& keys) {
    if (!keys.IsArray()) {
      return Status(TErrorCode::JWKS_PARSE_ERROR,
          Substitute(
              "'keys' must be of type Array but is a '$0'", NameOfTypeOfJsonValue(keys)));
    } else if (keys.Size() == 0) {
      return Status(
          TErrorCode::JWKS_PARSE_ERROR, Substitute("'keys' must be a non empty Array"));
    }
    for (rapidjson::SizeType key_idx = 0; key_idx < keys.Size(); ++key_idx) {
      const Value& key = keys[key_idx];
      if (!key.IsObject()) {
        return Status(TErrorCode::JWKS_PARSE_ERROR,
            Substitute("parsing key #$0, key should be a JSON Object but is a '$1'.",
                key_idx, NameOfTypeOfJsonValue(key)));
      }
      Status status = ParseKey(key);
      if (!status.ok()) {
        Status parse_status(
            TErrorCode::JWKS_PARSE_ERROR, Substitute("parsing key #$0, ", key_idx));
        parse_status.MergeStatus(status);
        return parse_status;
      }
    }
    return Status::OK();
  }

  // Parse a public key and populate JWKS's internal map.
  Status ParseKey(const Value& json_key) {
    std::unordered_map<std::string, std::string> kv_map;
    for (Value::ConstMemberIterator member = json_key.MemberBegin();
         member != json_key.MemberEnd(); ++member) {
      string k, v, values[MAX_X5C_CERTIFICATES];
      k = string(member->name.GetString());
      const Value& json_value = json_key[k.c_str()];
      if (NameOfTypeOfJsonValue(json_value) == ARRAY_TYPE) {
        RETURN_IF_ERROR(ReadKeyArrayProperty(k.c_str(), json_key, values,
            /*required*/ false));
        // If x5c certificate is present, pick up the first element
        if (!values[0].empty()) {
          v = values[0];
        } else {
          return Status(Substitute("$0 property must be a non-empty array", k));
        }
      } else {
        RETURN_IF_ERROR(ReadKeyProperty(k.c_str(), json_key, &v, /*required*/ false));
      }
      if (kv_map.find(k) == kv_map.end()) {
        kv_map.insert(make_pair(k, v));
      } else {
        LOG(WARNING) << "Duplicate property of JWK: " << k;
      }
    }

    auto it_kty = kv_map.find("kty");
    if (it_kty == kv_map.end()) return Status("'kty' property is required");
    auto it_kid = kv_map.find("kid");
    if (it_kid == kv_map.end()) return Status("'kid' property is required");
    string key_id = it_kid->second;
    if (key_id.empty()) {
      return Status(Substitute("'kid' property must be a non-empty string"));
    }

    Status status;
    string key_type = boost::algorithm::to_lower_copy(it_kty->second);
    if (key_type == "oct") {
      JWTPublicKey* jwt_pub_key;
      status = HSJWTPublicKeyBuilder::CreateJWKPublicKey(kv_map, &jwt_pub_key);
      if (status.ok()) jwks_->AddHSKey(key_id, jwt_pub_key);
    } else if (key_type == "rsa") {
      JWTPublicKey* jwt_pub_key;
      status = RSAJWTPublicKeyBuilder::CreateJWKPublicKey(kv_map, &jwt_pub_key);
      if (status.ok()) jwks_->AddRSAPublicKey(key_id, jwt_pub_key);
    } else if (key_type == "ec") {
      JWTPublicKey* jwt_pub_key;
      status = ECJWTPublicKeyBuilder::CreateJWKPublicKey(kv_map, &jwt_pub_key);
      if (status.ok()) jwks_->AddECPublicKey(key_id, jwt_pub_key);
    } else {
      return Status(Substitute("Unsupported kty: '$0'", key_type));
    }
    return status;
  }

  // Reads a key property of the given name and assigns the property value to the out
  // parameter. A true return value indicates success.
  template <typename T>
  Status ReadKeyProperty(
      const string& name, const Value& json_key, T* value, bool required = true) {
    const Value& json_value = json_key[name.c_str()];
    if (json_value.IsNull()) {
      if (required) {
        return Status(Substitute("'$0' property is required and cannot be null", name));
      } else {
        return Status::OK();
      }
    }
    return ValidateTypeAndExtractValue(name, json_value, value);
  }

  // Reads a key property of type array of the given name and assigns the property
  // value to the out parameter. A true return value indicates success.
  template <typename T>
  Status ReadKeyArrayProperty(
      const string& name, const Value& json_key, T* value, bool required = true) {
    const Value& json_value = json_key[name.c_str()];
    if (json_value.IsNull()) {
      if (required) {
        return Status(Substitute("'$0' property is required and cannot be null", name));
      } else {
        return Status::OK();
      }
    }
    return ValidateTypeAndExtractArrayValue(name, json_value, value);
  }

// Extract a value stored in a rapidjson::Value and assign it to the out parameter.
// The type will be validated before extraction. A true return value indicates success.
// The name parameter is only used to generate an error message upon failure.
#define EXTRACT_VALUE(json_type, cpp_type)                                             \
  Status ValidateTypeAndExtractValue(                                                  \
      const string& name, const Value& json_value, cpp_type* value) {                  \
    if (!json_value.Is##json_type()) {                                                 \
      return Status(                                                                   \
          Substitute("'$0' property must be of type " #json_type " but is a $1", name, \
              NameOfTypeOfJsonValue(json_value)));                                     \
    }                                                                                  \
    *value = json_value.Get##json_type();                                              \
    return Status::OK();                                                               \
  }

// Extract a value stored in a rapidjson::Value and assign it to the out parameter.
// The type will be validated before extraction. Upon success, status is returned as OK.
// The name parameter is only used to generate an error message upon failure.
#define EXTRACT_ARRAY_VALUE(json_type, cpp_type)                                       \
  Status ValidateTypeAndExtractArrayValue(                                             \
      const string& name, const Value& json_value, cpp_type* value) {                  \
    if (!json_value.Is##json_type()) {                                                 \
      return Status(                                                                   \
          Substitute("'$0' property must be of type " #json_type " but is a $1", name, \
              NameOfTypeOfJsonValue(json_value)));                                     \
    }                                                                                  \
                                                                                       \
    for (size_t i = 0; i < json_value.Size() && i < MAX_X5C_CERTIFICATES; i++)  {      \
      value[i] = json_value[i].GetString();                                            \
    }                                                                                  \
    return Status::OK();                                                               \
  }                                                                                    \

  EXTRACT_VALUE(String, string)
  EXTRACT_ARRAY_VALUE(Array, string)
  // EXTRACT_VALUE(Bool, bool)
};

//
// JWTPublicKey member functions.
//
// Verify JWT's signature for the given decoded token with jwt-cpp API.
Status JWTPublicKey::Verify(
    const DecodedJWT& decoded_jwt, const std::string& algorithm) const {
  // Verify if algorithms are matching.
  if (algorithm_.compare(algorithm) != 0) {
    return Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("JWT algorithm '$0' is not matching with JWK algorithm '$1'",
            algorithm, algorithm_));
  }

  Status status;
  try {
    // Call jwt-cpp API to verify token's signature.
    verifier_.verify(decoded_jwt);
  } catch (const jwt::error::rsa_exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED, Substitute("RSA error: $0", e.what()));
  } catch (const jwt::error::token_verification_exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Verification failed, error: $0", e.what()));
  } catch (const std::exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Varification failed, error: $0", e.what()));
  }
  return status;
}

// Create a JWKPublicKey of HS from the JWK.
Status HSJWTPublicKeyBuilder::CreateJWKPublicKey(
    JsonKVMap& kv_map, JWTPublicKey** pub_key_out) {
  // Octet Sequence keys for HS256, HS384 or HS512.
  // JWK Sample:
  // {
  //   "kty":"oct",
  //   "alg":"HS256",
  //   "k":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
  //   "kid":"Id that can be uniquely Identified"
  // }
  auto it_alg = kv_map.find("alg");
  if (it_alg == kv_map.end()) return Status("'alg' property is required");
  string algorithm = boost::algorithm::to_lower_copy(it_alg->second);
  if (algorithm.empty()) {
    return Status(Substitute("'alg' property must be a non-empty string"));
  }
  auto it_k = kv_map.find("k");
  if (it_k == kv_map.end()) return Status("'k' property is required");
  if (it_k->second.empty()) {
    return Status(Substitute("'k' property must be a non-empty string"));
  }

  Status status;
  JWTPublicKey* jwt_pub_key = nullptr;
  try {
    if (algorithm == "hs256") {
      jwt_pub_key = new HS256JWTPublicKey(algorithm, it_k->second);
    } else if (algorithm == "hs384") {
      jwt_pub_key = new HS384JWTPublicKey(algorithm, it_k->second);
    } else if (algorithm == "hs512") {
      jwt_pub_key = new HS512JWTPublicKey(algorithm, it_k->second);
    } else {
      return Status(Substitute("Invalid 'alg' property value: '$0'", algorithm));
    }
  } catch (const std::exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Failed to initialize verifier, error: $0", e.what()));
  }
  if (!status.ok()) return status;
  *pub_key_out = jwt_pub_key;
  return Status::OK();
}

// Create a JWKPublicKey of RSA from the JWK.
Status RSAJWTPublicKeyBuilder::CreateJWKPublicKey(
    JsonKVMap& kv_map, JWTPublicKey** pub_key_out) {
  // JWK Sample:
  // {
  //   "kty":"RSA",
  //   "alg":"RS256",
  //   "n":"sttddbg-_yjXzcFpbMJB1fI9...Q_QDhvqXx8eQ1r9smM",
  //   "e":"AQAB",
  //   "kid":"Id that can be uniquely Identified"
  // }
  string pub_key_str, algorithm;
  auto it_n = kv_map.find("n");
  auto it_e = kv_map.find("e");
  if (it_n == kv_map.end() || it_e == kv_map.end()) {
    return Status("'n' and 'e' properties are required");
  } else if (it_n->second.empty() || it_e->second.empty()) {
    return Status("'n' and 'e' properties must be a non-empty string");
  }
  // Converts public key to PEM encoded form.
  string pub_key;
  if (!ConvertJwkToPem(it_n->second, it_e->second, pub_key)) {
    return Status(
        Substitute("Invalid public key 'n':'$0', 'e':'$1'", it_n->second, it_e->second));
  }

  // if jwk contains "x5c", then use it instead of "n" and "e"
  // to construct the public key
  auto it_x5c = kv_map.find("x5c");
  if (it_x5c != kv_map.end()) {
    pub_key_str = jwt::helper::convert_base64_der_to_pem(it_x5c->second);
    // if "x5c" is present but "alg" is not present, extract it from x5c certificate.
    auto it_alg = kv_map.find("alg");
    if (it_alg == kv_map.end()) {
      const char *data = pub_key_str.c_str();
      BIO *bio = BIO_new(BIO_s_mem());
      if (!bio) {
        return Status(Substitute("Failed to allocate memory for BIO"));
      }
      BIO_puts(bio, data);
      X509 *cert = PEM_read_bio_X509(bio, NULL, NULL, NULL);
      if (!cert) {
        BIO_free(bio);
        return Status(Substitute("Invalid x5c certificate"));
      }
#if OPENSSL_VERSION_NUMBER >= 0x10002000L
      int pkey_nid = X509_get_signature_nid(cert);
#else
      // Older version of OpenSSL appear not to have a public way to get the
      // signature digest method from a certificate. Instead, we reach into the
      // 'private' internals.
      int pkey_nid = OBJ_obj2nid(cert->sig_alg->algorithm);
#endif
      std::string sigalg(OBJ_nid2ln(pkey_nid));
      if (sigalg == "sha256WithRSAEncryption") {
        algorithm = "rs256";
      } else if (sigalg == "sha384WithRSAEncryption") {
        algorithm = "rs384";
      } else if (sigalg == "sha512WithRSAEncryption") {
        algorithm = "rs512";
      } else {
        BIO_free(bio);
        return Status(Substitute("Unsupported alg $0 in signature", sigalg));
      }
      BIO_free(bio);
      X509_free(cert);
    } else {
      algorithm = boost::algorithm::to_lower_copy(it_alg->second);
    }
  } else {
    // if "x5c" is not present "alg" must be present.
    pub_key_str = pub_key;
    auto it_alg = kv_map.find("alg");
    if (it_alg == kv_map.end()) return Status("'alg' property is required");
    algorithm = boost::algorithm::to_lower_copy(it_alg->second);
  }

  if (algorithm.empty()) {
    return Status(Substitute("'alg' property must be a non-empty string"));
  }
  Status status;
  JWTPublicKey* jwt_pub_key = nullptr;
  try {
    if (algorithm == "rs256") {
      jwt_pub_key = new RS256JWTPublicKey(algorithm, pub_key_str);
    } else if (algorithm == "rs384") {
      jwt_pub_key = new RS384JWTPublicKey(algorithm, pub_key_str);
    } else if (algorithm == "rs512") {
      jwt_pub_key = new RS512JWTPublicKey(algorithm, pub_key_str);
    } else if (algorithm == "ps256") {
      jwt_pub_key = new PS256JWTPublicKey(algorithm, pub_key_str);
    } else if (algorithm == "ps384") {
      jwt_pub_key = new PS384JWTPublicKey(algorithm, pub_key_str);
    } else if (algorithm == "ps512") {
      jwt_pub_key = new PS512JWTPublicKey(algorithm, pub_key_str);
    } else {
      return Status(Substitute("Invalid 'alg' property value: '$0'", algorithm));
    }
  } catch (const jwt::error::rsa_exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED, Substitute("RSA error: $0", e.what()));
  } catch (const std::exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Failed to initialize verifier, error: $0", e.what()));
  }
  if (!status.ok()) return status;
  *pub_key_out = jwt_pub_key;
  return Status::OK();
}

// Convert public key of RSA from JWK format to PEM encoded format by using OpenSSL APIs.
bool RSAJWTPublicKeyBuilder::ConvertJwkToPem(
    const std::string& base64_n, const std::string& base64_e, std::string& pub_key) {
  pub_key.clear();
  string str_n, str_e;
  if (!WebSafeBase64Unescape(base64_n, &str_n)) return false;
  if (!WebSafeBase64Unescape(base64_e, &str_e)) return false;
  BIGNUM* modul = BN_bin2bn((const unsigned char*)str_n.c_str(), str_n.size(), nullptr);
  BIGNUM* expon = BN_bin2bn((const unsigned char*)str_e.c_str(), str_e.size(), nullptr);

  RSA* rsa = RSA_new();
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  rsa->n = modul;
  rsa->e = expon;
#else
  // RSA_set0_key is a new API introduced in OpenSSL version 1.1
  RSA_set0_key(rsa, modul, expon, nullptr);
#endif

  unsigned char desc[1024];
  memset(desc, 0, 1024);
  BIO* bio = BIO_new(BIO_s_mem());
  PEM_write_bio_RSA_PUBKEY(bio, rsa);
  if (BIO_read(bio, desc, 1024) > 0) {
    pub_key = (char*)desc;
    // Remove last '\n'.
    if (pub_key.length() > 0 && pub_key[pub_key.length() - 1] == '\n') pub_key.pop_back();
  }
  BIO_free(bio);
  RSA_free(rsa);
  return !pub_key.empty();
}

// Create a JWKPublicKey of EC (ES256, ES384 or ES512) from the JWK.
Status ECJWTPublicKeyBuilder::CreateJWKPublicKey(
    JsonKVMap& kv_map, JWTPublicKey** pub_key_out) {
  // JWK Sample:
  // {
  //   "kty":"EC",
  //   "crv":"P-256",
  //   "x":"f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
  //   "y":"x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0",
  //   "kid":"Id that can be uniquely Identified"
  // }
  string algorithm;
  int eccgrp;
  auto it_crv = kv_map.find("crv");
  if (it_crv != kv_map.end()) {
    string curve = boost::algorithm::to_upper_copy(it_crv->second);
    if (curve == "P-256") {
      algorithm = "es256";
      eccgrp = NID_X9_62_prime256v1;
    } else if (curve == "P-384") {
      algorithm = "es384";
      eccgrp = NID_secp384r1;
    } else if (curve == "P-521") {
      algorithm = "es512";
      eccgrp = NID_secp521r1;
    } else {
      return Status(Substitute("Unsupported crv: '$0'", curve));
    }
  } else {
    auto it_alg = kv_map.find("alg");
    if (it_alg == kv_map.end()) {
      return Status("'alg' or 'crv' property is required");
    }
    algorithm = boost::algorithm::to_lower_copy(it_alg->second);
    if (algorithm.empty()) {
      return Status(Substitute("'alg' property must be a non-empty string"));
    } else if (algorithm == "es256") {
      // ECDSA using P-256 and SHA-256 (OBJ_txt2nid("prime256v1")).
      eccgrp = NID_X9_62_prime256v1;
    } else if (algorithm == "es384") {
      // ECDSA using P-384 and SHA-384 (OBJ_txt2nid("secp384r1")).
      eccgrp = NID_secp384r1;
    } else if (algorithm == "es512") {
      // ECDSA using P-521 and SHA-512 (OBJ_txt2nid("secp521r1")).
      eccgrp = NID_secp521r1;
    } else {
      return Status(Substitute("Unsupported alg: '$0'", algorithm));
    }
  }

  auto it_x = kv_map.find("x");
  auto it_y = kv_map.find("y");
  if (it_x == kv_map.end() || it_y == kv_map.end()) {
    return Status("'x' and 'y' properties are required");
  } else if (it_x->second.empty() || it_y->second.empty()) {
    return Status("'x' and 'y' properties must be a non-empty string");
  }
  // Converts public key to PEM encoded form.
  string pub_key;
  if (!ConvertJwkToPem(eccgrp, it_x->second, it_y->second, pub_key)) {
    return Status(
        Substitute("Invalid public key 'x':'$0', 'y':'$1'", it_x->second, it_y->second));
  }

  Status status;
  JWTPublicKey* jwt_pub_key = nullptr;
  try {
    if (algorithm == "es256") {
      jwt_pub_key = new ES256JWTPublicKey(algorithm, pub_key);
    } else if (algorithm == "es384") {
      jwt_pub_key = new ES384JWTPublicKey(algorithm, pub_key);
    } else {
      DCHECK(algorithm == "es512");
      jwt_pub_key = new ES512JWTPublicKey(algorithm, pub_key);
    }
  } catch (const jwt::error::ecdsa_exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED, Substitute("EC error: $0", e.what()));
  } catch (const std::exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Failed to initialize verifier, error: $0", e.what()));
  }
  if (!status.ok()) return status;
  *pub_key_out = jwt_pub_key;
  return Status::OK();
}

// Convert public key of EC from JWK format to PEM encoded format by using OpenSSL APIs.
bool ECJWTPublicKeyBuilder::ConvertJwkToPem(int eccgrp, const std::string& base64_x,
    const std::string& base64_y, std::string& pub_key) {
  pub_key.clear();
  string ascii_x, ascii_y;
  if (!WebSafeBase64Unescape(base64_x, &ascii_x)) return false;
  if (!WebSafeBase64Unescape(base64_y, &ascii_y)) return false;
  BIGNUM* x = BN_bin2bn((const unsigned char*)ascii_x.c_str(), ascii_x.size(), nullptr);
  BIGNUM* y = BN_bin2bn((const unsigned char*)ascii_y.c_str(), ascii_y.size(), nullptr);

  BIO* bio = nullptr;
  EC_KEY* ecKey = EC_KEY_new_by_curve_name(eccgrp);
  EC_KEY_set_asn1_flag(ecKey, OPENSSL_EC_NAMED_CURVE);
  if (EC_KEY_set_public_key_affine_coordinates(ecKey, x, y) == 0) goto cleanup;

  unsigned char desc[1024];
  memset(desc, 0, 1024);
  bio = BIO_new(BIO_s_mem());
  if (PEM_write_bio_EC_PUBKEY(bio, ecKey) != 0) {
    if (BIO_read(bio, desc, 1024) > 0) {
      pub_key = (char*)desc;
      // Remove last '\n'.
      if (pub_key.length() > 0 && pub_key[pub_key.length() - 1] == '\n') {
        pub_key.pop_back();
      }
    }
  }

cleanup:
  if (bio != nullptr) BIO_free(bio);
  EC_KEY_free(ecKey);
  BN_free(x);
  BN_free(y);
  return !pub_key.empty();
}

//
// JWKSSnapshot member functions.
//

// Load JWKS from the given local json file.
Status JWKSSnapshot::LoadKeysFromFile(const string& jwks_file_path) {
  hs_key_map_.clear();
  rsa_pub_key_map_.clear();

  // Read the file.
  FILE* jwks_file = fopen(jwks_file_path.c_str(), "r");
  if (jwks_file == nullptr) {
    return Status(
        Substitute("Could not open JWKS file '$0'; $1", jwks_file_path, strerror(errno)));
  }
  // Check for an empty file and ignore it.
  struct stat jwks_file_stats;
  if (fstat(fileno(jwks_file), &jwks_file_stats)) {
    fclose(jwks_file);
    return Status(
        Substitute("Error reading JWKS file '$0'; $1", jwks_file_path, strerror(errno)));
  }
  if (jwks_file_stats.st_size == 0) {
    fclose(jwks_file);
    return Status::OK();
  }

  char readBuffer[65536];
  rapidjson::FileReadStream stream(jwks_file, readBuffer, sizeof(readBuffer));
  Document jwks_doc;
  jwks_doc.ParseStream(stream);
  fclose(jwks_file);
  if (jwks_doc.HasParseError()) {
    return Status(
        TErrorCode::JWKS_PARSE_ERROR, GetParseError_En(jwks_doc.GetParseError()));
  } else if (!jwks_doc.IsObject()) {
    return Status(TErrorCode::JWKS_PARSE_ERROR, "root element must be a JSON Object");
  } else if (!jwks_doc.HasMember("keys")) {
    return Status(TErrorCode::JWKS_PARSE_ERROR, "keys is required");
  }

  JWKSetParser jwks_parser(this);
  return jwks_parser.Parse(jwks_doc);
}

// Download JWKS from the given URL with Kudu's EasyCurl wrapper.
Status JWKSSnapshot::LoadKeysFromUrl(
    const std::string& jwks_url, bool jwks_verify_server_certificate,
    const std::string& jwks_ca_certificate, uint64_t cur_jwks_checksum,
    bool* is_changed) {
  kudu::EasyCurl curl;
  kudu::faststring dst;
  Status status;

  curl.set_timeout(
      kudu::MonoDelta::FromMilliseconds(FLAGS_jwks_pulling_timeout_s * 1000));
  curl.set_verify_peer(jwks_verify_server_certificate);
  curl.set_ca_certificates(jwks_ca_certificate);
  // TODO support CurlAuthType by calling kudu::EasyCurl::set_auth().
  KUDU_RETURN_IF_ERROR(curl.FetchURL(jwks_url, &dst),
      Substitute("Error downloading JWKS from '$0'", jwks_url));
  if (dst.size() > 0) {
    // Verify if the checksum of the downloaded JWKS has been changed.
    jwks_checksum_ = HashUtil::FastHash64(dst.data(), dst.size(), /*seed*/ 0xcafebeef);
    if (jwks_checksum_ == cur_jwks_checksum) return Status::OK();
    *is_changed = true;
    // Append '\0' so that the in-memory object could be parsed as StringStream.
    dst.push_back('\0');
#ifndef NDEBUG
    VLOG(3) << "JWKS: " << dst.data();
#endif
    // Parse in-memory JWKS JSON object as StringStream.
    Document jwks_doc;
    jwks_doc.Parse((char*)dst.data());
    if (jwks_doc.HasParseError()) {
      status = Status(
          TErrorCode::JWKS_PARSE_ERROR, GetParseError_En(jwks_doc.GetParseError()));
    } else if (!jwks_doc.IsObject()) {
      status = Status(TErrorCode::JWKS_PARSE_ERROR, "root element must be a JSON Object");
    } else if (!jwks_doc.HasMember("keys")) {
      status = Status(TErrorCode::JWKS_PARSE_ERROR, "keys is required");
    } else {
      // Load and initialize public keys.
      JWKSetParser jwks_parser(this);
      status = jwks_parser.Parse(jwks_doc);
    }
  }
  return status;
}

void JWKSSnapshot::AddHSKey(std::string key_id, JWTPublicKey* jwk_pub_key) {
  if (hs_key_map_.find(key_id) == hs_key_map_.end()) {
    hs_key_map_[key_id].reset(jwk_pub_key);
  } else {
    LOG(WARNING) << "Duplicate key ID of JWK for HS key: " << key_id;
  }
}

void JWKSSnapshot::AddRSAPublicKey(std::string key_id, JWTPublicKey* jwk_pub_key) {
  if (rsa_pub_key_map_.find(key_id) == rsa_pub_key_map_.end()) {
    rsa_pub_key_map_[key_id].reset(jwk_pub_key);
  } else {
    LOG(WARNING) << "Duplicate key ID of JWK for RSA public key: " << key_id;
  }
}

void JWKSSnapshot::AddECPublicKey(std::string key_id, JWTPublicKey* jwk_pub_key) {
  if (ec_pub_key_map_.find(key_id) == ec_pub_key_map_.end()) {
    ec_pub_key_map_[key_id].reset(jwk_pub_key);
  } else {
    LOG(WARNING) << "Duplicate key ID of JWK for EC public key: " << key_id;
  }
}

const JWTPublicKey* JWKSSnapshot::LookupHSKey(const std::string& kid) const {
  auto find_it = hs_key_map_.find(kid);
  if (find_it == hs_key_map_.end()) {
    // Could not find key for the given key ID.
    return nullptr;
  }
  return find_it->second.get();
}

const JWTPublicKey* JWKSSnapshot::LookupRSAPublicKey(const std::string& kid) const {
  auto find_it = rsa_pub_key_map_.find(kid);
  if (find_it == rsa_pub_key_map_.end()) {
    // Could not find key for the given key ID.
    return nullptr;
  }
  return find_it->second.get();
}

const JWTPublicKey* JWKSSnapshot::LookupECPublicKey(const std::string& kid) const {
  auto find_it = ec_pub_key_map_.find(kid);
  if (find_it == ec_pub_key_map_.end()) {
    // Could not find key for the given key ID.
    return nullptr;
  }
  return find_it->second.get();
}

//
// JWKSMgr member functions.
//

JWKSMgr::~JWKSMgr() {
  shut_down_promise_.Set(true);
  if (jwks_update_thread_ != nullptr) jwks_update_thread_->Join();
}

Status JWKSMgr::Init(const std::string& jwks_uri, bool jwks_verify_server_certificate,
    const std::string& jwks_ca_certificate, bool is_local_file) {
  Status status;
  jwks_uri_ = jwks_uri;
  jwks_verify_server_certificate_ = jwks_verify_server_certificate;
  jwks_ca_certificate_ = jwks_ca_certificate;
  std::shared_ptr<JWKSSnapshot> new_jwks = std::make_shared<JWKSSnapshot>();
  if (is_local_file) {
    status = new_jwks->LoadKeysFromFile(jwks_uri);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to load JWKS: " << status;
      return status;
    }
    SetJWKSSnapshot(new_jwks);
  } else {
    if (FLAGS_jwks_update_frequency_s <= 0) {
      LOG(WARNING) << "Invalid value for flag jwks_update_frequency_s: "
                   << FLAGS_jwks_update_frequency_s << ", use default value 60.";
      FLAGS_jwks_update_frequency_s = 60;
    }
    if (FLAGS_jwks_pulling_timeout_s <= 0) {
      LOG(WARNING) << "Invalid value for flag jwks_pulling_timeout_s: "
                   << FLAGS_jwks_pulling_timeout_s << ", use default value 10.";
      FLAGS_jwks_pulling_timeout_s = 10;
    }

    bool is_changed = false;
    status = new_jwks->LoadKeysFromUrl(jwks_uri, jwks_verify_server_certificate,
        jwks_ca_certificate, current_jwks_checksum_, &is_changed);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to load JWKS: " << status;
      return status;
    }
    DCHECK(is_changed);
    if (is_changed) SetJWKSSnapshot(new_jwks);

    // Start a working thread to periodically check the JWKS URL for updates.
    RETURN_IF_ERROR(Thread::Create("impala-server", "JWKS-mgr",
        &JWKSMgr::UpdateJWKSThread, this, &jwks_update_thread_));
  }

  if (new_jwks->IsEmpty()) LOG(WARNING) << "JWKS file is empty.";
  return Status::OK();
}

void JWKSMgr::UpdateJWKSThread() {
  std::shared_ptr<JWKSSnapshot> new_jwks;
  int64_t timeout_millis = FLAGS_jwks_update_frequency_s * 1000;
  while (true) {
    // This Get() will time out until shutdown, when the promise is set.
    bool timed_out;
    shut_down_promise_.Get(timeout_millis, &timed_out);
    if (!timed_out) break;

    new_jwks = std::make_shared<JWKSSnapshot>();
    bool is_changed = false;
    Status status =
        new_jwks->LoadKeysFromUrl(jwks_uri_, jwks_verify_server_certificate_,
            jwks_ca_certificate_, current_jwks_checksum_, &is_changed);
    if (!status.ok()) {
      LOG(WARNING) << "Failed to update JWKS: " << status;
    } else if (is_changed) {
      SetJWKSSnapshot(new_jwks);
    }
    new_jwks.reset();
  }
  // The promise must be set to true.
  DCHECK(shut_down_promise_.IsSet());
  DCHECK(shut_down_promise_.Get());
}

JWKSSnapshotPtr JWKSMgr::GetJWKSSnapshot() const {
  std::lock_guard<std::mutex> l(current_jwks_lock_);
  DCHECK(current_jwks_.get() != nullptr);
  JWKSSnapshotPtr jwks = current_jwks_;
  return jwks;
}

void JWKSMgr::SetJWKSSnapshot(const JWKSSnapshotPtr& new_jwks) {
  std::lock_guard<std::mutex> l(current_jwks_lock_);
  DCHECK(new_jwks.get() != nullptr);
  current_jwks_ = new_jwks;
  current_jwks_checksum_ = new_jwks->GetChecksum();
}

//
// JWTHelper member functions.
//

struct JWTHelper::JWTDecodedToken {
  JWTDecodedToken(const DecodedJWT& decoded_jwt) : decoded_jwt_(decoded_jwt) {}
  DecodedJWT decoded_jwt_;
};

JWTHelper* JWTHelper::jwt_helper_ = new JWTHelper();

void JWTHelper::TokenDeleter::operator()(JWTHelper::JWTDecodedToken* token) const {
  if (token != nullptr) delete token;
};

Status JWTHelper::Init(const std::string& jwks_file_path) {
  return Init(jwks_file_path, false, "", true);
}

Status JWTHelper::Init(const std::string& jwks_uri, bool jwks_verify_server_certificate,
    const std::string& jwks_ca_certificate, bool is_local_file) {
  jwks_mgr_.reset(new JWKSMgr());
  RETURN_IF_ERROR(jwks_mgr_->Init(jwks_uri, jwks_verify_server_certificate,
      jwks_ca_certificate, is_local_file));
  if (!initialized_) initialized_ = true;
  return Status::OK();
}

JWKSSnapshotPtr JWTHelper::GetJWKS() const {
  DCHECK(initialized_);
  return jwks_mgr_->GetJWKSSnapshot();
}

// Decode the given JWT token.
Status JWTHelper::Decode(const string& token, UniqueJWTDecodedToken& decoded_token_out) {
  Status status;
  try {
    // Call jwt-cpp API to decode the JWT token with default jwt::json_traits
    // (jwt::picojson_traits).
    decoded_token_out.reset(new JWTDecodedToken(jwt::decode(token)));
#ifndef NDEBUG
    std::stringstream msg;
    msg << "JWT token header: ";
    for (auto& e : decoded_token_out.get()->decoded_jwt_.get_header_claims()) {
      msg << e.first << "=" << e.second.to_json().serialize() << ";";
    }
    msg << " JWT token payload: ";
    for (auto& e : decoded_token_out.get()->decoded_jwt_.get_payload_claims()) {
      msg << e.first << "=" << e.second.to_json().serialize() << ";";
    }
    VLOG(3) << msg.str();
#endif
  } catch (const std::invalid_argument& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Token is not in correct format, error: $0", e.what()));
  } catch (const std::runtime_error& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Base64 decoding failed or invalid json, error: $0", e.what()));
  }
  return status;
}

// Validate the token's signature with public key.
Status JWTHelper::Verify(const JWTDecodedToken* decoded_token) const {
  Status status;
  DCHECK(initialized_);
  DCHECK(decoded_token != nullptr);

  if (decoded_token->decoded_jwt_.get_signature().empty()) {
    // Don't accept JWT without a signature.
    return Status(TErrorCode::JWT_VERIFY_FAILED, "Unsecured JWT");
  } else if (jwks_mgr_ == nullptr) {
    // Skip to signature validation if JWKS file or url is not specified.
    return Status::OK();
  }

  JWKSSnapshotPtr jwks = GetJWKS();
  if (jwks->IsEmpty()) {
    return Status(
        TErrorCode::JWT_VERIFY_FAILED, "Verification failed, no matching valid key");
  };

  try {
    string algorithm =
        boost::algorithm::to_lower_copy(decoded_token->decoded_jwt_.get_algorithm());
    string prefix = algorithm.substr(0, 2);
    if (decoded_token->decoded_jwt_.has_key_id()) {
      // Get key id from token's header and use it to retrieve the public key from JWKS.
      std::string key_id = decoded_token->decoded_jwt_.get_key_id();

      const JWTPublicKey* pub_key = nullptr;
      if (prefix == "hs") {
        pub_key = jwks->LookupHSKey(key_id);
      } else if (prefix == "rs" || prefix == "ps") {
        pub_key = jwks->LookupRSAPublicKey(key_id);
      } else if (prefix == "es") {
        pub_key = jwks->LookupECPublicKey(key_id);
      } else {
        return Status(TErrorCode::JWT_VERIFY_FAILED,
            Substitute("Unsupported cryptographic algorithm '$0' for JWT", algorithm));
      }
      if (pub_key == nullptr) {
        return Status(TErrorCode::JWT_VERIFY_FAILED, "Invalid JWK ID in the JWT token");
      }
      // Use the public key to verify the token's signature.
      status = pub_key->Verify(decoded_token->decoded_jwt_, algorithm);
    } else {
      // According to RFC 7517 (JSON Web Key), 'kid' is OPTIONAL so it's possible there
      // is no key id in the token's header. In this case, get all of public keys from
      // JWKS for the family of algorithms.
      const JWKSSnapshot::JWTPublicKeyMap* key_map = nullptr;
      if (prefix == "hs") {
        key_map = jwks->GetAllHSKeys();
      } else if (prefix == "rs" || prefix == "ps") {
        key_map = jwks->GetAllRSAPublicKeys();
      } else if (prefix == "es") {
        key_map = jwks->GetAllECPublicKeys();
      } else {
        return Status(TErrorCode::JWT_VERIFY_FAILED,
            Substitute("Unsupported cryptographic algorithm '$0' for JWT", algorithm));
      }
      if (key_map->size() == 0) {
        return Status(
            TErrorCode::JWT_VERIFY_FAILED, "Verification failed, no matching key");
      }
      // Try each key with matching algorithm util the signature is verified.
      for (auto& key : *key_map) {
        status = key.second->Verify(decoded_token->decoded_jwt_, algorithm);
        if (status.ok()) return status;
      }
    }
  } catch (const std::bad_cast& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Claim was present but not a string, error: $0", e.what()));
  } catch (const jwt::error::claim_not_present_exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Claim not present in JWT token, error $0", e.what()));
  } catch (const std::exception& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Token varification failed, error: $0", e.what()));
  }
  return status;
}

Status JWTHelper::GetCustomClaimUsername(const JWTDecodedToken* decoded_token,
    const string& jwt_custom_claim_username, string& username) {
  DCHECK(decoded_token != nullptr);
  DCHECK(!jwt_custom_claim_username.empty());
  Status status;
  try {
    // Get value of custom claim 'username' from the token payload.
    if (decoded_token->decoded_jwt_.has_payload_claim(jwt_custom_claim_username)) {
      // Assume the claim data type of 'username' is string.
      username.assign(
          decoded_token->decoded_jwt_.get_payload_claim(jwt_custom_claim_username)
              .to_json()
              .to_str());
      if (username.empty()) {
        status = Status(TErrorCode::JWT_VERIFY_FAILED,
            Substitute("Claim '$0' is empty", jwt_custom_claim_username));
      }
    } else {
      status = Status(TErrorCode::JWT_VERIFY_FAILED,
          Substitute("Claim '$0' was not present", jwt_custom_claim_username));
    }
  } catch (const std::runtime_error& e) {
    status = Status(TErrorCode::JWT_VERIFY_FAILED,
        Substitute("Claim '$0' was not present, error: $1", jwt_custom_claim_username,
            e.what()));
  }
  return status;
}

} // namespace impala
