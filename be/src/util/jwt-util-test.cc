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

#include <cstdio> // file stuff
#include <gutil/strings/substitute.h>

#include "jwt-util-internal.h"
#include "jwt-util.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

using std::string;

std::string rsa_priv_key_pem = R"(-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQC4ZtdaIrd1BPIJ
tfnF0TjIK5inQAXZ3XlCrUlJdP+XHwIRxdv1FsN12XyMYO/6ymLmo9ryoQeIrsXB
XYqlET3zfAY+diwCb0HEsVvhisthwMU4gZQu6TYW2s9LnXZB5rVtcBK69hcSlA2k
ZudMZWxZcj0L7KMfO2rIvaHw/qaVOE9j0T257Z8Kp2CLF9MUgX0ObhIsdumFRLaL
DvDUmBPr2zuh/34j2XmWwn1yjN/WvGtdfhXW79Ki1S40HcWnygHgLV8sESFKUxxQ
mKvPUTwDOIwLFL5WtE8Mz7N++kgmDcmWMCHc8kcOIu73Ta/3D4imW7VbKgHZo9+K
3ESFE3RjAgMBAAECggEBAJTEIyjMqUT24G2FKiS1TiHvShBkTlQdoR5xvpZMlYbN
tVWxUmrAGqCQ/TIjYnfpnzCDMLhdwT48Ab6mQJw69MfiXwc1PvwX1e9hRscGul36
ryGPKIVQEBsQG/zc4/L2tZe8ut+qeaK7XuYrPp8bk/X1e9qK5m7j+JpKosNSLgJj
NIbYsBkG2Mlq671irKYj2hVZeaBQmWmZxK4fw0Istz2WfN5nUKUeJhTwpR+JLUg4
ELYYoB7EO0Cej9UBG30hbgu4RyXA+VbptJ+H042K5QJROUbtnLWuuWosZ5ATldwO
u03dIXL0SH0ao5NcWBzxU4F2sBXZRGP2x/jiSLHcqoECgYEA4qD7mXQpu1b8XO8U
6abpKloJCatSAHzjgdR2eRDRx5PMvloipfwqA77pnbjTUFajqWQgOXsDTCjcdQui
wf5XAaWu+TeAVTytLQbSiTsBhrnoqVrr3RoyDQmdnwHT8aCMouOgcC5thP9vQ8Us
rVdjvRRbnJpg3BeSNimH+u9AHgsCgYEA0EzcbOltCWPHRAY7B3Ge/AKBjBQr86Kv
TdpTlxePBDVIlH+BM6oct2gaSZZoHbqPjbq5v7yf0fKVcXE4bSVgqfDJ/sZQu9Lp
PTeV7wkk0OsAMKk7QukEpPno5q6tOTNnFecpUhVLLlqbfqkB2baYYwLJR3IRzboJ
FQbLY93E8gkCgYB+zlC5VlQbbNqcLXJoImqItgQkkuW5PCgYdwcrSov2ve5r/Acz
FNt1aRdSlx4176R3nXyibQA1Vw+ztiUFowiP9WLoM3PtPZwwe4bGHmwGNHPIfwVG
m+exf9XgKKespYbLhc45tuC08DATnXoYK7O1EnUINSFJRS8cezSI5eHcbQKBgQDC
PgqHXZ2aVftqCc1eAaxaIRQhRmY+CgUjumaczRFGwVFveP9I6Gdi+Kca3DE3F9Pq
PKgejo0SwP5vDT+rOGHN14bmGJUMsX9i4MTmZUZ5s8s3lXh3ysfT+GAhTd6nKrIE
kM3Nh6HWFhROptfc6BNusRh1kX/cspDplK5x8EpJ0QKBgQDWFg6S2je0KtbV5PYe
RultUEe2C0jYMDQx+JYxbPmtcopvZQrFEur3WKVuLy5UAy7EBvwMnZwIG7OOohJb
vkSpADK6VPn9lbqq7O8cTedEHttm6otmLt8ZyEl3hZMaL3hbuRj6ysjmoFKx6CrX
rK0/Ikt5ybqUzKCMJZg2VKGTxg==
-----END PRIVATE KEY-----)";
std::string rsa_pub_key_pem = R"(-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuGbXWiK3dQTyCbX5xdE4
yCuYp0AF2d15Qq1JSXT/lx8CEcXb9RbDddl8jGDv+spi5qPa8qEHiK7FwV2KpRE9
83wGPnYsAm9BxLFb4YrLYcDFOIGULuk2FtrPS512Qea1bXASuvYXEpQNpGbnTGVs
WXI9C+yjHztqyL2h8P6mlThPY9E9ue2fCqdgixfTFIF9Dm4SLHbphUS2iw7w1JgT
69s7of9+I9l5lsJ9cozf1rxrXX4V1u/SotUuNB3Fp8oB4C1fLBEhSlMcUJirz1E8
AziMCxS+VrRPDM+zfvpIJg3JljAh3PJHDiLu902v9w+Iplu1WyoB2aPfitxEhRN0
YwIDAQAB
-----END PUBLIC KEY-----)";
std::string rsa_pub_key_jwk_n =
    "uGbXWiK3dQTyCbX5xdE4yCuYp0AF2d15Qq1JSXT_lx8CEcXb9RbDddl8jGDv-sp"
    "i5qPa8qEHiK7FwV2KpRE983wGPnYsAm9BxLFb4YrLYcDFOIGULuk2FtrPS512Qe"
    "a1bXASuvYXEpQNpGbnTGVsWXI9C-yjHztqyL2h8P6mlThPY9E9ue2fCqdgixfTF"
    "IF9Dm4SLHbphUS2iw7w1JgT69s7of9-I9l5lsJ9cozf1rxrXX4V1u_SotUuNB3F"
    "p8oB4C1fLBEhSlMcUJirz1E8AziMCxS-VrRPDM-zfvpIJg3JljAh3PJHDiLu902"
    "v9w-Iplu1WyoB2aPfitxEhRN0Yw";
std::string rsa_pub_key_jwk_e = "AQAB";
std::string rsa_invalid_pub_key_jwk_n =
    "xzYuc22QSst_dS7geYYK5l5kLxU0tayNdixkEQ17ix-CUcUbKIsnyftZxaCYT46"
    "rQtXgCaYRdJcbB3hmyrOavkhTpX79xJZnQmfuamMbZBqitvscxW9zRR9tBUL6vd"
    "i_0rpoUwPMEh8-Bw7CgYR0FK0DhWYBNDfe9HKcyZEv3max8Cdq18htxjEsdYO0i"
    "wzhtKRXomBWTdhD5ykd_fACVTr4-KEY-IeLvubHVmLUhbE5NgWXxrRpGasDqzKh"
    "CTmsa2Ysf712rl57SlH0Wz_Mr3F7aM9YpErzeYLrl0GhQr9BVJxOvXcVd4kmY-X"
    "kiCcrkyS1cnghnllh-LCwQu1sYw";

std::string rsa512_priv_key_pem = R"(-----BEGIN RSA PRIVATE KEY-----
MIICWwIBAAKBgQDdlatRjRjogo3WojgGHFHYLugdUWAY9iR3fy4arWNA1KoS8kVw
33cJibXr8bvwUAUparCwlvdbH6dvEOfou0/gCFQsHUfQrSDv+MuSUMAe8jzKE4qW
+jK+xQU9a03GUnKHkkle+Q0pX/g6jXZ7r1/xAK5Do2kQ+X5xK9cipRgEKwIDAQAB
AoGAD+onAtVye4ic7VR7V50DF9bOnwRwNXrARcDhq9LWNRrRGElESYYTQ6EbatXS
3MCyjjX2eMhu/aF5YhXBwkppwxg+EOmXeh+MzL7Zh284OuPbkglAaGhV9bb6/5Cp
uGb1esyPbYW+Ty2PC0GSZfIXkXs76jXAu9TOBvD0ybc2YlkCQQDywg2R/7t3Q2OE
2+yo382CLJdrlSLVROWKwb4tb2PjhY4XAwV8d1vy0RenxTB+K5Mu57uVSTHtrMK0
GAtFr833AkEA6avx20OHo61Yela/4k5kQDtjEf1N0LfI+BcWZtxsS3jDM3i1Hp0K
Su5rsCPb8acJo5RO26gGVrfAsDcIXKC+bQJAZZ2XIpsitLyPpuiMOvBbzPavd4gY
6Z8KWrfYzJoI/Q9FuBo6rKwl4BFoToD7WIUS+hpkagwWiz+6zLoX1dbOZwJACmH5
fSSjAkLRi54PKJ8TFUeOP15h9sQzydI8zJU+upvDEKZsZc/UhT/SySDOxQ4G/523
Y0sz/OZtSWcol/UMgQJALesy++GdvoIDLfJX5GBQpuFgFenRiRDabxrE9MNUZ2aP
FaFp+DyAe+b4nDwuJaW2LURbr8AEZga7oQj0uYxcYw==
-----END RSA PRIVATE KEY-----)";
std::string rsa512_pub_key_pem = R"(-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDdlatRjRjogo3WojgGHFHYLugd
UWAY9iR3fy4arWNA1KoS8kVw33cJibXr8bvwUAUparCwlvdbH6dvEOfou0/gCFQs
HUfQrSDv+MuSUMAe8jzKE4qW+jK+xQU9a03GUnKHkkle+Q0pX/g6jXZ7r1/xAK5D
o2kQ+X5xK9cipRgEKwIDAQAB
-----END PUBLIC KEY-----)";
std::string rsa512_pub_key_jwk_n =
    "3ZWrUY0Y6IKN1qI4BhxR2C7oHVFgGPYkd38uGq1jQNSqEvJFcN93CYm16_G78FA"
    "FKWqwsJb3Wx-nbxDn6LtP4AhULB1H0K0g7_jLklDAHvI8yhOKlvoyvsUFPWtNxl"
    "Jyh5JJXvkNKV_4Oo12e69f8QCuQ6NpEPl-cSvXIqUYBCs";
std::string rsa512_pub_key_jwk_e = "AQAB";
std::string rsa512_invalid_pub_key_jwk_n =
    "xzYuc22QSst_dS7geYYK5l5kLxU0tayNdixkEQ17ix-CUcUbKIsnyftZxaCYT46"
    "rQtXgCaYRdJcbB3hmyrOavkhTpX79xJZnQmfuamMbZBqitvscxW9zRR9tBUL6vd"
    "i_0rpoUwPMEh8-Bw7CgYR0FK0DhWYBNDfe9HKcyZEv3max8Cdq18htxjEsdYO0i"
    "wzhtKRXomBWTdhD5ykd_fACVTr4-KEY-IeLvubHVmLUhbE5NgWXxrRpGasDqzKh"
    "CTmsa2Ysf712rl57SlH0Wz_Mr3F7aM9YpErzeYLrl0GhQr9BVJxOvXcVd4kmY-X"
    "kiCcrkyS1cnghnllh-LCwQu1sYw";

std::string kid_1 = "public:c424b67b-fe28-45d7-b015-f79da50b5b21";
std::string kid_2 = "public:9b9d0b47-b9ed-4ba6-9180-52fc5b161a3a";

std::string jwks_hs_file_format = R"(
{
  "keys": [
    { "kty": "oct", "kid": "$0", "alg": "$1", "k": "$2" }
  ]
})";

std::string jwks_rsa_file_format = R"(
{
  "keys": [
    { "kty": "RSA", "kid": "$0", "alg": "$1", "n": "$2", "e": "$3" },
    { "kty": "RSA", "kid": "$4", "alg": "$5", "n": "$6", "e": "$7" }
  ]
})";

/// Utility class for creating a file that will be automatically deleted upon test
/// completion.
class TempTestDataFile {
 public:
  // Creates a temporary file with the specified contents.
  TempTestDataFile(const std::string& contents);

  ~TempTestDataFile() { Delete(); }

  /// Returns the absolute path to the file.
  const std::string& Filename() const { return name_; }

 private:
  std::string name_;
  bool deleted_;

  // Delete this temporary file
  void Delete();
};

TempTestDataFile::TempTestDataFile(const std::string& contents)
  : name_("/tmp/jwks_XXXXXX"), deleted_(false) {
  int fd = mkstemp(&name_[0]);
  if (fd == -1) {
    std::cout << "Error creating temp file; " << strerror(errno) << std::endl;
    abort();
  }
  if (close(fd) != 0) {
    std::cout << "Error closing temp file; " << strerror(errno) << std::endl;
    abort();
  }

  FILE* handle = fopen(name_.c_str(), "w");
  if (handle == nullptr) {
    std::cout << "Error creating temp file; " << strerror(errno) << std::endl;
    abort();
  }
  int status = fputs(contents.c_str(), handle);
  if (status < 0) {
    std::cout << "Error writing to temp file; " << strerror(errno) << std::endl;
    abort();
  }
  status = fclose(handle);
  if (status != 0) {
    std::cout << "Error closing temp file; " << strerror(errno) << std::endl;
    abort();
  }
}

void TempTestDataFile::Delete() {
  if (deleted_) return;
  deleted_ = true;
  if (remove(name_.c_str()) != 0) {
    std::cout << "Error deleting temp file; " << strerror(errno) << std::endl;
    abort();
  }
}

TEST(JwtUtilTest, LoadJwksFile) {
  // Load JWKS from file.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  const JsonWebKeySet* jwks = jwt_helper.GetJWKS();
  ASSERT_FALSE(jwks->IsEmpty());
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ("rs256", key1->get_algorithm());
  ASSERT_EQ(rsa_pub_key_pem, key1->get_key());

  std::string non_existing_kid("public:c424b67b-fe28-45d7-b015-f79da5-xxxxx");
  const JWTPublicKey* key3 = jwks->LookupRSAPublicKey(non_existing_kid);
  ASSERT_FALSE(key3 != nullptr);
}

TEST(JwtUtilTest, LoadInvalidJwksFiles) {
  // JWK without kid.
  std::unique_ptr<TempTestDataFile> jwks_file(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "    }"
      "  ]"
      "}"));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.msg().msg().find("parsing key #0") != std::string::npos)
      << " Actual error: " << status.msg().msg();
  ASSERT_TRUE(status.GetDetail().find("'kid' property is required") != std::string::npos)
      << "actual error: " << status.GetDetail();
  ASSERT_TRUE(jwt_helper.GetJWKS()->IsEmpty());

  // Invalid JSON format, missing "]" and "}".
  jwks_file.reset(new TempTestDataFile(
      "{"
      "  \"keys\": ["
      "    {"
      "      \"use\": \"sig\","
      "      \"kty\": \"RSA\","
      "      \"kid\": \"public:c424b67b-fe28-45d7-b015-f79da50b5b21\","
      "      \"alg\": \"RS256\","
      "      \"n\": \"sttddbg-_yjXzcFpbMJB1fIFam9lQBeXWbTqzJwbuFbspHMsRowa8FaPw\","
      "      \"e\": \"AQAB\""
      "}"));
  status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.GetDetail().find("Missing a comma or ']' after an array element")
      != std::string::npos)
      << " Actual error: " << status.GetDetail();

  // JWKS with empty key id.
  jwks_file.reset(new TempTestDataFile(
      Substitute(jwks_rsa_file_format, "", "RS256", rsa_pub_key_jwk_n, rsa_pub_key_jwk_e,
          "", "RS256", rsa_invalid_pub_key_jwk_n, rsa_pub_key_jwk_e)));
  status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.msg().msg().find("parsing key #0") != std::string::npos)
      << " Actual error: " << status.msg().msg();
  ASSERT_TRUE(status.GetDetail().find("'kid' property must be a non-empty string")
      != std::string::npos)
      << " Actual error: " << status.GetDetail();

  // JWKS with empty key value.
  jwks_file.reset(new TempTestDataFile(
      Substitute(jwks_rsa_file_format, kid_1, "RS256", "", "", kid_2, "RS256", "", "")));
  status = jwt_helper.Init(jwks_file->Filename());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.msg().msg().find("parsing key #0") != std::string::npos)
      << " Actual error: " << status.msg().msg();
  ASSERT_TRUE(status.GetDetail().find("'n' and 'e' properties must be a non-empty string")
      != std::string::npos)
      << " Actual error: " << status.GetDetail();
}

TEST(JwtUtilTest, VerifyJwtHS256) {
  // Cryptographic algorithm: HS256.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "Yx57JSBzhGFDgDj19CabRpH/+kiaKqI6UZI6lDunQKw=";
  TempTestDataFile jwks_file(
      Substitute(jwks_hs_file_format, kid_1, "HS256", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  const JsonWebKeySet* jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS256.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS256")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs256(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtHS384) {
  // Cryptographic algorithm: HS384.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret =
      "TlqmKRc2PNQJXTC3Go7eAadwPxA7x9byyXCi5I8tSvxrE77tYbuF5pfZAyswrkou";
  TempTestDataFile jwks_file(
      Substitute(jwks_hs_file_format, kid_1, "HS384", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  const JsonWebKeySet* jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS384.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS384")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs384(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtHS512) {
  // Cryptographic algorithm: HS512.
  // SharedSecret (Generated for MAC key (Base64 encoded)).
  string shared_secret = "ywc6DN7+iRw1E5HOqzvrsYodykSLFutT28KN3bJnLZcZpPCNjn0b6gbMfXPcxeY"
                         "VyuWWGDxh6gCDwPMejbuEEg==";
  TempTestDataFile jwks_file(
      Substitute(jwks_hs_file_format, kid_1, "HS512", shared_secret));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  const JsonWebKeySet* jwks = jwt_helper.GetJWKS();
  EXPECT_OK(status);
  ASSERT_EQ(1, jwks->GetHSKeyNum());

  const JWTPublicKey* key1 = jwks->LookupHSKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(key1->get_key(), shared_secret);

  // Create a JWT token and sign it with HS512.
  auto token = jwt::create()
                   .set_issuer("auth0")
                   .set_type("JWS")
                   .set_algorithm("HS512")
                   .set_key_id(kid_1)
                   .set_payload_claim("username", picojson::value("impala"))
                   .sign(jwt::algorithm::hs512(shared_secret));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS256) {
  // Cryptographic algorithm: RS256.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  const JsonWebKeySet* jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  ASSERT_EQ(
      "eyJhbGciOiJSUzI1NiIsImtpZCI6InB1YmxpYzpjNDI0YjY3Yi1mZTI4LTQ1ZDctYjAxNS1mNzlkYTUwYj"
      "ViMjEiLCJ0eXAiOiJKV1MifQ.eyJpc3MiOiJhdXRoMCIsInVzZXJuYW1lIjoiaW1wYWxhIn0.OW5H2SClL"
      "lsotsCarTHYEbqlbRh43LFwOyo9WubpNTwE7hTuJDsnFoVrvHiWI02W69TZNat7DYcC86A_ogLMfNXagHj"
      "lMFJaRnvG5Ekag8NRuZNJmHVqfX-qr6x7_8mpOdU554kc200pqbpYLhhuK4Qf7oT7y9mOrtNrUKGDCZ0Q2"
      "y_mizlbY6SMg4RWqSz0RQwJbRgXIWSgcbZd0GbD_MQQ8x7WRE4nluU-5Fl4N2Wo8T9fNTuxALPiuVeIczO"
      "25b5n4fryfKasSgaZfmk0CoOJzqbtmQxqiK9QNSJAiH2kaqMwLNgAdgn8fbd-lB1RAEGeyPH8Px8ipqcKs"
      "Pk0bg",
      token);

  // Verify the JWT token with jwt-cpp APIs directly.
  auto jwt_decoded_token = jwt::decode(token);
  auto verifier = jwt::verify()
                      .allow_algorithm(jwt::algorithm::rs256(rsa_pub_key_pem, "", "", ""))
                      .with_issuer("auth0");
  verifier.verify(jwt_decoded_token);

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS384) {
  // Cryptographic algorithm: RS384.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS384",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS384", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  const JsonWebKeySet* jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with RS384.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS384")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs384(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtRS512) {
  // Cryptographic algorithm: RS512.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS512",
      rsa512_pub_key_jwk_n, rsa512_pub_key_jwk_e, kid_2, "RS512",
      rsa512_invalid_pub_key_jwk_n, rsa512_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);
  const JsonWebKeySet* jwks = jwt_helper.GetJWKS();
  ASSERT_EQ(2, jwks->GetRSAPublicKeyNum());

  const JWTPublicKey* key1 = jwks->LookupRSAPublicKey(kid_1);
  ASSERT_TRUE(key1 != nullptr);
  ASSERT_EQ(rsa512_pub_key_pem, key1->get_key());

  // Create a JWT token and sign it with RS512.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kid_1)
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs512(rsa512_pub_key_pem, rsa512_priv_key_pem, "", ""));

  // Verify the JWT token with our wrapper class which use public key retrieved from JWKS,
  // and read username from the token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtNotVerifySignature) {
  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));

  // Do not verify signature.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  Status status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  string username;
  status = JWTHelper::GetCustomClaimUsername(decoded_token.get(), "username", username);
  EXPECT_OK(status);
  ASSERT_EQ("impala", username);
}

TEST(JwtUtilTest, VerifyJwtFailMismatchingAlgorithms) {
  // JWT algorithm is not matching with algorithm in JWK.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token, but set mismatching algorithm.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS512")
          .set_key_id(kid_1)
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  // Failed to verify the token due to mismatching algorithms.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.GetDetail().find(
                  "JWT algorithm 'rs512' is not matching with JWK algorithm 'rs256'")
      != std::string::npos)
      << " Actual error: " << status.GetDetail();
}

TEST(JwtUtilTest, VerifyJwtFailKeyNotFound) {
  // The key cannot be found in JWKS.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token with a key ID which can not be found in JWKS.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id("unfound-key-id")
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  // Failed to verify the token since key is not found in JWKS.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(
      status.GetDetail().find("Invalid JWK ID in the JWT token") != std::string::npos)
      << " Actual error: " << status.GetDetail();
}

TEST(JwtUtilTest, VerifyJwtTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").set_algorithm("RS256").sign(
          jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));
  // Verify the token by trying each key in JWK set and there is one matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  EXPECT_OK(status);
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutKeyId) {
  // Verify JWT token without key ID.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token without key ID.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").set_algorithm("RS512").sign(
          jwt::algorithm::rs512(rsa512_pub_key_pem, rsa512_priv_key_pem, "", ""));
  // Verify the token by trying each key in JWK set, but there is no matched key.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
}

TEST(JwtUtilTest, VerifyJwtFailTokenWithoutSignature) {
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token without signature.
  auto token =
      jwt::create().set_issuer("auth0").set_type("JWS").sign(jwt::algorithm::none{});
  // Failed to verify the unsigned token.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.GetDetail().find("Unsecured JWT") != std::string::npos)
      << " Actual error: " << status.GetDetail();
}

TEST(JwtUtilTest, VerifyJwtFailExpiredToken) {
  // Sign JWT token with RS256.
  TempTestDataFile jwks_file(Substitute(jwks_rsa_file_format, kid_1, "RS256",
      rsa_pub_key_jwk_n, rsa_pub_key_jwk_e, kid_2, "RS256", rsa_invalid_pub_key_jwk_n,
      rsa_pub_key_jwk_e));
  JWTHelper jwt_helper;
  Status status = jwt_helper.Init(jwks_file.Filename());
  EXPECT_OK(status);

  // Create a JWT token and sign it with RS256.
  auto token =
      jwt::create()
          .set_issuer("auth0")
          .set_type("JWS")
          .set_algorithm("RS256")
          .set_key_id(kid_1)
          .set_issued_at(std::chrono::system_clock::now())
          .set_expires_at(std::chrono::system_clock::now() - std::chrono::seconds{10})
          .set_payload_claim("username", picojson::value("impala"))
          .sign(jwt::algorithm::rs256(rsa_pub_key_pem, rsa_priv_key_pem, "", ""));

  // Verify the token, including expiring time.
  JWTHelper::UniqueJWTDecodedToken decoded_token;
  status = JWTHelper::Decode(token, decoded_token);
  EXPECT_OK(status);
  status = jwt_helper.Verify(decoded_token.get());
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.GetDetail().find("Verification failed, error: token expired")
      != std::string::npos)
      << " Actual error: " << status.GetDetail();
}

} // namespace impala
