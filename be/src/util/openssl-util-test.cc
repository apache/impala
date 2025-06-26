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

#include <random>

#include <gtest/gtest.h>
#include <openssl/err.h>
#include <openssl/rand.h>

#include "common/init.h"
#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/status.h"
#include "testutil/gtest-util.h"
#include "util/openssl-util.h"

using std::uniform_int_distribution;
using std::mt19937_64;
using strings::Substitute;
using std::string;

namespace impala {

class OpenSSLUtilTest : public ::testing::Test {
 protected:
  virtual void SetUp() { SeedOpenSSLRNG(); }
  virtual void TearDown() {
    EXPECT_EQ(ERR_peek_error(), 0) << "Did not clear OpenSSL error queue";
  }

  /// Fill buffer 'data' with 'len' bytes of random binary data from 'rng_'.
  /// 'len' must be a multiple of 8 bytes'.
  void GenerateRandomData(uint8_t* data, int64_t len) {
    DCHECK_EQ(len % 8, 0);
    for (int64_t i = 0; i < len; i += sizeof(uint64_t)) {
      *(reinterpret_cast<uint64_t*>(&data[i])) =
          uniform_int_distribution<uint64_t>(0, numeric_limits<uint64_t>::max())(rng_);
    }
  }

  /// Fill arbitrary-length buffer with random bytes
  void GenerateRandomBytes(uint8_t* data, int64_t len) {
    DCHECK_GE(len, 0);
    for (int64_t i = 0; i < len; i++) {
      data[i] = uniform_int_distribution<uint8_t>(0, UINT8_MAX)(rng_);
    }
  }

  void TestEncryptionDecryption(const int64_t buffer_size) {
    vector<uint8_t> original(buffer_size);
    // Scratch buffer for in-place encryption.
    vector<uint8_t> scratch(buffer_size + AES_BLOCK_SIZE);
    if (buffer_size % 8 == 0) {
      GenerateRandomData(original.data(), buffer_size);
    } else {
      GenerateRandomBytes(original.data(), buffer_size);
    }

    // Check all the modes
    AES_CIPHER_MODE modes[] = {
        AES_CIPHER_MODE::AES_256_GCM,
        AES_CIPHER_MODE::AES_256_CTR,
        AES_CIPHER_MODE::AES_256_CFB,
        AES_CIPHER_MODE::AES_256_ECB,
        AES_CIPHER_MODE::AES_128_GCM,
        AES_CIPHER_MODE::AES_128_ECB,
    };
    for (auto m : modes) {
      memcpy(scratch.data(), original.data(), buffer_size);

      EncryptionKey key;
      ASSERT_OK(key.InitializeRandom(AES_BLOCK_SIZE, m));

      int64_t encrypted_length;
      if (key.IsEcbMode() && buffer_size > numeric_limits<int>::max() - AES_BLOCK_SIZE) {
          ASSERT_ERROR_MSG(key.Encrypt(scratch.data(),buffer_size, scratch.data()),
              "Input buffer length exceeds the supported length for ECB mode.");
          continue;
      }
      ASSERT_OK(key.Encrypt(scratch.data(), buffer_size, scratch.data(),
          &encrypted_length));

      // Check that encryption did something
      ASSERT_NE(0, memcmp(original.data(), scratch.data(), buffer_size));
      ASSERT_OK(key.Decrypt(scratch.data(), encrypted_length, scratch.data()));
      // Check that we get the original data back.
      ASSERT_EQ(0, memcmp(original.data(), scratch.data(), buffer_size));
    }
  }

  mt19937_64 rng_;
};

/// Test basic encryption functionality.
TEST_F(OpenSSLUtilTest, Encryption) {
  const int buffer_size = 1024 * 1024;
  vector<uint8_t> original(buffer_size);
  vector<uint8_t> prev_encrypted(buffer_size);
  vector<uint8_t> encrypted(buffer_size);
  vector<uint8_t> decrypted(buffer_size);
  GenerateRandomData(original.data(), buffer_size);

  // Check GCM, CTR and CFB
  AES_CIPHER_MODE modes[] = {
      AES_CIPHER_MODE::AES_256_GCM,
      AES_CIPHER_MODE::AES_256_CTR,
      AES_CIPHER_MODE::AES_256_CFB,
      AES_CIPHER_MODE::AES_128_GCM
  };
  for (auto m : modes) {
    // Iterate multiple times to ensure that key regeneration works correctly.
    EncryptionKey key;
    for (int i = 0; i < 2; ++i) {
      // Generate a new key for each iteration.
      ASSERT_OK(key.InitializeRandom(AES_BLOCK_SIZE, m));

      // Check that OpenSSL is happy with the amount of entropy we're feeding it.
      DCHECK_EQ(1, RAND_status());

      ASSERT_OK(key.Encrypt(original.data(), buffer_size, encrypted.data()));
      if (i > 0) {
        // Check that we're not somehow reusing the same key.
        ASSERT_NE(0, memcmp(encrypted.data(), prev_encrypted.data(), buffer_size));
      }
      memcpy(prev_encrypted.data(), encrypted.data(), buffer_size);

      // We should get the original data by decrypting it.
      ASSERT_OK(key.Decrypt(encrypted.data(), buffer_size, decrypted.data()));
      ASSERT_EQ(0, memcmp(original.data(), decrypted.data(), buffer_size));
    }
  }
}

/// Test to check whether key and mode are validated in InitializeFields().
TEST_F(OpenSSLUtilTest, ValidateInitialize) {
  EncryptionKey key;
  uint8_t IV[AES_BLOCK_SIZE] = {};
  uint8_t key32bits[32] = {};
  // Using AES_256_CFB mode to test since it's supported in all Impala
  // supported OpenSSL versions.
  Status status_initialize_fields = key.InitializeFields
      (key32bits, 16, IV, AES_BLOCK_SIZE, AES_CIPHER_MODE::AES_256_CFB);
  ASSERT_FALSE(status_initialize_fields.ok());
  ASSERT_OK(key.InitializeFields(key32bits,
      32, IV, AES_BLOCK_SIZE, AES_CIPHER_MODE::AES_256_CFB));
}

/// Test that encryption and decryption work in-place.
TEST_F(OpenSSLUtilTest, EncryptInPlace) {
  const int buffer_size = 1024 * 1024;
  TestEncryptionDecryption(buffer_size);
}

/// Test that encryption works with buffer lengths that don't fit in a 32-bit integer.
TEST_F(OpenSSLUtilTest, EncryptInPlaceHugeBuffer) {
  const int64_t buffer_size = 3 * 1024L * 1024L * 1024L;
  TestEncryptionDecryption(buffer_size);
}

/// Test that encryption works with arbitrary-length buffer
TEST_F(OpenSSLUtilTest, EncryptArbitraryLength) {
  std::uniform_int_distribution<uint64_t> dis(0, 1024 * 1024);
  const int buffer_size = dis(rng_);
  TestEncryptionDecryption(buffer_size);
}

/// Test integrity in GCM mode
TEST_F(OpenSSLUtilTest, GcmIntegrity) {
  const int buffer_size = 1024 * 1024;
  vector<uint8_t> buffer(buffer_size);

  EncryptionKey key;
  ASSERT_OK(key.InitializeRandom(AES_BLOCK_SIZE, AES_CIPHER_MODE::AES_256_GCM));

  // Even it has been set as GCM mode, it may fall back to other modes.
  // Check if GCM mode is supported at runtime.
  if (key.IsGcmMode()) {
    GenerateRandomData(buffer.data(), buffer_size);
    ASSERT_OK(key.Encrypt(buffer.data(), buffer_size, buffer.data()));

    // tamper the data
    ++buffer[0];
    Status s = key.Decrypt(buffer.data(), buffer_size, buffer.data());
    EXPECT_STR_CONTAINS(s.GetDetail(), "EVP_DecryptFinal");
  }
}

/// Test basic integrity hash functionality.
TEST_F(OpenSSLUtilTest, IntegrityHash) {
  const int buffer_size = 1024 * 1024;
  vector<uint8_t> buf1(buffer_size);
  vector<uint8_t> buf1_copy(buffer_size);
  vector<uint8_t> buf2(buffer_size);
  GenerateRandomData(buf1.data(), buffer_size);
  memcpy(buf1_copy.data(), buf1.data(), buffer_size);
  memcpy(buf2.data(), buf1.data(), buffer_size);
  ++buf2[buffer_size - 1]; // Alter a byte in buf2 to ensure it's different.

  IntegrityHash buf1_hash;
  buf1_hash.Compute(buf1.data(), buffer_size);
  EXPECT_TRUE(buf1_hash.Verify(buf1.data(), buffer_size));
  EXPECT_TRUE(buf1_hash.Verify(buf1_copy.data(), buffer_size));

  EXPECT_FALSE(buf1_hash.Verify(buf2.data(), buffer_size));
  // We should generally get different results for different buffer sizes (unless we're
  // cosmically lucky and get a hash collision).
  EXPECT_FALSE(buf1_hash.Verify(buf1.data(), buffer_size / 2));
}

/// Test that integrity hash works with buffer lengths that don't fit in a 32-bit integer.
TEST_F(OpenSSLUtilTest, IntegrityHashHugeBuffer) {
  const int64_t buffer_size = 3L * 1024L * 1024L * 1024L;
  vector<uint8_t> buf(buffer_size);
  GenerateRandomData(buf.data(), buffer_size);

  IntegrityHash hash;
  hash.Compute(buf.data(), buffer_size);
  EXPECT_TRUE(hash.Verify(buf.data(), buffer_size));
  // We should generally get different results for different buffer sizes (unless we're
  // cosmically lucky and get a hash collision).
  EXPECT_FALSE(hash.Verify(buf.data(), buffer_size - 10));
}

/// Test that we are seeding the OpenSSL random number generator well enough to
/// generate lots of encryption keys.
TEST_F(OpenSSLUtilTest, RandSeeding) {
  for (int i = 0; i < 100000; ++i) {
    // Check that OpenSSL is happy with the amount of entropy we're feeding it.
    DCHECK_EQ(1, RAND_status());

    EncryptionKey key;
    ASSERT_OK(key.InitializeRandom(AES_BLOCK_SIZE, key.GetSupportedDefaultMode()));
  }
}

///
/// ValidatePemBundle tests
///
/// Constants defining paths to test files.
static const string& EXPIRED_CERT = "bad-cert";
static const string& FUTURE_CERT = "future-cert";
static const string& INVALID_CERT = "invalid-server-cert";
static const string& VALID_CERT_1 = "server-cert";
static const string& VALID_CERT_2 = "wildcardCA";

/// Constants and functions defining expected error messages.
static const string& MSG_NEW_BIO_ERR = "OpenSSL error in PEM_read_bio_X509 unexpected "
    "error '$0' while reading PEM bundle";
static const string& MSG_NONE_VALID = "PEM bundle contains no valid certificates";
static string _msg_time(int invalid_notbefore_cnt, int invalid_notafter_cnt) {
  return Substitute("PEM bundle contains $0 invalid certificate(s) with notBefore in the "
      "future and $1 invalid certificate(s) with notAfter in the past",
      invalid_notbefore_cnt, invalid_notafter_cnt);
}

/// Constants used in the tests
static const string& INVALID_CERT_TEXT = "-----BEGIN CERTIFICATE-----\nnot a cert";

/// Helper function to read a certificate from the be/src/testutil directory.
static string read_cert(const string& cert_name) {
  kudu::faststring contents;
  kudu::Status s = kudu::ReadFileToString(kudu::Env::Default(),
      Substitute("$0/be/src/testutil/$1.pem", getenv("IMPALA_HOME"), cert_name),
      &contents);

  EXPECT_TRUE(s.ok())<< "Certificate '" << cert_name << "' could not be read: "
      << s.ToString();
  EXPECT_FALSE(contents.ToString().empty()) << "Certificate '" << cert_name
      << "' is empty. Please check the test data.";

  string cert = contents.ToString();
  if (cert.back() != '\n') {
    cert.push_back('\n');
  }

  return cert;
} // function read_cert

/// Asserts a PEM bundle containing one valid certificate is valid.
TEST_F(OpenSSLUtilTest, ValidatePemBundleHappyPathOneCert) {
  ASSERT_OK(ValidatePemBundle(Substitute("$0", read_cert(VALID_CERT_1))));
}

/// Asserts a PEM bundle containing two valid certificates is valid.
TEST_F(OpenSSLUtilTest, ValidatePemBundleHappyPath) {
  ASSERT_OK(ValidatePemBundle(Substitute("$0$1", read_cert(VALID_CERT_1),
      read_cert(VALID_CERT_2))));
}

/// Asserts an empty PEM bundle is invalid.
TEST_F(OpenSSLUtilTest, ValidatePemBundleEmpty) {
  ASSERT_ERROR_MSG(ValidatePemBundle(""), "bundle is empty");
}

/// Asserts a bundle containing a single expired certificate is invalid.
TEST_F(OpenSSLUtilTest, ValidatePemBundleExpired) {
  ASSERT_ERROR_MSG(ValidatePemBundle(read_cert(EXPIRED_CERT)), _msg_time(0, 1));
}

/// Asserts a bundle containing a single future dated certificate is invalid.
TEST_F(OpenSSLUtilTest, ValidatePemBundleFutureDated) {
  ASSERT_ERROR_MSG(ValidatePemBundle(read_cert(FUTURE_CERT)), _msg_time(1, 0));
}

/// Asserts a bundle containing a single invalid certificate is invalid.
TEST_F(OpenSSLUtilTest, ValidatePemBundleNotACert) {
  ASSERT_ERROR_MSG(ValidatePemBundle(INVALID_CERT_TEXT), MSG_NONE_VALID);
}

/// Asserts a bundle is invalid if it contains one expired and two non-expired
/// certificates where the expired cert changes position in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleThreeCertsOneExpired) {
  // Expired cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(EXPIRED_CERT),
      read_cert(VALID_CERT_1), read_cert(VALID_CERT_2))), _msg_time(0, 1));

  // Expired cert is in the middle of the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_1),
      read_cert(EXPIRED_CERT), read_cert(VALID_CERT_2))), _msg_time(0, 1));

  // Expired cert is last in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_1),
      read_cert(VALID_CERT_2), read_cert(EXPIRED_CERT))), _msg_time(0, 1));
}

/// Asserts a bundle is invalid if it contains one future dated and two valid
/// certificates where the expired cert changes position in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleThreeCertsOneFutureDated) {
  // Expired cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(FUTURE_CERT),
      read_cert(VALID_CERT_1), read_cert(VALID_CERT_2))), _msg_time(1, 0));

  // Expired cert is in the middle of the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_1),
      read_cert(FUTURE_CERT), read_cert(VALID_CERT_2))), _msg_time(1, 0));

  // Expired cert is last in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_1),
      read_cert(VALID_CERT_2), read_cert(FUTURE_CERT))), _msg_time(1, 0));
}

/// Asserts a bundle is invalid if it contains two expired and one non-expired
/// certificate no matter the position of the expired cert in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleThreeCertsTwoExpired) {
  // First two certs are expired.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(EXPIRED_CERT),
      read_cert(EXPIRED_CERT), read_cert(VALID_CERT_2))), _msg_time(0, 2));

  // Last two certs are expired.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_2),
      read_cert(EXPIRED_CERT), read_cert(EXPIRED_CERT))), _msg_time(0, 2));
}

/// Asserts a bundle is invalid if it contains two future dated and one valid
/// certificate no matter the position of the future dated cert in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleThreeCertsTwoFutureDated) {
  // First two certs are expired.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(FUTURE_CERT),
      read_cert(FUTURE_CERT), read_cert(VALID_CERT_2))), _msg_time(2, 0));

  // Last two certs are expired.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_2),
      read_cert(FUTURE_CERT), read_cert(FUTURE_CERT))), _msg_time(2, 0));
}

/// Asserts a bundle is invalid if it contains one valid and two invalid certificates
/// where the expired cert changes position in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleThreeCertsOneInvalid) {
  // Invalid cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2", INVALID_CERT_TEXT,
      read_cert(VALID_CERT_1), read_cert(VALID_CERT_2))), MSG_NONE_VALID);

  // Invalid cert is in the middle of the bundle.
  EXPECT_STR_CONTAINS(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_1),
      INVALID_CERT_TEXT, read_cert(VALID_CERT_2))).GetDetail(),
      Substitute(MSG_NEW_BIO_ERR, 123));

  // Invalid cert is last in the bundle.
  EXPECT_STR_CONTAINS(ValidatePemBundle(Substitute("$0$1$2", read_cert(VALID_CERT_1),
      read_cert(VALID_CERT_2), INVALID_CERT_TEXT)).GetDetail(),
      Substitute(MSG_NEW_BIO_ERR, 112));
}

/// Asserts a bundle is invalid if it contains one expired and one non-expired certificate
/// no matter the position of the expired cert in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleTwoCertsExpired) {
  // Expired cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", read_cert(EXPIRED_CERT),
      read_cert(VALID_CERT_1))), _msg_time(0, 1));

  // Expired cert is last in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", read_cert(VALID_CERT_1),
      read_cert(EXPIRED_CERT))), _msg_time(0, 1));
}

/// Asserts a bundle is invalid if it contains one future dated and one valid certificate
/// no matter the position of the future dated cert in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleTwoCertsFutureDated) {
  // Expired cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", read_cert(FUTURE_CERT),
      read_cert(VALID_CERT_1))), _msg_time(1, 0));

  // Expired cert is last in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", read_cert(VALID_CERT_1),
      read_cert(FUTURE_CERT))), _msg_time(1, 0));
}

/// Asserts a bundle is invalid if it contains one valid and one invalid certificate
/// no matter the position of the expired cert in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleTwoCertsNotCertFirst) {
  // Invalid cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", INVALID_CERT_TEXT,
      read_cert(VALID_CERT_1))), MSG_NONE_VALID);

  // Invalid cert is last in the bundle.
  EXPECT_STR_CONTAINS(ValidatePemBundle(Substitute("$0$1", read_cert(VALID_CERT_1),
      INVALID_CERT_TEXT)).GetDetail(), Substitute(MSG_NEW_BIO_ERR, 112));
}

/// Asserts a bundle is invalid if it contains two valid, one expired, and one future
/// dated certificate no matter the position of the certs in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleFourCerts) {
  // Expired and future dated certs are first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", read_cert(EXPIRED_CERT),
      read_cert(FUTURE_CERT), read_cert(VALID_CERT_1), read_cert(VALID_CERT_2))),
      _msg_time(1, 1));

  // Expired and future dated certs are last in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2$3", read_cert(VALID_CERT_1),
      read_cert(VALID_CERT_2), read_cert(EXPIRED_CERT), read_cert(FUTURE_CERT))),
      _msg_time(1, 1));

  // Expired and future dated certs are intermixed in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1$2$3", read_cert(VALID_CERT_1),
      read_cert(FUTURE_CERT), read_cert(VALID_CERT_2), read_cert(EXPIRED_CERT))),
      _msg_time(1, 1));
}

/// Asserts a bundle is invalid if it contains one expired and one future dated
/// certificate no matter the position of the certs in the bundle.
TEST_F(OpenSSLUtilTest, ValidatePemBundleOneExpiredOneFutureDated) {
  // Expired cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", read_cert(EXPIRED_CERT),
      read_cert(FUTURE_CERT))), _msg_time(1, 1));

  // Future dated cert is first in the bundle.
  ASSERT_ERROR_MSG(ValidatePemBundle(Substitute("$0$1", read_cert(FUTURE_CERT),
      read_cert(EXPIRED_CERT))), _msg_time(1, 1));
}

} // namespace impala
