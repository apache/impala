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
#include "testutil/gtest-util.h"
#include "util/openssl-util.h"

using std::uniform_int_distribution;
using std::mt19937_64;

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
    vector<uint8_t> scratch(buffer_size); // Scratch buffer for in-place encryption.
    if (buffer_size % 8 == 0) {
      GenerateRandomData(original.data(), buffer_size);
    } else {
      GenerateRandomBytes(original.data(), buffer_size);
    }

    // Check all the modes
    AES_CIPHER_MODE modes[] = {AES_256_GCM, AES_256_CTR, AES_256_CFB};
    for (auto m : modes) {
      memcpy(scratch.data(), original.data(), buffer_size);

      EncryptionKey key;
      key.InitializeRandom();
      key.SetCipherMode(m);

      ASSERT_OK(key.Encrypt(scratch.data(), buffer_size, scratch.data()));
      // Check that encryption did something
      ASSERT_NE(0, memcmp(original.data(), scratch.data(), buffer_size));
      ASSERT_OK(key.Decrypt(scratch.data(), buffer_size, scratch.data()));
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

  // Check both CTR & CFB
  AES_CIPHER_MODE modes[] = {AES_256_GCM, AES_256_CTR, AES_256_CFB};
  for (auto m : modes) {
    // Iterate multiple times to ensure that key regeneration works correctly.
    EncryptionKey key;
    for (int i = 0; i < 2; ++i) {
      key.InitializeRandom(); // Generate a new key for each iteration.
      key.SetCipherMode(m);

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
  key.InitializeRandom();
  key.SetCipherMode(AES_256_GCM);

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
    key.InitializeRandom();
  }
}
}

