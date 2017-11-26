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

#ifndef IMPALA_UTIL_OPENSSL_UTIL_H
#define IMPALA_UTIL_OPENSSL_UTIL_H

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

#include "common/status.h"

namespace impala {

#define OPENSSL_VERSION_1_0_1 0x1000100L

/// Add entropy from the system RNG to OpenSSL's global RNG. Called at system startup
/// and again periodically to add new entropy.
void SeedOpenSSLRNG();

enum AES_CIPHER_MODE {
  AES_256_CTR,
  AES_256_CFB,
  AES_256_GCM // not supported now.
};

/// The hash of a data buffer used for checking integrity. A SHA256 hash is used
/// internally.
class IntegrityHash {
 public:
  /// Computes the hash of the data in a buffer and stores it in this object.
  void Compute(const uint8_t* data, int64_t len);

  /// Verify that the data in a buffer matches this hash. Returns true on match, false
  /// otherwise.
  bool Verify(const uint8_t* data, int64_t len) const WARN_UNUSED_RESULT;

 private:
  uint8_t hash_[SHA256_DIGEST_LENGTH];
};

/// The key and initialization vector (IV) required to encrypt and decrypt a buffer of
/// data. This should be regenerated for each buffer of data.
///
/// We use AES with a 256-bit key and CTR/CFB cipher block mode, which gives us a stream
/// cipher that can support arbitrary-length ciphertexts. If OpenSSL version at runtime
/// is 1.0.1 or above, CTR mode is used, otherwise CFB mode is used. The IV is used as
/// an input to the cipher as the "block to supply before the first block of plaintext".
/// This is required because all ciphers (except the weak ECB) are built such that each
/// block depends on the output from the previous block. Since the first block doesn't
/// have a previous block, we supply this IV. Think of it  as starting off the chain of
/// encryption.
class EncryptionKey {
 public:
  EncryptionKey() : initialized_(false) {
    mode_ = SSLeay() < OPENSSL_VERSION_1_0_1 ? AES_256_CFB : AES_256_CTR;
  }

  /// Initialize a key for temporary use with randomly generated data. Reinitializes with
  /// new random values if the key was already initialized. We use AES-CTR/AES-CFB mode
  /// so key/IV pairs should not be reused. This function automatically reseeds the RNG
  /// periodically, so callers do not need to do it.
  void InitializeRandom();

  /// Encrypts a buffer of input data 'data' of length 'len' into an output buffer 'out'.
  /// Exactly 'len' bytes will be written to 'out'. This key must be initialized before
  /// calling. Operates in-place if 'in' == 'out', otherwise the buffers must not overlap.
  Status Encrypt(const uint8_t* data, int64_t len, uint8_t* out) const WARN_UNUSED_RESULT;

  /// Decrypts a buffer of input data 'data' of length 'len' that was encrypted with this
  /// key into an output buffer 'out'. Exactly 'len' bytes will be written to 'out'.
  /// This key must be initialized before calling. Operates in-place if 'in' == 'out',
  /// otherwise the buffers must not overlap.
  Status Decrypt(const uint8_t* data, int64_t len, uint8_t* out) const WARN_UNUSED_RESULT;

  /// Specify a cipher mode. Currently used only for testing but maybe in future we
  /// can provide a configuration option for the end user who can choose a preferred
  /// mode(GCM, CTR, CFB...) based on their software/hardware environment.
  void SetCipherMode(AES_CIPHER_MODE m) { mode_ = m; }

 private:
  /// Helper method that encrypts/decrypts if 'encrypt' is true/false respectively.
  /// A buffer of input data 'data' of length 'len' is encrypted/decrypted with this
  /// key into an output buffer 'out'. Exactly 'len' bytes will be written to 'out'.
  /// This key must be initialized before calling. Operates in-place if 'in' == 'out',
  /// otherwise the buffers must not overlap.
  Status EncryptInternal(bool encrypt, const uint8_t* data, int64_t len,
      uint8_t* out) const WARN_UNUSED_RESULT;

  /// Track whether this key has been initialized, to avoid accidentally using
  /// uninitialized keys.
  bool initialized_;

  /// return a EVP_CIPHER according to cipher mode at runtime
  const EVP_CIPHER* GetCipher() const;

  /// An AES 256-bit key.
  uint8_t key_[32];

  /// An initialization vector to feed as the first block to AES.
  uint8_t iv_[AES_BLOCK_SIZE];

  /// Cipher Mode
  AES_CIPHER_MODE mode_;
};
}

#endif
