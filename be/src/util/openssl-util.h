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

#pragma once

#include <openssl/aes.h>
#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>

#include "common/status.h"

namespace impala {

// From https://github.com/apache/kudu/commit/b88117415a02699c12a6eacbf065c4140ee0963c
//
// Hard code OpenSSL flag values from OpenSSL 1.0.1e[1][2] when compiling
// against OpenSSL 1.0.0 and below. We detect when running against a too-old
// version of OpenSSL using these definitions at runtime so that Kudu has full
// functionality when run against a new OpenSSL version, even if it's compiled
// against an older version.
//
// [1]: https://github.com/openssl/openssl/blob/OpenSSL_1_0_1e/ssl/ssl.h#L605-L609
// [2]: https://github.com/openssl/openssl/blob/OpenSSL_1_0_1e/ssl/tls1.h#L166-L172
#ifndef TLS1_1_VERSION
#define TLS1_1_VERSION 0x0302
#endif
#ifndef TLS1_2_VERSION
#define TLS1_2_VERSION 0x0303
#endif

/// Returns the maximum supported TLS version available in the linked OpenSSL library.
int MaxSupportedTlsVersion();

/// Returns true if, per the process configuration flags, server<->server communications
/// should use TLS.
bool IsInternalTlsConfigured();

/// Returns true if, per the process configuration flags, client<->server communications
/// should use TLS.
bool IsExternalTlsConfigured();

/// Add entropy from the system RNG to OpenSSL's global RNG. Called at system startup
/// and again periodically to add new entropy.
void SeedOpenSSLRNG();

/// Returns true if OpenSSL's FIPS mode is enabled. This calculation varies by the OpenSSL
/// version, so it is useful to have a shared function.
inline bool IsFIPSMode() {
#if OPENSSL_VERSION_NUMBER < 0x30000000L
  return FIPS_mode() > 0;
#else
  return EVP_default_properties_is_fips_enabled(nullptr) > 0;
#endif
};

enum AES_CIPHER_MODE {
  AES_256_CFB,
  AES_256_CTR,
  AES_256_GCM
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

/// Stores a random key that it can use to calculate and verify HMACs of data buffers for
/// authentication, eg. checking signatures of cookies. A SHA256 hash is used internally.
class AuthenticationHash {
 public:
  AuthenticationHash();

  /// Computes the HMAC of 'data', which has length 'len', and stores it in 'out', which
  /// must already be allocated with a length of HashLen() bytes. Returns an error if
  /// computing the hash was unsuccessful.
  Status Compute(const uint8_t* data, int64_t len, uint8_t* out) const WARN_UNUSED_RESULT;

  /// Computes the HMAC of 'data', which has length 'len', and returns true if it matches
  /// 'signature', which is expected to have length HashLen().
  bool Verify(const uint8_t* data, int64_t len,
      const uint8_t* signature) const WARN_UNUSED_RESULT;

  /// Returns the length in bytes of the generated hashes. Currently we always use SHA256.
  static int HashLen() { return SHA256_DIGEST_LENGTH; }

 private:
  /// An AES 256-bit key.
  uint8_t key_[SHA256_DIGEST_LENGTH];
};

/// The key and initialization vector (IV) required to encrypt and decrypt a buffer of
/// data. This should be regenerated for each buffer of data.
///
/// We use AES with a 256-bit key and GCM/CTR/CFB cipher block mode, which gives us a
/// stream cipher that can support arbitrary-length ciphertexts. The mode is chosen
/// depends on the OpenSSL version & the hardware support at runtime. The IV is used as
/// an input to the cipher as the "block to supply before the first block of plaintext".
/// This is required because all ciphers (except the weak ECB) are built such that each
/// block depends on the output from the previous block. Since the first block doesn't
/// have a previous block, we supply this IV. Think of it  as starting off the chain of
/// encryption.
///
/// Notes for GCM:
/// (1) GCM mode was supported since OpenSSL 1.0.1, however the tag verification
/// in decryption was only supported since OpenSSL 1.0.1d.
/// (2) The plaintext and the Additional Authenticated Data(AAD) are the two
/// categories of data that GCM protects. GCM protects the authenticity of the
/// plaintext and the AAD, and GCM also protects the confidentiality of the
/// plaintext. The AAD itself is not required or won't change the security.
/// In our case(Spill to Disk), we just ignore the AAD.

class EncryptionKey {
 public:
  EncryptionKey() : initialized_(false) { mode_ = GetSupportedDefaultMode(); }

  /// Initializes a key for temporary use with randomly generated data, and clears the
  /// tag for GCM mode. Reinitializes with new random values if the key was already
  /// initialized. We use AES-GCM/AES-CTR/AES-CFB mode so key/IV pairs should not be
  /// reused. This function automatically reseeds the RNG periodically, so callers do
  /// not need to do it.
  void InitializeRandom();

  /// Encrypts a buffer of input data 'data' of length 'len' into an output buffer 'out'.
  /// Exactly 'len' bytes will be written to 'out'. This key must be initialized before
  /// calling. Operates in-place if 'in' == 'out', otherwise the buffers must not overlap.
  /// For GCM mode, the hash tag will be kept inside(gcm_tag_ variable).
  Status Encrypt(const uint8_t* data, int64_t len, uint8_t* out) WARN_UNUSED_RESULT;

  /// Decrypts a buffer of input data 'data' of length 'len' that was encrypted with this
  /// key into an output buffer 'out'. Exactly 'len' bytes will be written to 'out'.
  /// This key must be initialized before calling. Operates in-place if 'in' == 'out',
  /// otherwise the buffers must not overlap. For GCM mode, the hash tag, which is
  /// computed during encryption, will be used for intgerity verification.
  Status Decrypt(const uint8_t* data, int64_t len, uint8_t* out) WARN_UNUSED_RESULT;

  /// Specify a cipher mode. Currently used only for testing but maybe in future we
  /// can provide a configuration option for the end user who can choose a preferred
  /// mode(GCM, CTR, CFB...) based on their software/hardware environment.
  /// If not supported, fall back to the supported mode at runtime.
  void SetCipherMode(AES_CIPHER_MODE m);

  /// If is GCM mode at runtime
  bool IsGcmMode() const { return mode_ == AES_256_GCM; }

  /// Returns the a default mode which is supported at runtime. If GCM mode
  /// is supported, return AES_256_GCM as the default. If GCM is not supported,
  /// but CTR is still supported, return AES_256_CTR. When both GCM and
  /// CTR modes are not supported, return AES_256_CFB.
  static AES_CIPHER_MODE GetSupportedDefaultMode();

  /// Converts mode type to string.
  static const std::string ModeToString(AES_CIPHER_MODE m);

 private:
  /// Helper method that encrypts/decrypts if 'encrypt' is true/false respectively.
  /// A buffer of input data 'data' of length 'len' is encrypted/decrypted with this
  /// key into an output buffer 'out'. Exactly 'len' bytes will be written to 'out'.
  /// This key must be initialized before calling. Operates in-place if 'in' == 'out',
  /// otherwise the buffers must not overlap.
  Status EncryptInternal(bool encrypt, const uint8_t* data, int64_t len,
      uint8_t* out) WARN_UNUSED_RESULT;

  /// Check if mode m is supported at runtime
  static bool IsModeSupported(AES_CIPHER_MODE m);

  /// Track whether this key has been initialized, to avoid accidentally using
  /// uninitialized keys.
  bool initialized_;

  /// Returns a EVP_CIPHER according to cipher mode at runtime
  const EVP_CIPHER* GetCipher() const;

  /// An AES 256-bit key.
  uint8_t key_[32];

  /// An initialization vector to feed as the first block to AES.
  uint8_t iv_[AES_BLOCK_SIZE];

  /// Tag for GCM mode
  uint8_t gcm_tag_[AES_BLOCK_SIZE];

  /// Cipher Mode
  AES_CIPHER_MODE mode_;
};

}
