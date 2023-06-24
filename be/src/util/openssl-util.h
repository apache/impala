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

#include <string_view>

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

// Enum of all the AES modes that are currently supported. INVALID is used whenever
// user inputs a wrong AES Mode, for example, AES_128_CTF etc.
enum class AES_CIPHER_MODE {
  AES_256_CFB,
  AES_256_CTR,
  AES_256_GCM,
  AES_256_ECB,
  AES_128_GCM,
  AES_128_ECB,
  INVALID
};

// AES cipher mode strings as constexpr static std::string_view used in StringToMode()
// and ModeToString() functions for string comparisons.
static constexpr std::string_view AES_256_GCM_STR = "AES_256_GCM";
static constexpr std::string_view AES_256_CTR_STR = "AES_256_CTR";
static constexpr std::string_view AES_256_CFB_STR = "AES_256_CFB";
static constexpr std::string_view AES_256_ECB_STR = "AES_256_ECB";
static constexpr std::string_view AES_128_GCM_STR = "AES_128_GCM";
static constexpr std::string_view AES_128_ECB_STR = "AES_128_ECB";

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
/// AES is employed with a 256-bit/128-bit key, and the cipher block mode is chosen
/// from GCM, CTR, CFB, or ECB based on the OpenSSL version and hardware support
/// at runtime. This configuration yields a cipher capable of handling
/// arbitrary-length ciphertexts (except ECB). The IV serves as input to the cipher,
/// acting as the "block to supply before the first block of plaintext." This is necessary
/// because all ciphers (except ECB) are designed such that each block depends on
/// the output from the preceding block. Since the first block lacks a previous
/// block, the IV is supplied to initiate the encryption chain.
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
  EncryptionKey() : initialized_(false) { mode_ = AES_CIPHER_MODE::INVALID; }

  /// Initializes a key for temporary use with randomly generated data, and clears the
  /// tag for GCM mode. Reinitializes with new random values if the key was already
  /// initialized. We use AES-GCM/AES-CTR/AES-CFB mode so key/IV pairs should not be
  /// reused. This function automatically reseeds the RNG periodically, so callers do
  /// not need to do it.
  Status InitializeRandom(int iv_len, AES_CIPHER_MODE mode);

  /// Encrypts a buffer of input data 'data' of length 'len' into an output buffer 'out'.
  /// The 'out' buffer must contain sufficient length to hold the extra padding block in
  /// case of ECB mode. In other modes, the 'out' buffer should at least be as big as the
  /// length of the 'data' buffer. This key must be initialized before calling this
  /// function. If 'in' == 'out', the operation is performed in-place; otherwise, the
  /// buffers must not overlap. In GCM mode, the hash tag is kept internally (in the
  /// 'gcm_tag_' variable). 'out_len' (if not NULL) will be set to the output length.
  Status Encrypt(const uint8_t* data, int64_t len, uint8_t* out,
      int64_t* out_len = nullptr) WARN_UNUSED_RESULT;

  /// Decrypts a buffer of input data 'data' of length 'len', encrypted with this key,
  /// into an output buffer 'out'.'out' buffer should be at least as big as the
  /// length of the 'data' buffer.
  /// Prior to calling this function, the key must be initialized. If 'in' == 'out', the
  /// operation is performed in-place; otherwise, the buffers must not overlap. In GCM
  /// mode, the hash tag computed during encryption is used for integrity verification.
  /// 'out_len' (if not NULL) will be set to the output length.
  Status Decrypt(const uint8_t* data, int64_t len, uint8_t* out,
      int64_t* out_len = nullptr) WARN_UNUSED_RESULT;

  /// If it is GCM mode at runtime
  bool IsGcmMode() const {
      return mode_ == AES_CIPHER_MODE::AES_256_GCM
          || mode_ == AES_CIPHER_MODE::AES_128_GCM;
  }

  /// If it is ECB mode at runtime.
  bool IsEcbMode() const {
      return mode_ == AES_CIPHER_MODE::AES_256_ECB
          || mode_ == AES_CIPHER_MODE::AES_128_ECB;
  }

  /// It initializes 'key_' and 'iv_'.
  /// Specifies a cipher mode. It gives an option to configure this mode for end users,
  /// allowing them to choose a preferred mode (e.g., GCM, CTR, CFB) based on their
  /// software/hardware environment. If the specified mode is not supported, the
  /// implementation falls back to a supported mode at runtime.
  Status InitializeFields(const uint8_t* key, int key_len, const uint8_t* iv, int iv_len,
      AES_CIPHER_MODE mode);

  /// Getter function for 'gcm_tag_' which is required for encryption using GCM mode.
  void GetGcmTag(uint8_t* out) const;
  /// Setter function for setting 'gcm_tag_' which is required for decryption using GCM
  /// mode.
  void SetGcmTag(const uint8_t* tag);

  /// Returns the default mode which is supported at runtime. If GCM mode
  /// is supported, return AES_256_GCM as the default. If GCM is not supported,
  /// but CTR is still supported, return AES_256_CTR. When both GCM and
  /// CTR modes are not supported, return AES_256_CFB.
  /// ECB mode is not used unless requested by the user specifically.
  static AES_CIPHER_MODE GetSupportedDefaultMode();

  /// Converts mode type to string.
  static const std::string ModeToString(AES_CIPHER_MODE m);
  static AES_CIPHER_MODE StringToMode(std::string_view str);

 private:
  /// Helper method that encrypts/decrypts if 'encrypt' is true/false respectively.
  /// A buffer of input data 'data' of length 'len' is encrypted/decrypted with this
  /// key into an output buffer 'out'.
  /// The 'out' buffer must contain sufficient length to hold the extra padding block in
  /// case of ECB mode. In other modes, 'out' buffer should be at least as big as the
  /// length of the 'data' buffer.
  /// This key must be initialized before calling. Operates in-place if 'in' == 'out',
  /// otherwise the buffers must not overlap.
  /// 'out_len' (if not NULL) will be set to the output length.
  Status EncryptInternal(bool encrypt, const uint8_t* data, int64_t len,
      uint8_t* out, int64_t* out_len) WARN_UNUSED_RESULT;

  /// Check if mode m is supported at runtime
  static bool IsModeSupported(AES_CIPHER_MODE m);

  /// Track whether this key has been initialized, to avoid accidentally using
  /// uninitialized keys.
  bool initialized_;

  /// Returns a EVP_CIPHER according to cipher mode at runtime
  const EVP_CIPHER* GetCipher() const;

  /// An AES 256-bit key.
  uint8_t key_[32];
  int key_length_;

  /// An initialization vector to feed as the first block to AES.
  uint8_t iv_[AES_BLOCK_SIZE];
  int iv_length_;

  /// Tag for GCM mode
  uint8_t gcm_tag_[AES_BLOCK_SIZE];

  /// Cipher Mode
  AES_CIPHER_MODE mode_;
};

}