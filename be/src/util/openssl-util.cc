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

#include "util/openssl-util.h"

#include <boost/algorithm/string.hpp>
#include <string_view>

#include <limits.h>
#include <sstream>
#include <iostream>

#include <glog/logging.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>
#include <openssl/tls1.h>

#include "common/atomic.h"
#include "gutil/port.h" // ATTRIBUTE_WEAK
#include "gutil/strings/substitute.h"

#include "common/names.h"
#include "cpu-info.h"

DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_cipher_list);

/// OpenSSL 1.0.1d
#define OPENSSL_VERSION_1_0_1D 0x1000104fL

/// If not defined at compile time, define them manually
/// see: openssl/evp.h
#ifndef EVP_CIPH_GCM_MODE
#define EVP_CTRL_GCM_SET_IVLEN 0x9
#define EVP_CTRL_GCM_GET_TAG 0x10
#define EVP_CTRL_GCM_SET_TAG 0x11
#endif

extern "C" {
ATTRIBUTE_WEAK
const EVP_CIPHER* EVP_aes_256_ctr();

ATTRIBUTE_WEAK
const EVP_CIPHER* EVP_aes_256_gcm();

ATTRIBUTE_WEAK
const EVP_CIPHER* EVP_aes_128_gcm();
}

namespace impala {

// Counter to track the number of encryption keys generated. Incremented before each key
// is generated.
static AtomicInt64 keys_generated(0);

// Reseed the OpenSSL with new entropy after generating this number of keys.
static const int RNG_RESEED_INTERVAL = 128;

// Number of bytes of entropy to add at RNG_RESEED_INTERVAL.
static const int RNG_RESEED_BYTES = 512;

int MaxSupportedTlsVersion() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  return SSLv23_method()->version;
#else
  // OpenSSL 1.1+ doesn't let us detect the supported TLS version at runtime. Assume
  // that the OpenSSL library we're linked against supports only up to TLS1.2.
  return TLS1_2_VERSION;
#endif
}

bool IsInternalTlsConfigured() {
  // Enable SSL between servers only if both the client validation certificate and the
  // server certificate are specified. 'Client' here means clients that are used by Impala
  // services to contact other Impala services (as distinct from user clients of Impala
  // like the shell), and 'servers' are the processes that serve those clients. The server
  // needs a certificate (FLAGS_ssl_server_certificate) to demonstrate it is who the
  // client thinks it is; the client needs a certificate (FLAGS_ssl_client_ca_certificate)
  // to validate that assertion from the server.
  return !FLAGS_ssl_client_ca_certificate.empty() &&
      !FLAGS_ssl_server_certificate.empty() && !FLAGS_ssl_private_key.empty();
}

bool IsExternalTlsConfigured() {
  // If the ssl_server_certificate is set, then external TLS is configured, i.e. external
  // clients can talk to Impala at least over unauthenticated TLS.
  return !FLAGS_ssl_server_certificate.empty() && !FLAGS_ssl_private_key.empty();
}

/// Wrapper around EVP_CIPHER_CTX that automatically cleans up the context
/// when it is destroyed. This helps avoid leaks like IMPALA-7145.
struct ScopedEVPCipherCtx {
  DISALLOW_COPY_AND_ASSIGN(ScopedEVPCipherCtx);

  explicit ScopedEVPCipherCtx(int padding) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    ctx = static_cast<EVP_CIPHER_CTX*>(malloc(sizeof(*ctx)));
    EVP_CIPHER_CTX_init(ctx);
#else
    ctx = EVP_CIPHER_CTX_new();
#endif
    EVP_CIPHER_CTX_set_padding(ctx, padding);
  }

  ~ScopedEVPCipherCtx() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    EVP_CIPHER_CTX_cleanup(ctx);
    free(ctx);
#else
    EVP_CIPHER_CTX_free(ctx);
#endif
  }

  EVP_CIPHER_CTX* ctx;
};

// Callback used by OpenSSLErr() - write the error given to us through buf to the
// stringstream that's passed in through ctx.
static int OpenSSLErrCallback(const char* buf, size_t len, void* ctx) {
  stringstream* errstream = static_cast<stringstream*>(ctx);
  *errstream << buf;
  return 1;
}

// Called upon OpenSSL errors; returns a non-OK status with an error message.
static Status OpenSSLErr(const string& function, const string& context) {
  stringstream errstream;
  ERR_print_errors_cb(OpenSSLErrCallback, &errstream);
  return Status(Substitute("OpenSSL error in $0 $1: $2", function, context, errstream.str()));
}

void SeedOpenSSLRNG() {
  RAND_load_file("/dev/urandom", RNG_RESEED_BYTES);
}

void IntegrityHash::Compute(const uint8_t* data, int64_t len) {
  // Explicitly ignore the return value from SHA256(); it can't fail.
  (void)SHA256(data, len, hash_);
  DCHECK_EQ(ERR_peek_error(), 0) << "Did not clear OpenSSL error queue";
}

bool IntegrityHash::Verify(const uint8_t* data, int64_t len) const {
  IntegrityHash test_hash;
  test_hash.Compute(data, len);
  return memcmp(hash_, test_hash.hash_, sizeof(hash_)) == 0;
}

AuthenticationHash::AuthenticationHash() {
  uint64_t next_key_num = keys_generated.Add(1);
  if (next_key_num % RNG_RESEED_INTERVAL == 0) {
    SeedOpenSSLRNG();
  }
  RAND_bytes(key_, sizeof(key_));
}

Status AuthenticationHash::Compute(const uint8_t* data, int64_t len, uint8_t* out) const {
  uint32_t out_len;
  uint8_t* result =
      HMAC(EVP_sha256(), key_, SHA256_DIGEST_LENGTH, data, len, out, &out_len);
  if (result == nullptr) {
    return OpenSSLErr("HMAC", "computing");
  }
  DCHECK_EQ(out_len, HashLen());
  DCHECK_EQ(ERR_peek_error(), 0) << "Did not clear OpenSSL error queue";
  return Status::OK();
}

bool AuthenticationHash::Verify(
    const uint8_t* data, int64_t len, const uint8_t* signature) const {
  uint8_t out[HashLen()];
  Status compute_status = Compute(data, len, out);
  if (!compute_status.ok()) {
    LOG(ERROR) << "Failed to compute hash for verification: " << compute_status;
    return false;
  }
  return memcmp(signature, out, HashLen()) == 0;
}

int GetKeyLenForMode(AES_CIPHER_MODE m) {
  switch (m) {
    case AES_CIPHER_MODE::AES_128_ECB:
    case AES_CIPHER_MODE::AES_128_GCM:
      return 16;
    case AES_CIPHER_MODE::AES_256_ECB:
    case AES_CIPHER_MODE::AES_256_GCM:
    case AES_CIPHER_MODE::AES_256_CTR:
    case AES_CIPHER_MODE::AES_256_CFB:
      return 32;
    default:
      return -1;
  }
}

Status ValidateModeAndKeyLength(AES_CIPHER_MODE m, int key_len) {
  if (m == AES_CIPHER_MODE::INVALID) {
    return Status("Invalid AES mode specified.");
  }

  int expected_key_len = GetKeyLenForMode(m);
  DCHECK_GT(expected_key_len, 0);
  if (key_len != expected_key_len) {
    return Status("Mismatch between mode and key length.");
  }

  return Status::OK();
}

Status EncryptionKey::InitializeRandom(int iv_len, AES_CIPHER_MODE m) {
  mode_ = m;
  if (!IsModeSupported(m)) {
    mode_ = GetSupportedDefaultMode();
    LOG(WARNING) << Substitute("$0 is not supported, fall back to $1.",
        ModeToString(m), ModeToString(mode_));
  }

  if (m == AES_CIPHER_MODE::INVALID) {
    return Status("Invalid AES mode specified.");
  }

  uint64_t next_key_num = keys_generated.Add(1);
  if (next_key_num % RNG_RESEED_INTERVAL == 0) {
    SeedOpenSSLRNG();
  }
  RAND_bytes(key_, sizeof(key_));
  RAND_bytes(iv_, sizeof(iv_));
  memset(gcm_tag_, 0, sizeof(gcm_tag_));
  initialized_ = true;
  key_length_ = GetKeyLenForMode(mode_);
  iv_length_ = iv_len;
  return Status::OK();
}

Status EncryptionKey::Encrypt(const uint8_t* data, int64_t len, uint8_t* out,
    int64_t* out_len) {
  return EncryptInternal(true, data, len, out, out_len);
}

Status EncryptionKey::Decrypt(const uint8_t* data, int64_t len, uint8_t* out,
    int64_t* out_len) {
  return EncryptInternal(false, data, len, out, out_len);
}

Status EncryptionKey::EncryptInternal(
    bool encrypt, const uint8_t* data, int64_t len, uint8_t* out, int64_t* out_len) {
  DCHECK(initialized_);
  DCHECK_GE(len, 0);
  if (IsEcbMode()) {
    if ((encrypt && len > numeric_limits<int>::max() - AES_BLOCK_SIZE)
        || (!encrypt && len > numeric_limits<int>::max())) {
      return Status("Input buffer length exceeds the supported length for ECB mode.");
    }
  }

  const char* err_context = encrypt ? "encrypting" : "decrypting";
  // Create and initialize the context for encryption.
  // If it is ECB mode then padding will be enabled
  // for this ctx, otherwise it will be disabled.
  bool padding_enabled = IsEcbMode();
  int padding_flag = padding_enabled ? 1 : 0;
  ScopedEVPCipherCtx ctx(padding_flag);

  // Start encryption/decryption. We use a 128/256-bit AES key and support GCM, CTR
  // CFB and ECB for encryption and decryption. When the cipher block mode is either
  // GCM, CTR or CFB(stream cipher), it supports arbitrary length ciphertexts - it
  // doesn't have to be a multiple of 16 bytes. While for ECB decryption,
  // the length has to be multiples of 16. Additionally, CTR mode is
  // well-optimized (instruction level parallelism) with hardware acceleration
  // on x86 and PowerPC.

  // In the first initialization, only evpCipher is initialized, and in the second
  // initialization, the key and IV vector are set. This approach is necessary because a
  // variable-length IV vector is used. Therefore, in GCM mode, the IV length must be
  // initialized before setting the IV vector.
  const EVP_CIPHER* evpCipher = GetCipher();
  DCHECK(evpCipher != nullptr);
  int success = encrypt ?
      EVP_EncryptInit_ex(ctx.ctx, evpCipher, nullptr, nullptr, nullptr):
      EVP_DecryptInit_ex(ctx.ctx, evpCipher, nullptr, nullptr, nullptr);
  if (success != 1) {
    return OpenSSLErr(encrypt ? "EVP_EncryptInit_ex" : "EVP_DecryptInit_ex", err_context);
  }

  if (IsGcmMode()) {
    // Set iv_vector for GCM mode.
    if (EVP_CIPHER_CTX_ctrl(ctx.ctx, EVP_CTRL_GCM_SET_IVLEN, iv_length_, nullptr)!= 1) {
      return OpenSSLErr("EVP_CIPHER_CTX_ctrl", err_context);
    }
  }
  // setting iv after changing iv len, see https://github.com/openssl/openssl/pull/22590
  success = encrypt ? EVP_EncryptInit_ex(ctx.ctx, nullptr, nullptr, key_, iv_):
                      EVP_DecryptInit_ex(ctx.ctx, nullptr, nullptr, key_, iv_);
  if (success != 1) {
    return OpenSSLErr(encrypt ? "EVP_EncryptInit_ex" : "EVP_DecryptInit_ex", err_context);
  }

  // The OpenSSL encryption APIs use INT for buffer lengths. To support larger buffers,
  // larger buffers need to be chunked into smaller parts.
  int64_t output_offset = 0;
  int64_t input_offset = 0;
  while (input_offset < len) {
    int in_len = static_cast<int>(min<int64_t>(len - input_offset,
        numeric_limits<int>::max()));
    int output_len;
    uint8_t* out_addr = out + output_offset;
    const uint8_t* in_addr = data + input_offset;
    success = encrypt ?
        EVP_EncryptUpdate(ctx.ctx, out_addr, &output_len, in_addr, in_len) :
        EVP_DecryptUpdate(ctx.ctx, out_addr, &output_len, in_addr, in_len);
    if (success != 1) {
      return OpenSSLErr(encrypt ? "EVP_EncryptUpdate" : "EVP_DecryptUpdate", err_context);
    }
    output_offset += output_len;
    input_offset += in_len;
  }

  if (IsGcmMode() && !encrypt) {
    // Set expected tag value
    if (EVP_CIPHER_CTX_ctrl(ctx.ctx, EVP_CTRL_GCM_SET_TAG, AES_BLOCK_SIZE, gcm_tag_)
        != 1) {
      return OpenSSLErr("EVP_CIPHER_CTX_ctrl", err_context);
    }
  }

  // Finalize encryption or decryption.
  int final_out_len;
  success = encrypt ? EVP_EncryptFinal_ex(ctx.ctx, out + output_offset, &final_out_len) :
                      EVP_DecryptFinal_ex(ctx.ctx, out + output_offset, &final_out_len);

  if (success != 1) {
    return OpenSSLErr(encrypt ? "EVP_EncryptFinal" : "EVP_DecryptFinal", err_context);
  }

  if (out_len != nullptr) *out_len = output_offset + final_out_len;

  if (IsGcmMode() && encrypt) {
    if (EVP_CIPHER_CTX_ctrl(ctx.ctx, EVP_CTRL_GCM_GET_TAG, AES_BLOCK_SIZE, gcm_tag_)
        != 1) {
      return OpenSSLErr("EVP_CIPHER_CTX_ctrl", err_context);
    }
  }

  DCHECK_EQ(ERR_peek_error(), 0) << "Did not clear OpenSSL error queue";
  return Status::OK();
}

const EVP_CIPHER* EncryptionKey::GetCipher() const {
  // use weak symbol to avoid compiling error on OpenSSL 1.0.0 environment
  switch (mode_) {
    case AES_CIPHER_MODE::AES_256_CTR: return EVP_aes_256_ctr();
    case AES_CIPHER_MODE::AES_256_GCM: return EVP_aes_256_gcm();
    case AES_CIPHER_MODE::AES_256_CFB: return EVP_aes_256_cfb();
    case AES_CIPHER_MODE::AES_256_ECB: return EVP_aes_256_ecb();
    case AES_CIPHER_MODE::AES_128_GCM: return EVP_aes_128_gcm();
    case AES_CIPHER_MODE::AES_128_ECB: return EVP_aes_128_ecb();
    default: return nullptr;
  }
}

Status EncryptionKey::InitializeFields(const uint8_t* key, int key_len, const uint8_t* iv,
    int iv_len, AES_CIPHER_MODE m) {
  mode_ = m;
  if (!IsModeSupported(m)) {
    mode_ = GetSupportedDefaultMode();
    LOG(WARNING) << Substitute("$0 is not supported, fall back to $1.",
        ModeToString(m), ModeToString(mode_));
  }
  Status status = ValidateModeAndKeyLength(m, key_len);
  RETURN_IF_ERROR(status);

  key_length_ = key_len;
  iv_length_ = iv_len;
  memcpy(key_, key, key_length_);
  if (iv_length_ != 0) {
    memcpy(iv_, iv, iv_length_);
  }
  initialized_ = true;
  return Status::OK();
}

void EncryptionKey::SetGcmTag(const uint8_t* tag)  {
  memcpy(gcm_tag_, tag, AES_BLOCK_SIZE);
}

void EncryptionKey::GetGcmTag(uint8_t* out) const {
  memcpy(out, gcm_tag_, AES_BLOCK_SIZE);
}

bool EncryptionKey::IsModeSupported(AES_CIPHER_MODE m) {
  switch (m) {
      // It becomes a bit tricky for GCM mode, because GCM mode is enabled since
      // OpenSSL 1.0.1, but the tag validation only works since 1.0.1d. We have
      // to make sure that OpenSSL version >= 1.0.1d for GCM. So we need
      // SSLeay(). Note that SSLeay() may return the compiling version on
      // certain platforms if it was built against an older version(see:
      // IMPALA-6418). In this case, it will return false, and EncryptionKey
      // will try to fall back to CTR mode, so it is not ideal but is OK to use
      // SSLeay() for GCM mode here since in the worst case, we will be using
      // AES_256_CTR in a system that supports AES_256_GCM.
    case AES_CIPHER_MODE::AES_256_GCM:
      return (CpuInfo::IsSupported(CpuInfo::PCLMULQDQ)
          && SSLeay() >= OPENSSL_VERSION_1_0_1D && EVP_aes_256_gcm);

    case AES_CIPHER_MODE::AES_128_GCM:
      return (CpuInfo::IsSupported(CpuInfo::PCLMULQDQ)
          && SSLeay() >= OPENSSL_VERSION_1_0_1D && EVP_aes_128_gcm);

    case AES_CIPHER_MODE::AES_256_CTR:
      // If TLS1.2 is supported, then we're on a verison of OpenSSL that
      // supports AES-256-CTR.
      return (MaxSupportedTlsVersion() >= TLS1_2_VERSION && EVP_aes_256_ctr);

    case AES_CIPHER_MODE::AES_256_CFB:
    case AES_CIPHER_MODE::AES_256_ECB:
    case AES_CIPHER_MODE::AES_128_ECB:
      return true;
    case AES_CIPHER_MODE::INVALID:
      return false;
    default:
      return false;
  }
}

AES_CIPHER_MODE EncryptionKey::GetSupportedDefaultMode() {
  if (IsModeSupported(AES_CIPHER_MODE::AES_256_GCM)) {
    return AES_CIPHER_MODE::AES_256_GCM;
  }
  if (IsModeSupported(AES_CIPHER_MODE::AES_256_CTR)) {
    return AES_CIPHER_MODE::AES_256_CTR;
  }
  return AES_CIPHER_MODE::AES_256_CFB;
}

const string EncryptionKey::ModeToString(AES_CIPHER_MODE m) {
  switch(m) {
    case AES_CIPHER_MODE::AES_256_GCM: return std::string(AES_256_GCM_STR);
    case AES_CIPHER_MODE::AES_256_CTR: return std::string(AES_256_CTR_STR);
    case AES_CIPHER_MODE::AES_256_CFB: return std::string(AES_256_CFB_STR);
    case AES_CIPHER_MODE::AES_256_ECB: return std::string(AES_256_ECB_STR);
    case AES_CIPHER_MODE::AES_128_GCM: return std::string(AES_128_GCM_STR);
    case AES_CIPHER_MODE::AES_128_ECB: return std::string(AES_128_ECB_STR);
    case AES_CIPHER_MODE::INVALID: return "INVALID";
  }
  return "INVALID";
}

AES_CIPHER_MODE EncryptionKey::StringToMode(std:: string_view str) {
  if (boost::iequals(str, AES_256_GCM_STR)) {
    return AES_CIPHER_MODE::AES_256_GCM;
  } else if (boost::iequals(str, AES_256_CTR_STR)) {
    return AES_CIPHER_MODE::AES_256_CTR;
  } else if (boost::iequals(str, AES_256_CFB_STR)) {
    return AES_CIPHER_MODE::AES_256_CFB;
  } else if (boost::iequals(str, AES_256_ECB_STR)) {
    return AES_CIPHER_MODE::AES_256_ECB;
  } else if (boost::iequals(str, AES_128_GCM_STR)) {
    return AES_CIPHER_MODE::AES_128_GCM;
  } else if (boost::iequals(str, AES_128_ECB_STR)) {
    return AES_CIPHER_MODE::AES_128_ECB;
  }
  return AES_CIPHER_MODE::INVALID;
}
}