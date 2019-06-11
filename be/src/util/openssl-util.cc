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

#include <limits.h>
#include <sstream>

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
  // that the OpenSSL library we're linked against supports only up to TLS1.2
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

void EncryptionKey::InitializeRandom() {
  uint64_t next_key_num = keys_generated.Add(1);
  if (next_key_num % RNG_RESEED_INTERVAL == 0) {
    SeedOpenSSLRNG();
  }
  RAND_bytes(key_, sizeof(key_));
  RAND_bytes(iv_, sizeof(iv_));
  memset(gcm_tag_, 0, sizeof(gcm_tag_));
  initialized_ = true;
}

Status EncryptionKey::Encrypt(const uint8_t* data, int64_t len, uint8_t* out) {
  return EncryptInternal(true, data, len, out);
}

Status EncryptionKey::Decrypt(const uint8_t* data, int64_t len, uint8_t* out) {
  return EncryptInternal(false, data, len, out);
}

Status EncryptionKey::EncryptInternal(
    bool encrypt, const uint8_t* data, int64_t len, uint8_t* out) {
  DCHECK(initialized_);
  DCHECK_GE(len, 0);
  const char* err_context = encrypt ? "encrypting" : "decrypting";
  // Create and initialize the context for encryption
  ScopedEVPCipherCtx ctx(0);

  // Start encryption/decryption.  We use a 256-bit AES key, and the cipher block mode
  // is either CTR or CFB(stream cipher), both of which support arbitrary length
  // ciphertexts - it doesn't have to be a multiple of 16 bytes. Additionally, CTR
  // mode is well-optimized(instruction level parallelism) with hardware acceleration
  // on x86 and PowerPC
  const EVP_CIPHER* evpCipher = GetCipher();
  int success = encrypt ? EVP_EncryptInit_ex(ctx.ctx, evpCipher, NULL, key_, iv_) :
                          EVP_DecryptInit_ex(ctx.ctx, evpCipher, NULL, key_, iv_);
  if (success != 1) {
    return OpenSSLErr(encrypt ? "EVP_EncryptInit_ex" : "EVP_DecryptInit_ex", err_context);
  }
  if (IsGcmMode()) {
    if (EVP_CIPHER_CTX_ctrl(ctx.ctx, EVP_CTRL_GCM_SET_IVLEN, AES_BLOCK_SIZE, NULL)
        != 1) {
      return OpenSSLErr("EVP_CIPHER_CTX_ctrl", err_context);
    }
  }

  // The OpenSSL encryption APIs use ints for buffer lengths for some reason. To support
  // larger buffers we need to chunk larger buffers into smaller parts.
  int64_t offset = 0;
  while (offset < len) {
    int in_len = static_cast<int>(min<int64_t>(len - offset, numeric_limits<int>::max()));
    int out_len;
    success = encrypt ?
        EVP_EncryptUpdate(ctx.ctx, out + offset, &out_len, data + offset, in_len) :
        EVP_DecryptUpdate(ctx.ctx, out + offset, &out_len, data + offset, in_len);
    if (success != 1) {
      return OpenSSLErr(encrypt ? "EVP_EncryptUpdate" : "EVP_DecryptUpdate", err_context);
    }
    // This is safe because we're using CTR/CFB mode without padding.
    DCHECK_EQ(in_len, out_len);
    offset += in_len;
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
  success = encrypt ? EVP_EncryptFinal_ex(ctx.ctx, out + offset, &final_out_len) :
                      EVP_DecryptFinal_ex(ctx.ctx, out + offset, &final_out_len);
  if (success != 1) {
    return OpenSSLErr(encrypt ? "EVP_EncryptFinal" : "EVP_DecryptFinal", err_context);
  }

  if (IsGcmMode() && encrypt) {
    if (EVP_CIPHER_CTX_ctrl(ctx.ctx, EVP_CTRL_GCM_GET_TAG, AES_BLOCK_SIZE, gcm_tag_)
        != 1) {
      return OpenSSLErr("EVP_CIPHER_CTX_ctrl", err_context);
    }
  }
  // Again safe due to GCM/CTR/CFB with no padding
  DCHECK_EQ(final_out_len, 0);
  DCHECK_EQ(ERR_peek_error(), 0) << "Did not clear OpenSSL error queue";
  return Status::OK();
}

const EVP_CIPHER* EncryptionKey::GetCipher() const {
  // use weak symbol to avoid compiling error on OpenSSL 1.0.0 environment
  if (mode_ == AES_256_CTR) return EVP_aes_256_ctr();
  if (mode_ == AES_256_GCM) return EVP_aes_256_gcm();

  return EVP_aes_256_cfb();
}

void EncryptionKey::SetCipherMode(AES_CIPHER_MODE m) {
  mode_ = m;

  if (!IsModeSupported(m)) {
    mode_ = GetSupportedDefaultMode();
    LOG(WARNING) << Substitute("$0 is not supported, fall back to $1.",
        ModeToString(m), ModeToString(mode_));
  }
}

bool EncryptionKey::IsModeSupported(AES_CIPHER_MODE m) {
  switch (m) {
    case AES_256_GCM:
      // It becomes a bit tricky for GCM mode, because GCM mode is enabled since
      // OpenSSL 1.0.1, but the tag validation only works since 1.0.1d. We have
      // to make sure that OpenSSL version >= 1.0.1d for GCM. So we need
      // SSLeay(). Note that SSLeay() may return the compiling version on
      // certain platforms if it was built against an older version(see:
      // IMPALA-6418). In this case, it will return false, and EncryptionKey
      // will try to fall back to CTR mode, so it is not ideal but is OK to use
      // SSLeay() for GCM mode here since in the worst case, we will be using
      // AES_256_CTR in a system that supports AES_256_GCM.
      return (CpuInfo::IsSupported(CpuInfo::PCLMULQDQ)
          && SSLeay() >= OPENSSL_VERSION_1_0_1D && EVP_aes_256_gcm);

    case AES_256_CTR:
      // If TLS1.2 is supported, then we're on a verison of OpenSSL that
      // supports AES-256-CTR.
      return (MaxSupportedTlsVersion() >= TLS1_2_VERSION && EVP_aes_256_ctr);

    case AES_256_CFB:
      return true;

    default:
      return false;
  }
}

AES_CIPHER_MODE EncryptionKey::GetSupportedDefaultMode() {
  if (IsModeSupported(AES_256_GCM)) return AES_256_GCM;
  if (IsModeSupported(AES_256_CTR)) return AES_256_CTR;
  return AES_256_CFB;
}

const string EncryptionKey::ModeToString(AES_CIPHER_MODE m) {
  switch(m) {
    case AES_256_GCM: return "AES-GCM";
    case AES_256_CTR: return "AES-CTR";
    case AES_256_CFB: return "AES-CFB";
  }
  return "Unknown mode";
}
}
