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

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/sha.h>

#include "common/atomic.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

namespace impala {

// Counter to track the number of encryption keys generated. Incremented before each key
// is generated.
static AtomicInt64 keys_generated(0);

// Reseed the OpenSSL with new entropy after generating this number of keys.
static const int RNG_RESEED_INTERVAL = 128;

// Number of bytes of entropy to add at RNG_RESEED_INTERVAL.
static const int RNG_RESEED_BYTES = 512;

// Callback used by OpenSSLErr() - write the error given to us through buf to the
// stringstream that's passed in through ctx.
static int OpenSSLErrCallback(const char* buf, size_t len, void* ctx) {
  stringstream* errstream = static_cast<stringstream*>(ctx);
  *errstream << buf;
  return 1;
}

// Called upon OpenSSL errors; returns a non-OK status with an error message.
static Status OpenSSLErr(const string& function) {
  stringstream errstream;
  ERR_print_errors_cb(OpenSSLErrCallback, &errstream);
  return Status(Substitute("OpenSSL error in $0: $1", function, errstream.str()));
}

void SeedOpenSSLRNG() {
  RAND_load_file("/dev/urandom", RNG_RESEED_BYTES);
}

void IntegrityHash::Compute(const uint8_t* data, int64_t len) {
  // Explicitly ignore the return value from SHA256(); it can't fail.
  (void)SHA256(data, len, hash_);
}

bool IntegrityHash::Verify(const uint8_t* data, int64_t len) const {
  IntegrityHash test_hash;
  test_hash.Compute(data, len);
  return memcmp(hash_, test_hash.hash_, sizeof(hash_)) == 0;
}

void EncryptionKey::InitializeRandom() {
  uint64_t next_key_num = keys_generated.Add(1);
  if (next_key_num % RNG_RESEED_INTERVAL == 0) {
    SeedOpenSSLRNG();
  }
  RAND_bytes(key_, sizeof(key_));
  RAND_bytes(iv_, sizeof(iv_));
  initialized_ = true;
}

Status EncryptionKey::Encrypt(const uint8_t* data, int64_t len, uint8_t* out) const {
  return EncryptInternal(true, data, len, out);
}

Status EncryptionKey::Decrypt(const uint8_t* data, int64_t len, uint8_t* out) const {
  return EncryptInternal(false, data, len, out);
}

Status EncryptionKey::EncryptInternal(
    bool encrypt, const uint8_t* data, int64_t len, uint8_t* out) const {
  DCHECK(initialized_);
  DCHECK_GE(len, 0);
  // Create and initialize the context for encryption
  EVP_CIPHER_CTX ctx;
  EVP_CIPHER_CTX_init(&ctx);
  EVP_CIPHER_CTX_set_padding(&ctx, 0);

  int success;

  // Start encryption/decryption.  We use a 256-bit AES key, and the cipher block mode
  // is CFB because this gives us a stream cipher, which supports arbitrary
  // length ciphertexts - it doesn't have to be a multiple of 16 bytes.
  success = encrypt ? EVP_EncryptInit_ex(&ctx, EVP_aes_256_cfb(), NULL, key_, iv_) :
                      EVP_DecryptInit_ex(&ctx, EVP_aes_256_cfb(), NULL, key_, iv_);
  if (success != 1) {
    return OpenSSLErr(encrypt ? "EVP_EncryptInit_ex" : "EVP_DecryptInit_ex");
  }

  // The OpenSSL encryption APIs use ints for buffer lengths for some reason. To support
  // larger buffers we need to chunk larger buffers into smaller parts.
  int64_t offset = 0;
  while (offset < len) {
    int in_len = static_cast<int>(min<int64_t>(len - offset, numeric_limits<int>::max()));
    int out_len;
    success = encrypt ?
        EVP_EncryptUpdate(&ctx, out + offset, &out_len, data + offset, in_len) :
        EVP_DecryptUpdate(&ctx, out + offset, &out_len, data + offset, in_len);
    if (success != 1) {
      return OpenSSLErr(encrypt ? "EVP_EncryptUpdate" : "EVP_DecryptUpdate");
    }
    // This is safe because we're using CFB mode without padding.
    DCHECK_EQ(in_len, out_len);
    offset += in_len;
  }

  // Finalize encryption or decryption.
  int final_out_len;
  success = encrypt ? EVP_EncryptFinal_ex(&ctx, out + offset, &final_out_len) :
                      EVP_DecryptFinal_ex(&ctx, out + offset, &final_out_len);
  if (success != 1) {
    return OpenSSLErr(encrypt ? "EVP_EncryptFinal" : "EVP_DecryptFinal");
  }
  // Again safe due to CFB with no padding
  DCHECK_EQ(final_out_len, 0);
  return Status::OK();
}
}
