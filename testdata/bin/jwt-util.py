# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Generates a new RSA 2048 public/private key pair and uses that key pair to sign
# two new JWTs, one that is expired and one that is not expired. The public key is
# written to a file in JWKS format, and the two JWTS are also written to files.
#
# Also generates a valid, non-expired JWT using another generated JWK. This JWT can be
# used to test that JWT authentication only accepts JWTs signed by the JWK it trusts.
#
# The generates JWKS/JWTs are used by the 'tests/custom_cluster/test_shell_jwt_auth.py'
# Python custom cluster tests. Since the generated JWTs are valid for 10 years, they
# should not need to be regenerated.

from __future__ import absolute_import, division, print_function

import json
import os
import sys

from datetime import datetime
from jwcrypto import jwk, jwt
from time import time

# ensure the first parameter was provided and is a valid directory
work_dir = ""
if len(sys.argv) != 2:
  print("[ERROR] missing first parameter to this script which must be a valid directory")
  sys.exit(1)

if not os.path.isdir(sys.argv[1]):
  print("[ERROR] first and only parameter to this script must be a valid directory")
  sys.exit(1)

work_dir = sys.argv[1]

#
# Generate a signing JWK and two JWTs that will be signed by that JWK
#

# generate a key id using the current date-time to enable easy tracking of the keys
key_id = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

# generate a new public/private keypair that can be used to sign JWTs
key = jwk.JWK.generate(kty="RSA", size=2048, alg="RS256", use="sig", kid=key_id)

# build a key set from the generated key
keyset = jwk.JWKSet()
keyset.add(key)
jwks_json_obj = json.loads(keyset.export(private_keys=False, as_dict=False))

# create and sign a JWT that expires in 10 years
token_valid = jwt.JWT(
  header={
    "alg": "RS256",
    "kid": key.get("kid"),
    "type": "JWT"
  },
  claims={
    "sub": "test-user",
    "kid": key.get("kid"),
    "iss": "file://tests/util/jwt/jwt_util.py",
    "aud": "impala-tests",
    "iat": int(time()),
    "exp": int(time()) + 315360000
  }
)
token_valid.make_signed_token(key)

# create and sign a JWT that expired in the past
token_expired = jwt.JWT(
  header={
    "alg": "RS256",
    "kid": key.get("kid"),
    "type": "JWT"
  },
  claims={
    "sub": "test-user",
    "kid": key.get("kid"),
    "iss": "file://tests/util/jwt/jwt_util.py",
    "aud": "impala-tests",
    "iat": int(time()) - 7200,
    "exp": int(time()) - 3600
  }
)
token_expired.make_signed_token(key)

# write out the jwks
with open(os.path.join(work_dir, "jwks_signing.json"), "w") as jwks_file:
  jwks_file.write(json.dumps(jwks_json_obj, indent=2))

# write out the signed valid jwt
with open(os.path.join(work_dir, "jwt_signed"), "w") as jwt_file:
  jwt_file.write(token_valid.serialize())

# write out the signed expired jwt
with open(os.path.join(work_dir, "jwt_expired"), "w") as jwt_file:
  jwt_file.write(token_expired.serialize())

#
# Generate another valid signed JWT using a different JWK
#

# generate a key id using the current date-time to enable easy tracking of the keys
key_id_untrusted_jwk = "untrusted_jwk-{0}" \
  .format(datetime.utcnow().strftime("%Y%m%d-%H%M%S"))

# generate a new public/private keypair that can be used to sign JWTs
untrusted_jwk = jwk.JWK.generate(kty="RSA", size=2048, alg="RS256", use="sig",
                                   kid=key_id_untrusted_jwk)

# create and sign a JWT that expires in 10 years
token_untrusted = jwt.JWT(
  header={
    "alg": "RS256",
    "kid": untrusted_jwk.get("kid"),
    "type": "JWT"
  },
  claims={
    "sub": "test-user",
    "kid": untrusted_jwk.get("kid"),
    "iss": "file://tests/util/jwt/jwt_util.py",
    "aud": "impala-tests",
    "iat": int(time()),
    "exp": int(time()) + 315360000
  }
)
token_untrusted.make_signed_token(untrusted_jwk)

# write out the signed jwt
with open(os.path.join(work_dir, "jwt_signed_untrusted"), "w") as jwt_untrusted_jwk_file:
  jwt_untrusted_jwk_file.write(token_untrusted.serialize())
