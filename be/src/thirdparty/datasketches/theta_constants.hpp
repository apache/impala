/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef THETA_CONSTANTS_HPP_
#define THETA_CONSTANTS_HPP_

#include <climits>

namespace datasketches {

namespace theta_constants {
  enum resize_factor { X1, X2, X4, X8 };
  static const uint64_t MAX_THETA = LLONG_MAX; // signed max for compatibility with Java
  static const uint8_t MIN_LG_K = 5;
  static const uint8_t MAX_LG_K = 26;
}

} /* namespace datasketches */

#endif
