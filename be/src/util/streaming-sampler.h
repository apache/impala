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

#include <string.h>

#include "util/ubsan.h"

namespace impala {

/// A fixed-size sampler to collect samples over time. AddSample should be
/// called periodically with the sampled value. Samples are added at the max
/// resolution possible.  When the sample buffer is full, the current samples
/// are collapsed and the collection period is doubled.
/// The input period and the streaming sampler period do not need to match, the
/// streaming sampler will average values.
/// T is the type of the sample and must be a native numerical type which fulfills
/// std::is_arithmetic (e.g. int or float).
///
/// This class is not thread-safe.
template<typename T, int MAX_SAMPLES>
class StreamingSampler {
  static_assert(std::is_arithmetic<T>::value, "Numerical type required");
 public:
  StreamingSampler(int initial_period)
    : samples_collected_(0) ,
      period_(initial_period),
      initial_period_(initial_period),
      current_sample_sum_(0),
      current_sample_count_(0),
      current_sample_total_time_(0) {
  }

  /// Resets everything back to the state when this instance was just created.
  void Reset() {
    samples_collected_ = 0;
    current_sample_sum_ = 0;
    current_sample_count_ = 0;
    current_sample_total_time_ = 0;
    // Reset 'period_' to the value got from the constructor.
    period_ = initial_period_;
  }

  /// Add a sample to the sampler. 'ms' is the time elapsed since the last time this
  /// was called.
  /// The input value is accumulated into current_*. If the total time elapsed
  /// in current_sample_total_time_ is higher than the storage period, the value is
  /// stored. 'sample' should be interpreted as a representative sample from
  /// (now - ms, now].
  /// TODO: we can make this more complex by taking a weighted average of samples
  /// accumulated in a period.
  void AddSample(T sample, int ms) {
    ++current_sample_count_;
    current_sample_sum_ += sample;
    current_sample_total_time_ += ms;

    if (current_sample_total_time_ >= period_) {
      samples_[samples_collected_++] = current_sample_sum_ / current_sample_count_;
      current_sample_count_ = 0;
      current_sample_sum_ = 0;
      current_sample_total_time_ = 0;

      if (samples_collected_ == MAX_SAMPLES) {
        /// collapse the samples in half by averaging them and doubling the storage period
        period_ *= 2;
        for (int i = 0; i < MAX_SAMPLES / 2; ++i) {
          samples_[i] = (samples_[i * 2] + samples_[i * 2 + 1]) / 2;
        }
        samples_collected_ /= 2;
      }
    }
  }

  /// Get the samples collected.  Returns the number of samples and the period they were
  /// collected at.
  const T* GetSamples(int* num_samples, int* period) const {
    *num_samples = samples_collected_;
    *period = period_;
    return samples_;
  }

 private:
  /// Aggregated samples collected. Note: this is not all the input samples from
  /// AddSample(), as logically, those samples get resampled and aggregated.
  T samples_[MAX_SAMPLES];

  /// Number of samples collected <= MAX_SAMPLES.
  int samples_collected_;

  /// Storage period in ms.
  int period_;
  /// Keeps the initial period got from the constructor. Used to reset 'period_'.
  int initial_period_;

  /// The sum of input samples that makes up the next stored sample.
  T current_sample_sum_;

  /// The number of input samples that contribute to current_sample_sum_.
  int current_sample_count_;

  /// The total time that current_sample_sum_ represents
  int current_sample_total_time_;
};
}
