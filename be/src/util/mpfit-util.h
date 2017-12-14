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


#ifndef IMPALA_MPFIT_UTIL_H
#define IMPALA_MPFIT_UTIL_H

#include <functional>
#include <memory>
#include <string>

#include "common/logging.h"
#include "common/status.h"
#include "thirdparty/mpfit/mpfit.h"

namespace impala {

/// Objective function to be fit using the MpFit library. An objective function has
/// exactly one 'x' variable and any number of additional non-variable parameters whose
/// values are unknown. The purpose of curve fitting is to find the best values for those
/// parameters given an objective function and a series of x/y data points.
/// This class contains the objective function as well as the data points because that's
/// the most natural setup for the MpFit API.
/// Calling LmsFit() determines the parameters for the objective function.
/// Calling GetY() computes the 'y' value for a given 'x' using the function parameters.
/// The objective function is of type function<double (double, const double*)>
/// By convention, the first argument is the value of the 'x' variable and the second
/// argument is an array of function parameters which are determined during fitting.
class ObjectiveFunction {
 public:
  ObjectiveFunction(std::string name, int num_params,
      std::function<double (double, const double*)> fn);

  /// Performs least mean squares (LMS) curve fitting using the MpFit library
  /// against the provided x/y data points.
  /// Returns true if fitting was successful, false otherwise.
  bool LmsFit(const double* xs, const double* ys, int num_points) WARN_UNUSED_RESULT;

  /// Evaluates the objective function over the given 'x' value.
  double GetY(int64_t x) const {
    DCHECK(params_ != nullptr);
    return fn_(x, params_.get());
  }

  /// Returns the difference between the y value of data point 'pidx' and the
  /// y value of the objective function with the given parameters over the x value
  /// of the same point.
  double GetDeltaY(int pidx, const double* params) const {
    DCHECK_LT(pidx, num_points_);
    return ys_[pidx] - fn_(xs_[pidx], params);
  }

  /// Returns the Chi-Square of fitting. This is an indication of how well the function
  /// fits. Lower is better. Valid to call after LmsFit().
  double GetError() const {
    DCHECK(params_ != nullptr);
    return result_.bestnorm;
  }

 private:
  /// Human-readable name of this function. Used for debugging.
  std::string name_;

  /// Function parameters to be determined by fitting.
  const int num_params_;
  std::unique_ptr<double[]> params_;

  /// MPFit result structure. Populated by in LmsFit(). All pointers in this structure
  /// are optional and must be allocated and owned by the caller of mpfit(). Passing
  /// nullptr indicates to MPFit that those fields should not be populated.
  mp_result result_;

  /// Objective function whose parameters should be fit to the data points.
  std::function<double (double, const double*)> fn_;

  /// Known x/y data points. Memory not owned.
  int num_points_;
  const double* xs_;
  const double* ys_;
};

}

#endif
