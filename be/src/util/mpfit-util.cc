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

#include "util/mpfit-util.h"

#include <cstring>

using namespace impala;
using std::string;
using std::function;

ObjectiveFunction::ObjectiveFunction(string name, int num_params,
    function<double (double, const double*)> fn)
  : name_(name), num_params_(num_params), fn_(fn), num_points_(0), xs_(nullptr),
    ys_(nullptr) {
  DCHECK_GT(num_params, 0);
}

/// User-supplied function called by MpFit during curve fitting. MpFit requires this
/// user-supplied function to compute "dy = y - fn(x)" for every known data point given
/// the objective function 'fn' and the current list of parameter values 'params'.
/// Declared here so it can be used in ObjectiveFunction below.
/// 'num_points' is the number of data points for fitting
/// 'num_params' is the number of parameters in the objective function
/// 'params' is an input array with 'num_params' parameter values
/// 'dy' is an output parameter allocated by MpFit. It contains the "delta y" for each
///      data point. This function sets dy[i] = y[i] - fn(x[i]) for each data point i and
///      for the objective function 'fn'.
/// 'obj_func' points to an ObjectiveFunction which contains the objective function
///            and the data points
/// The 'dvec' parameter is not important for our purposes. See the MpFit documentation.
/// MpFit allows these user-supplied functions to indicate errors by returning a non-zero
/// value. Our objective functions are simple and we always return 0.
static int ComputeDeltaY(int num_points, int num_params, double* params, double* dy,
    double** dvec, void* obj_func);

bool ObjectiveFunction::LmsFit(const double* xs, const double* ys, int num_points) {
  DCHECK(xs != nullptr);
  DCHECK(ys != nullptr);
  DCHECK_GE(num_points, 0);
  num_points_ = num_points;
  xs_ = xs;
  ys_ = ys;

  // Initialize function parameters.
  params_.reset(new (std::nothrow) double[num_params_]);
  if (params_ == nullptr) return false;
  for (int i = 0; i < num_params_; ++i) params_.get()[i] = 1.0;

  struct mp_config_struct config;
  memset(&config, 0, sizeof(config));
  // Maximum number of calls to SampledNdvMpFit(). Value determined empirically.
  config.maxfev = 1000;
  // Maximum number of fitting iterations. Value determined empirically.
  config.maxiter = 200;
  memset(&result_, 0, sizeof(result_));
  int ret = mpfit(ComputeDeltaY, num_points, num_params_, params_.get(), nullptr,
      &config, this, &result_);
  // Any positive integer indicates success.
  return ret > 0;
}

int ComputeDeltaY(int num_points, int num_params, double* params, double* dy,
    double** dvec, void* obj_func) {
  ObjectiveFunction* fn = reinterpret_cast<ObjectiveFunction*>(obj_func);
  for (int i = 0; i < num_points; ++i) dy[i] = fn->GetDeltaY(i, params);
  return 0;
}
