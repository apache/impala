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

import {renderTimingDiagram, setTimingDiagramDimensions} from "./fragment_diagram.js";
import {resizeFragmentMetricsChart} from "./fragment_metrics_diagram.js";
import {resizeUtilizationChart} from "./host_utilization_diagram.js";

export var profile = {};
export var maxts = 0;
export var decimals = 2;
export var border_stroke_width = 2;
export var diagram_width = window.innerWidth - border_stroke_width; // border width
export var margin_header_footer = 5;
export var margin_chart_end = 60;
export var diagram_min_height = 200;
export var diagram_controls_height = Math.max(28, window.innerHeight * 0.025);

export function set_profile(val) {
  profile = val;
}

export function set_decimals(val) {
  decimals = val;
}

export function set_maxts(val) {
  maxts = val;
}

export function set_diagram_width(val) {
  diagram_width = val;
}

export function clearDOMChildren(element) {
  while (element.firstChild) {
    element.removeChild(element.firstChild);
  }
}

export function resizeHorizontalAll() {
  renderTimingDiagram();
  resizeUtilizationChart();
  resizeFragmentMetricsChart();
}

export function resizeVerticalAll() {
  setTimingDiagramDimensions();
  resizeUtilizationChart();
  resizeFragmentMetricsChart();
}
