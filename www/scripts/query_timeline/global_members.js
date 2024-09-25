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

// Stroke width of fragment diagrams borders
export const BORDER_STROKE_WIDTH = 2;
// Default margin provided for fragment diagram's header and footer
export const MARGIN_HEADER_FOOTER = 5;
// Default margin provided on the right(at the end) of all charts
export const MARGIN_CHART_END = 60;
// The minimum height of diagrams / charts
export const DIAGRAM_MIN_HEIGHT = 200;

export let diagram_width = window.innerWidth - BORDER_STROKE_WIDTH; // border width
export let profile = {};
export let maxts = 0;
export let decimals = 2;

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
