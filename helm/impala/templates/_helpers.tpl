{{/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/}}

{{- define "impala.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "impala.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if eq .Release.Name $name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "impala.labels" -}}
app.kubernetes.io/name: {{ include "impala.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}

{{- define "impala.image" -}}
{{- $prefix := .Values.image.prefix | default "" -}}
{{- printf "%s%s" $prefix .imageName -}}
{{- end -}}

{{- define "impala.kuduMasterService" -}}
{{- printf "%s-kudu-master" (include "impala.fullname" .) -}}
{{- end -}}

{{- define "impala.kuduTserverService" -}}
{{- printf "%s-kudu-tserver" (include "impala.fullname" .) -}}
{{- end -}}

{{- define "impala.kuduMasterPvc" -}}
{{- printf "%s-kudu-master-data" (include "impala.fullname" .) -}}
{{- end -}}

{{- define "impala.kuduTserverPvc" -}}
{{- printf "%s-kudu-tserver-data" (include "impala.fullname" .) -}}
{{- end -}}
