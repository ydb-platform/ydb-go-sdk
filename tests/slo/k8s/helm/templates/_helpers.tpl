{{/*
Expand the name of the chart.
*/}}
{{- define "ydb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "ydb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "ydb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ydb.labels" -}}
helm.sh/chart: {{ include "ydb.chart" . }}
{{ include "ydb.selectorLabels" . }}
{{- if or (.Chart.AppVersion) (.Values.image.tag) }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ydb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ydb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}


{{/*
Create webhooks pathPrefix used by service fqdn url
*/}}
{{- define "ydb.webhookPathPrefix" -}}
{{- if .Values.webhook.service.enableDefaultPathPrefix -}}
{{- printf "/%s/%s" .Release.Namespace ( include "ydb.fullname" . ) -}}
{{- end }}
{{- if .Values.webhook.service.customPathPrefix -}}
{{- .Values.webhook.service.customPathPrefix -}}
{{- end }}
{{- end -}}