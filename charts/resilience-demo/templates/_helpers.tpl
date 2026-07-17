{{/*
Expand the name of the chart.
*/}}
{{- define "resilience-demo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a fully-qualified app name (truncated to 63 chars for DNS compliance).
*/}}
{{- define "resilience-demo.fullname" -}}
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
Chart name and version label value.
*/}}
{{- define "resilience-demo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "resilience-demo.labels" -}}
helm.sh/chart: {{ include "resilience-demo.chart" . }}
{{ include "resilience-demo.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels. Includes the legacy `app` label the base manifests used so
existing Services/PDBs keep matching.
*/}}
{{- define "resilience-demo.selectorLabels" -}}
app.kubernetes.io/name: {{ include "resilience-demo.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: {{ include "resilience-demo.name" . }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "resilience-demo.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "resilience-demo.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Resolved image reference (repository:tag, tag defaults to appVersion).
*/}}
{{- define "resilience-demo.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{/*
Name of the ConfigMap holding application config.
*/}}
{{- define "resilience-demo.configMapName" -}}
{{- printf "%s-config" (include "resilience-demo.fullname" .) }}
{{- end }}

{{/*
Name of the Secret holding the API key.
*/}}
{{- define "resilience-demo.secretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-auth" (include "resilience-demo.fullname" .) -}}
{{- end -}}
{{- end }}
