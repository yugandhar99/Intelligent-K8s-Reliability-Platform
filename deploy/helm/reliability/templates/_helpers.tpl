{{- define "reliability.name" -}}
{{- $name := printf "%s-reliability" .Release.Name -}}
{{- if gt (len $name) 48 -}}
{{- printf "%s-%s" ($name | trunc 37 | trimSuffix "-") (.Release.Name | sha256sum | trunc 10) -}}
{{- else -}}
{{- $name -}}
{{- end -}}
{{- end -}}

{{- define "reliability.selector" -}}
app.kubernetes.io/name: reliability-platform
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/component: api-worker
{{- end -}}

{{- define "reliability.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else if .Values.image.requireDigest -}}
{{- fail "image.digest is required for this profile" -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "reliability.containerSecurity" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}
