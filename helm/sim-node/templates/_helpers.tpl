{{- define "sim-node.name" -}}
{{- .Release.Name -}}
{{- end }}
{{- define "sim-node.labels" -}}
app.kubernetes.io/name: sim-node
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
{{- define "sim-node.selectorLabels" -}}
app.kubernetes.io/name: sim-node
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
