{{/*
Expand the name of the chart.
*/}}
{{- define "gas-killer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "gas-killer.fullname" -}}
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
{{- define "gas-killer.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "gas-killer.labels" -}}
helm.sh/chart: {{ include "gas-killer.chart" . }}
{{ include "gas-killer.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "gas-killer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gas-killer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Simulation profile (GK_SIM_PROFILE) shared by the router and every node. Both deployments render
this one helper, so they cannot be given different values — a divergence would change the derived
storage_updates on one side and fork the quorum's digests.

Rejects the LOCAL + unbounded combination unless it is explicitly acknowledged. The profile
lifts the gas limits a tracked function is SIMULATED under, but executing an above-block-limit
call also needs the node serving debug_traceCall to have its own execution cap lifted. In LOCAL
mode that node is the bundled Anvil, whose flags come from the ethereum image's entrypoint rather
than this chart, so the chart cannot make the deployment work on its own — without the flag every
heavy task fails analysis at runtime instead of at install time.
*/}}
{{- define "gas-killer.simProfile" -}}
{{- $profile := .Values.global.simProfile | default "chain" -}}
{{- if not (has $profile (list "chain" "unbounded")) -}}
{{- fail (printf "global.simProfile must be \"chain\" or \"unbounded\", got %q" $profile) -}}
{{- end -}}
{{- if and (eq $profile "unbounded") (eq .Values.global.environment "LOCAL") (not .Values.global.localAnvilUnboundedReady) -}}
{{- fail "global.simProfile=unbounded in LOCAL mode requires the bundled Anvil to run with --disable-block-gas-limit, which comes from the ethereum image's entrypoint and not this chart. Set global.localAnvilUnboundedReady=true to confirm the image provides it." -}}
{{- end -}}
{{- $profile -}}
{{- end }}

{{/*
L1 service name
*/}}
{{- define "gas-killer.l1.fullname" -}}
{{- printf "%s-l1" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Signer service name
*/}}
{{- define "gas-killer.signer.fullname" -}}
{{- printf "%s-signer" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Router service name
*/}}
{{- define "gas-killer.router.fullname" -}}
{{- printf "%s-router" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Node name helper
*/}}
{{- define "gas-killer.node.fullname" -}}
{{- printf "%s-node" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Setup job name
*/}}
{{- define "gas-killer.setup.fullname" -}}
{{- printf "%s-setup" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Shared data PVC name
*/}}
{{- define "gas-killer.shareddata.fullname" -}}
{{- printf "%s-shared-data" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Router persistent data PVC name
*/}}
{{- define "gas-killer.routerdata.fullname" -}}
{{- printf "%s-router-data" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Config ConfigMap name
*/}}
{{- define "gas-killer.config.fullname" -}}
{{- printf "%s-config" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Secret name - supports existing secret or creates new one
*/}}
{{- define "gas-killer.secret.fullname" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- printf "%s-secret" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Kubernetes ServiceAccount name (for Workload Identity with GCP Secret Manager)
*/}}
{{- define "gas-killer.serviceaccount.fullname" -}}
{{- printf "%s-sa" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Key export job name
*/}}
{{- define "gas-killer.keyexport.fullname" -}}
{{- printf "%s-key-export" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Bridge job name
*/}}
{{- define "gas-killer.bridge.fullname" -}}
{{- printf "%s-bridge" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Deploy-target job name
*/}}
{{- define "gas-killer.deployTarget.fullname" -}}
{{- printf "%s-deploy-target" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Playground job name
*/}}
{{- define "gas-killer.playground.fullname" -}}
{{- printf "%s-playground" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
L2 service name
*/}}
{{- define "gas-killer.l2.fullname" -}}
{{- printf "%s-l2" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Yield distribution job name
*/}}
{{- define "gas-killer.yield-distribution.fullname" -}}
{{- printf "%s-yield-distribution" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Generate router key job name
*/}}
{{- define "gas-killer.generate-router-key.fullname" -}}
{{- printf "%s-generate-router-key" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Schnorr operator-set job name
*/}}
{{- define "gas-killer.schnorr-operators.fullname" -}}
{{- printf "%s-schnorr-operators" (include "gas-killer.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Whether the deployment runs the aggregate-Schnorr quorum scheme rather than BLS. Every guard
that gates schnorr-only chart behaviour reads this, so the default and the spelling live in one
place. Emits the string "true" or "", so it reads as `if (include "gas-killer.isSchnorr" .)`.

Rejects an unrecognized scheme here rather than letting it reach the pods: `signature_scheme()`
panics on anything but "bls" or "schnorr", so a typo would otherwise install cleanly and then
crash-loop the whole fleet.
*/}}
{{- define "gas-killer.isSchnorr" -}}
{{- if eq (include "gas-killer.signatureScheme" .) "schnorr" -}}true{{- end }}
{{- end }}

{{/*
The quorum signature scheme (SIGNATURE_SCHEME) shared by the router and every node. Both
deployments render this one helper, so they cannot be given different values. A mixed fleet
signs with two incompatible schemes and certifies nothing.

Rejects an unrecognized scheme here rather than letting it reach the pods: `signature_scheme()`
panics on anything but "bls" or "schnorr", so a typo would otherwise install cleanly and then
crash-loop the whole fleet.

Trimmed and lowercased to match how the binaries parse it, and normalized before it is emitted.
The chart gates whole jobs and key paths on this value, so a spelling the binaries accept but
the templates did not would hand a schnorr fleet a bls-shaped deployment.
*/}}
{{- define "gas-killer.signatureScheme" -}}
{{- $scheme := .Values.global.signatureScheme | default "bls" | trim | lower -}}
{{- if eq $scheme "" -}}{{- $scheme = "bls" -}}{{- end -}}
{{- if not (has $scheme (list "bls" "schnorr")) -}}
{{- fail (printf "global.signatureScheme must be \"bls\" or \"schnorr\", got %q" .Values.global.signatureScheme) -}}
{{- end -}}
{{- $scheme -}}
{{- end }}
