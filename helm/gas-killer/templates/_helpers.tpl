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
Whether the bundled Anvil runs as a simulation fork beside an external chain RPC.

Distinct from LOCAL mode, where the same workload is the chain itself rather than a fork of one.
Rendered as a string so callers can test it: `include "gas-killer.simFork.enabled" . | eq "true"`.
*/}}
{{- define "gas-killer.simFork.enabled" -}}
{{- if and .Values.l1.enabled .Values.secrets.forkUrl (.Values.l1.simFork).enabled (ne .Values.global.environment "LOCAL") -}}
true
{{- else -}}
false
{{- end -}}
{{- end }}

{{/*
Simulation profile (GK_SIM_PROFILE) shared by the router and every node. Both deployments render
this one helper, so they cannot be given different values — a divergence would change the derived
storage_updates on one side and fork the quorum's digests.

Rejects unbounded whenever the deployment's own Anvil serves debug_traceCall without a lifted
execution cap. The profile lifts the gas limits a tracked function is SIMULATED under, but an
above-block-limit call still OOGs inside a clamped tracer — and extraction returns a truncated
result rather than an error, so every heavy task would be silently wrong at runtime instead of
loudly wrong at install time. Satisfied by l1.extraArgs carrying --disable-block-gas-limit, or by
global.localAnvilUnboundedReady for an image that bakes the flag into its entrypoint.

Says nothing about TESTNET without a sim fork: there the cap belongs to an endpoint the chart
cannot see, which is what l1.simFork.enabled exists to bring in-cluster.
*/}}
{{- define "gas-killer.simProfile" -}}
{{- $profile := .Values.global.simProfile | default "chain" -}}
{{- if not (has $profile (list "chain" "unbounded")) -}}
{{- fail (printf "global.simProfile must be \"chain\" or \"unbounded\", got %q" $profile) -}}
{{- end -}}
{{- $bundledAnvil := or (eq .Values.global.environment "LOCAL") (include "gas-killer.simFork.enabled" . | eq "true") -}}
{{- $capLifted := or (contains "--disable-block-gas-limit" (.Values.l1.extraArgs | default "")) .Values.global.localAnvilUnboundedReady -}}
{{- if and (eq $profile "unbounded") $bundledAnvil (not $capLifted) -}}
{{- fail "global.simProfile=unbounded needs the bundled Anvil to run with --disable-block-gas-limit, or an above-block-limit call OOGs inside the tracer and analysis returns a truncated payload instead of an error. Set l1.extraArgs=\"--disable-block-gas-limit\", or global.localAnvilUnboundedReady=true if the ethereum image already supplies it." -}}
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
What Schnorr scaffolding to provision while the fleet signs another scheme, from
schnorr.provision: "" (nothing), "registry" (deploy the registry and publish its address), or
"full" (also register the operator set).

Ignored under signatureScheme=schnorr, where the operator-set job always runs and always
registers: a schnorr fleet certifies nothing against an empty registry, so there is no useful
half-provisioned state to select.

Rejects an unrecognized value here rather than letting it reach the pods, matching
gas-killer.signatureScheme: setup_schnorr_operators errors on anything else, so a typo would
otherwise install cleanly and then fail a job.
*/}}
{{- define "gas-killer.schnorrProvision" -}}
{{- $provision := .Values.schnorr.provision | default "" | trim | lower -}}
{{- if not (has $provision (list "" "registry" "full")) -}}
{{- fail (printf "schnorr.provision must be \"\", \"registry\" or \"full\", got %q" .Values.schnorr.provision) -}}
{{- end -}}
{{- $provision -}}
{{- end }}

{{/*
Whether this deployment provisions a SchnorrStakeRegistry at all, which is what gates the
operator-set job and the handoff of its address to the router. Emits "true" or "".
*/}}
{{- define "gas-killer.provisionsSchnorrRegistry" -}}
{{- if or (include "gas-killer.isSchnorr" .) (include "gas-killer.schnorrProvision" .) -}}true{{- end }}
{{- end }}

{{/*
Whether the operator set is registered against that registry. Emits "true" or "".

Separate from provisioning because registering needs every operator's secp256k1 key on the shared
volume, and a deployment whose keys are gone can still deploy and publish a registry for
integrators to wire against, filling it later. The registry verifies nothing until it is filled,
which only matters once a fleet signs schnorr.
*/}}
{{- define "gas-killer.registersSchnorrOperators" -}}
{{- if or (include "gas-killer.isSchnorr" .) (eq (include "gas-killer.schnorrProvision" .) "full") -}}true{{- end }}
{{- end }}

{{/*
The SCHNORR_PROVISION value the operator-set job runs with: "full" where the operator set is
registered, "registry" where only the registry is deployed.

Derived from the chart's own two decisions rather than passed through from schnorr.provision, so
the binary and the templates cannot disagree. Under signatureScheme=schnorr that means "full"
whatever schnorr.provision says, which is what the templates already gate on. Only meaningful
where gas-killer.provisionsSchnorrRegistry holds.
*/}}
{{- define "gas-killer.schnorrProvisionMode" -}}
{{- if include "gas-killer.registersSchnorrOperators" . -}}full{{- else -}}registry{{- end }}
{{- end }}

{{/*
Path the operator-set job records its registry address at, on the shared volume. The router reads
it from there as a named record, so a job that finishes after the router is serving still lands.
*/}}
{{- define "gas-killer.schnorrRegistryRecord" -}}
/app/.nodes/schnorr_stake_registry.txt
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
