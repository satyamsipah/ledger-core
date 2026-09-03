{{/*
Chart name, capped at 63 characters the way every Kubernetes object name is
-- Helm's own starter chart truncates for the identical reason and this
chart follows the same convention rather than inventing another one.
*/}}
{{- define "ledger-core.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Release-scoped full name, so two releases of this chart in one namespace
(a blue/green pair, or a staging and a canary) never collide on a resource
name.
*/}}
{{- define "ledger-core.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
The standard recommended Kubernetes labels, applied to every resource this
chart renders -- what lets `kubectl get all -l app.kubernetes.io/instance=
<release>` actually mean something.
*/}}
{{- define "ledger-core.labels" -}}
helm.sh/chart: {{ printf "%s-%s" (include "ledger-core.name" .) .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "ledger-core.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels only -- the subset a Service/Deployment selector uses, kept
separate from the full label set above because a selector must never change
across an upgrade (app.kubernetes.io/version and helm.sh/chart both do).
*/}}
{{- define "ledger-core.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ledger-core.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Per-service selector labels: the chart-wide selector above, plus which of
the five services this resource belongs to. Every Deployment/Service/HPA/
PDB template's range loop calls this with (dict "root" $ "svcName"
$svcName) -- "root" because that wrapping dict is not itself a valid
selectorLabels context (it has no .Release, .Chart of its own), so this
re-dereferences into .root before delegating to selectorLabels above.
*/}}
{{- define "ledger-core.serviceSelectorLabels" -}}
{{ include "ledger-core.selectorLabels" .root }}
app.kubernetes.io/component: {{ .svcName }}
{{- end -}}

{{/*
The image reference for one service: <registry>-<service>:<tag>, matching
deploy/Dockerfile.prod's own naming (built once per SERVICE build arg,
pushed as one image per service) and docker-compose.prod.yml's identical
${REGISTRY}-<service>:${TAG} pattern, so all three deployment paths name
an image the same way.
*/}}
{{- define "ledger-core.image" -}}
{{- printf "%s-%s:%s" .root.Values.image.registry .svcName (.root.Values.image.tag | required "image.tag is required -- the release tag CI built and pushed") -}}
{{- end -}}

{{/*
ServiceAccount name: the one this chart creates, or an operator-supplied
existing one.
*/}}
{{- define "ledger-core.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "ledger-core.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
