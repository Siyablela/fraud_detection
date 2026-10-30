{{/* Chart name, truncated to the 63-character DNS label limit. */}}
{{- define "fraud-detection.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "fraud-detection.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Labels shared by every object; pass a dict with "root" and "component". */}}
{{- define "fraud-detection.labels" -}}
helm.sh/chart: {{ include "fraud-detection.chart" .root }}
app.kubernetes.io/name: {{ include "fraud-detection.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/component: {{ .component }}
app: {{ .component }}
{{- end }}

{{/* The one application image; the command selects api, worker, batch job or migrations. */}}
{{- define "fraud-detection.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.digest -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.image.repository $tag }}
{{- end -}}
{{- end }}

{{/* Pod-level hardening for the application image (UID 10001 is created in the Dockerfile). */}}
{{- define "fraud-detection.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end }}

{{- define "fraud-detection.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end }}

{{/* Environment shared by every application process. Secrets come only from the existing Secret. */}}
{{- define "fraud-detection.appEnv" -}}
- name: DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecretName }}
      key: database-url
- name: REDIS_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecretName }}
      key: redis-url
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ .Values.config.kafkaBootstrapServers | quote }}
- name: KAFKA_TOPIC_NAME
  value: {{ .Values.config.kafkaTopicName | quote }}
- name: KAFKA_CONSUMER_GROUP_ID
  value: {{ .Values.config.kafkaConsumerGroupId | quote }}
- name: KAFKA_SECURITY_PROTOCOL
  value: {{ .Values.config.kafkaSecurityProtocol | quote }}
{{- if hasPrefix "SASL_" .Values.config.kafkaSecurityProtocol }}
- name: KAFKA_OAUTH_TOKEN_URL
  value: {{ required "config.kafkaOauthTokenUrl is required for SASL" .Values.config.kafkaOauthTokenUrl | quote }}
- name: KAFKA_OAUTH_CLIENT_ID
  value: {{ required "config.kafkaOauthClientId is required for SASL" .Values.config.kafkaOauthClientId | quote }}
- name: KAFKA_OAUTH_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecretName }}
      key: kafka-oauth-client-secret
{{- end }}
- name: JWT_ISSUER
  value: {{ required "config.jwtIssuer is required" .Values.config.jwtIssuer | quote }}
- name: JWT_AUDIENCE
  value: {{ .Values.config.jwtAudience | quote }}
- name: JWT_JWKS_URL
  value: {{ required "config.jwtJwksUrl is required" .Values.config.jwtJwksUrl | quote }}
- name: RULE_SET_KEY
  value: {{ .Values.config.ruleSetKey | quote }}
- name: CORS_ALLOWED_ORIGINS
  value: {{ .Values.config.corsAllowedOrigins | quote }}
- name: RATE_LIMIT_REQUESTS
  value: {{ .Values.config.rateLimitRequests | quote }}
- name: OBSERVABILITY_LOG_LEVEL
  value: {{ .Values.config.logLevel | quote }}
- name: OBSERVABILITY_ENABLE_TRACING
  value: {{ .Values.config.enableTracing | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.config.otlpEndpoint | quote }}
{{- end }}
