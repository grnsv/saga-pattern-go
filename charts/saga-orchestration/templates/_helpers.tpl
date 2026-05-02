{{/*
Release prefix used for resource names.
Helm release names can be 53 chars, but Kubernetes DNS labels are limited to
63 chars and this chart appends component suffixes such as -saga-orchestrator.
*/}}
{{- define "saga-orchestration.fullname" -}}
{{- .Release.Name | trunc 45 | trimSuffix "-" }}
{{- end }}

{{/*
initContainer that waits until PostgreSQL accepts connections.
*/}}
{{- define "saga-orchestration.postgresWaitContainer" -}}
- name: wait-for-postgres
  image: "{{ .Values.postgres.image }}:{{ .Values.postgres.tag }}"
  env:
    - name: PGPASSWORD
      value: {{ .Values.postgres.password | quote }}
  command:
    - sh
    - -c
    - |
      echo "Waiting for PostgreSQL..."
      until pg_isready -h {{ include "saga-orchestration.fullname" . }}-postgres \
            -U {{ .Values.postgres.user | quote }} \
            -d {{ .Values.postgres.database | quote }}; do
        sleep 2
      done
      echo "PostgreSQL ready."
{{- end }}

{{/*
Common labels attached to every resource.
*/}}
{{- define "saga-orchestration.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Kafka broker address: use the override value or derive from the Kafka service name.
*/}}
{{- define "saga-orchestration.kafkaBrokers" -}}
{{- if .Values.global.kafkaBrokers -}}
{{- .Values.global.kafkaBrokers -}}
{{- else -}}
{{- printf "%s-kafka:9092" (include "saga-orchestration.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
PostgreSQL connection URL for the orchestrator.
*/}}
{{- define "saga-orchestration.databaseURL" -}}
{{- printf "postgres://%s:%s@%s-postgres:5432/%s?sslmode=disable" .Values.postgres.user .Values.postgres.password (include "saga-orchestration.fullname" .) .Values.postgres.database -}}
{{- end }}

{{/*
initContainer that waits until all orchestration topics exist.
Topic creation is handled by the kafka-init Job.
*/}}
{{- define "saga-orchestration.kafkaWaitTopicsContainer" -}}
- name: wait-for-topics
  image: "{{ .Values.kafka.image }}:{{ .Values.kafka.tag }}"
  command:
    - bash
    - -c
    - |
      BROKER={{ include "saga-orchestration.kafkaBrokers" . }}
      for topic in saga-commands payment-commands payment-events inventory-commands inventory-events saga-events; do
        echo "Waiting for topic $topic..."
        until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" \
              --describe --topic "$topic" > /dev/null 2>&1; do
          sleep 2
        done
        echo "Topic $topic ready."
      done
{{- end }}
