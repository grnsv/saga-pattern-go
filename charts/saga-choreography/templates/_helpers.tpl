{{/*
Full release name — used as prefix for all resources.
*/}}
{{- define "saga-choreography.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels attached to every resource.
*/}}
{{- define "saga-choreography.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Kafka broker address: use the override value or derive from the Kafka service name.
*/}}
{{- define "saga-choreography.kafkaBrokers" -}}
{{- if .Values.global.kafkaBrokers -}}
{{- .Values.global.kafkaBrokers -}}
{{- else -}}
{{- printf "%s-kafka:9092" (include "saga-choreography.fullname" .) -}}
{{- end -}}
{{- end }}

{{/*
initContainer that waits until all three saga topics exist.
Topic creation is handled by the kafka-init Job (helm post-install/post-upgrade hook).
*/}}
{{- define "saga-choreography.kafkaWaitTopicsContainer" -}}
- name: wait-for-topics
  image: "{{ .Values.kafka.image }}:{{ .Values.kafka.tag }}"
  command:
    - bash
    - -c
    - |
      BROKER={{ include "saga-choreography.kafkaBrokers" . }}
      for topic in order-events payment-events inventory-events; do
        echo "Waiting for topic $topic..."
        until /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" \
              --describe --topic "$topic" > /dev/null 2>&1; do
          sleep 2
        done
        echo "Topic $topic ready."
      done
{{- end }}
