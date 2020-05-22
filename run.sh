#!/usr/bin/env sh

if [ "$JAVA_MODULE_SYSTEM_ENABLED" == "true" ]; then
  echo "Starting java using MODULE-SYSTEM"
  export JPMS_SWITCHES=""
  exec java $JPMS_SWITCHES -p /app/lib -m no.ssb.dapla.exploration_concept_ingest/no.ssb.dapla.exploration_concept_ingest.ExplorationConceptIngestApplication
else
  echo "Starting java using CLASSPATH"
  exec java -cp "/app/lib/*" no.ssb.dapla.exploration_concept_ingest.ExplorationConceptIngestApplication
fi
