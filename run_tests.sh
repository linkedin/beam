#!/usr/bin/env bash

./gradlew :runners:samza:job-server:validatesPortableRunnerEmbedded || true

echo "Show Test Result"
cat ./runners/samza/job-server/build/reports/tests/validatesPortableRunnerEmbedded/index.html
