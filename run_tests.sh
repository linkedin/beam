#!/usr/bin/env bash

./gradlew :runners:samza:job-server:validatesPortableRunnerEmbedded || true

echo "Show Test Result"
cat ./runners/samza/job-server/build/reports/tests/validatesPortableRunnerEmbedded/index.html

echo "SimplePushbackSideInputDoFnRunnerTest"
cat ./runners/samza/job-server/build/reports/tests/validatesPortableRunnerEmbedded/classes/org.apache.beam.runners.core.SimplePushbackSideInputDoFnRunnerTest.html

echo "WindowingTest"
cat ./runners/samza/job-server/build/reports/tests/validatesPortableRunnerEmbedded/classes/org.apache.beam.sdk.transforms.windowing.WindowingTest.html

echo "PCollectionRowTupleTest"
cat ./runners/samza/job-server/build/reports/tests/validatesPortableRunnerEmbedded/classes/org.apache.beam.sdk.values.PCollectionRowTupleTest.html

echo "PCollectionTupleTest"
cat ./runners/samza/job-server/build/reports/tests/validatesPortableRunnerEmbedded/classes/org.apache.beam.sdk.values.PCollectionTupleTest.html
