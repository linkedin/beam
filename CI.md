<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Apache Beam

## CI Environment

Continuous Integration is important component of making Apache Beam robust and stable.

Our execution environment for CI is the [GitHub Actions](https://github.com/features/actions).
See [.github/workflow/README](.github/workflow/README.md) for trigger phrase,
status and link of all GHA jobs.

GitHub Actions (GHA) are very well integrated with GitHub code and Workflow and
it has evolved fast in 2019/2020 to become a fully-fledged CI environment, easy
to use and develop for, so we decided to use it firstly for a few workflows then
migrated all workflows previously run on Jenkins.

For this reason there are mainly two types of GHA workflows running
- Self-hosted runner GHAs. These were migrated from Jenkins with workflow name
  prefix (beam_*.yml) as well as new workflows added following the same naming
  convention, including PreCommit, PostCommit, LoadTest, PerformanceTest, and
  several infrastructure jobs. See [.github/workflow/README](.github/workflow/README.md)
  for the complete list.
- GitHub-hosted runner GHAs. Most of these jobs exercises the workflow in different
  OS (linux, macOS, Windows). They were added prior to Jenkins migration.
  Some Linux jobs later migrated to use self-hosted runner.

### LinkedIn fork: Beam 2.75 Flink validation

The [LI Flink Runner Test](.github/workflows/li_runners_flink_tests.yml) workflow runs
on pull requests and pushes to `li-release-2.75`, and supports manual dispatch. It uses
GitHub-hosted Ubuntu runners, JDK 11, and Go 1.26.2, with separate Flink 1.20 and 2.2 jobs.
Each job runs the full runner build and `validatesRunner` suite, then uploads test reports
even if validation fails. No cloud credentials or package-publishing permissions are required.

To run both jobs' validation locally with JDK 11 and Go available:

```bash
./gradlew :runners:flink:1.20:build :runners:flink:1.20:validatesRunner \
  :runners:flink:2.2:build :runners:flink:2.2:validatesRunner \
  --continue --rerun-tasks --no-build-cache --no-daemon --max-workers=2
```

`validatesRunner` covers batch, DataStream batch, streaming, and checkpointing using
the repository's existing unsupported-category and sickbay exclusions. This Flink gate
does not replace repository-wide validation: the root build also includes cloud-backed
integration tests, which require a separately approved environment and resource scope.

### LinkedIn fork: Java artifact packaging

`gradle.properties` is the version source for the Java fork. Run
`./update_version.sh 2.75.2` to set it, or omit the argument to increment the patch
number. The script updates only `version` and `sdk_version`; it does not stage
files, create tags, or change Python/Go/TypeScript SDK releases. Python 3 is required.

| Setting | LinkedIn release | LinkedIn snapshot | Upstream mode |
|---|---|---|---|
| Flags | `-PisLinkedin -PisRelease` | `-PisLinkedin` (or add `-PisSnapshot` to a release build) | No `-PisLinkedin` flag |
| Maven group | `com.linkedin.beam` | `com.linkedin.beam` | `org.apache.beam` |
| Version at this revision | `2.75.2` | `2.75.2-SNAPSHOT` | `2.75.2` |
| Remote publication destinations | JFrog only with `-PlinkedinPublish` | JFrog only with `-PlinkedinPublish` | Existing Apache destinations |

For Java fork modules using `BeamModulePlugin`, LinkedIn mode remains local-only
unless `-PlinkedinPublish` explicitly configures the JFrog destination. That flag
does not upload anything by itself: only a remote publication task does so.
Native publishing mode reads `ARTIFACTORY_USER` and `ARTIFACTORY_KEY` directly,
without inheriting unrelated Maven settings credentials. Default local and
upstream authentication behavior is unchanged. Vendored Apache artifacts retain
their upstream coordinates and independent publishing configuration. Use explicit
module tasks, never the root `publish` selector. Snapshot BOMs are preserved.
The JFrog destination is only registered for Java artifact publishers managed by
`applyJavaNature`; custom BOM/vendor publishers are not added to this release path.

With JDK 11 and Go available, run the version-script and real POM-generation
regressions with:

```bash
python3 release/src/test/scripts/test_linkedin_build.py -v
```

The Flink 1.20 CI job also runs these checks. They generate metadata and SDK
version resources and publish to temporary local repositories, never remote ones. They cover
release/snapshot/disabled flags, dependency coordinates under
configuration-on-demand, and unchanged upstream publication configuration.
The checks use temporary Maven settings rather than CI or developer credentials.
A synthetic Maven home containing an unresolved credential placeholder verifies
that ambient settings are ignored and left untouched; the existing Gradle cache
is reused. Normal publication authentication is unchanged.

The protobuf publication checks also verify its shared Google Cloud BOM and
storage dependency, the published exclusion of Wire's unused compiler, and the
absence of compiler-only YAML libraries at runtime. The publisher runs the full
protobuf module checks before staging. These checks supplement, not replace,
ELR license and vulnerability validation of the published dependency graph.

To stage one artifact without touching your normal Maven cache:

```bash
./gradlew :sdks:java:core:publishMavenJavaPublicationToMavenLocal \
  -Ppublishing -PisLinkedin -PisRelease -PnoSigning \
  -Dmaven.repo.local=/absolute/path/to/isolated-repository \
  --no-daemon --no-build-cache
```

Individual-module publication does not publish sibling artifacts. To stage the
complete required Java bundle instead, use:

```bash
./gradlew :auto-elr:publishMavenJavaPublicationToMavenLocal \
  -Ppublishing -PisLinkedin -PisRelease -PnoSigning \
  -Dmaven.repo.local=/absolute/path/to/isolated-repository \
  --no-daemon --no-build-cache --max-workers=2
```

`com.linkedin.beam:beam-auto-elr` is a dependency aggregator, not an ELR network
client. Its 21 roots cover the required 30 Java artifacts; Parquet is explicit
because SQL declares it as provided, which is not inherited by Maven consumers.
The aggregator is for import/discovery, not application runtime use: it includes
multiple runner versions. Its publication tasks
schedule the complete closure to the same destination before publishing the
aggregator. A dependency publication failure blocks the aggregator even with
`--continue`. Already-published dependencies are not rolled back: investigate
partial remote releases before retrying. The module is not published under
Apache coordinates.

The regression suite tests actual release/snapshot staging, standalone consumer
resolution and failure-before-marker behavior. Its test fixture refuses network
publication tasks, and publisher configuration tests use dummy credentials.

### LinkedIn fork: controlled JFrog publication

The [LI JFrog publisher](.github/workflows/li_maven_publish.yml) is manual-only,
restricted to `linkedin/beam` on `li_trunk` or `li-release-*` branches. It defaults
to validation/local staging; `publish=true` explicitly enables the upload step.
Jobs for the same artifact version are serialized without cancellation. JFrog
secrets are passed only to that upload step.

The default branch still carries the legacy workflow. Select the intended
release branch explicitly when arranging an approved dry run, for example:

```bash
gh workflow run li_maven_publish.yml --repo linkedin/beam \
  --ref li-release-2.75 -f version=2.75.2 -f publish=false
```

The workflow first validates the requested version in a short preparation job.
Build/publication contracts and the full Java/runner matrix then run in parallel,
each with its own 120-minute budget. The staging/publishing job requires the
preparation and both validation jobs to succeed; failed, cancelled, or skipped
validation cannot reach publication.

Every job checks out the same workflow commit. Jobs have independent workspaces,
so the final job builds and stages the complete bundle locally rather than
assuming validation outputs are available. It has its own 120-minute budget and
uses native publishing mode to avoid ambient Maven settings, but invokes only
local tasks until upload is explicitly enabled.
Java test XML and HTML reports are retained in the
`linkedin-publisher-test-reports` artifact by the runner-validation job even when
validation fails. The workflow name, inputs, concurrency guard,
and JFrog credential isolation are unchanged; other workflows are not reorganized.
After an approved dry run, an approved live invocation can select `publish=true`.
Snapshot publication requires the matching `-SNAPSHOT` version input.

For an approved direct invocation from a clean release branch:

```bash
./gradlew :auto-elr:publishLinkedin \
  -Ppublishing -PisLinkedin -PisRelease -PlinkedinPublish -PnoSigning \
  -PlinkedinVersion=2.75.2 --no-daemon --no-build-cache --max-workers=2
```

The publication guard checks branch, CI event/repository, clean worktree,
checked-in version, SDK version, explicit version confirmation and credentials
before any JFrog task. Add `-PisSnapshot` and confirm the snapshot version for a
snapshot upload. Do not put credentials in command-line arguments.

Workflow dispatch permissions, actual JFrog authentication/uploads, Auto ELR
ingestion and downstream deployment are separate live validation gates. Local
staging does not certify them or production runtime/JRE compatibility.

## GitHub Actions

This section applies to GitHub-hosted runner GHAs. New workflows unless intended
to run on a matrix of OS should refer to Self-hosted runner GHAs [.github/workflow/README](.github/workflow/README.md).

### GitHub actions run types

The following GA CI Job runs are currently run for Apache Beam, and each of the runs have different
purpose and context.

#### Pull request run

Those runs are results of PR from the forks made by contributors. Most builds for Apache Beam fall
into this category. They are executed in the context of the "Fork", not main
Beam Code Repository which means that they have only "read" permission to all the GitHub resources
(container registry, code repository). This is necessary as the code in those PRs (including CI job
definition) might be modified by people who are not committers for the Apache Beam Code Repository.

The main purpose of those jobs is to check if PR builds cleanly, if the test run properly and if
the PR is ready to review and merge.

#### Direct Push/Merge Run

Those runs are results of direct pushes done by the committers or as result of merge of a Pull Request
by the committers. Those runs execute in the context of the Apache Beam Code Repository and have also
write permission for GitHub resources (container registry, code repository).
The main purpose for the run is to check if the code after merge still holds all the assertions - like
whether it still builds, all tests are green.

This is needed because some of the conflicting changes from multiple PRs might cause build and test failures
after merge even if they do not fail in isolation.

#### Scheduled runs

Those runs are results of (nightly) triggered job - only for `master` branch. The
main purpose of the job is to check if there was no impact of external dependency changes on the Apache
Beam code (for example transitive dependencies released that fail the build). Another reason for the nightly
build is that the builds tags most recent master with `nightly-master`.

All runs consist of the same jobs, but the jobs behave slightly differently or they are skipped in different
run categories. Here is a summary of the run categories with regards of the jobs they are running.
Those jobs often have matrix run strategy which runs several different variations of the jobs
(with different platform type / Python version to run for example)

### Google Cloud Platform Credentials

Some of the jobs require variables stored as [GitHub Secrets](https://docs.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets)
to perform operations on Google Cloud Platform.
These variables are:
 * `GCP_PROJECT_ID` - ID of the Google Cloud project. For example: `apache-beam-testing`.
 * `GCP_REGION` - Region of the bucket and dataflow jobs. For example: `us-central1`.
 * `GCP_TESTING_BUCKET` - Name of the bucket where temporary files for Dataflow tests will be stored. For example: `beam-github-actions-tests`.
 * `GCP_PYTHON_WHEELS_BUCKET` - Name of the bucket where python source distribution and wheels will be stored. For example: `beam-wheels-staging`.
 * `GCP_SA_EMAIL` - Service account email address. This is usually of the format `<name>@<project-id>.iam.gserviceaccount.com`.
 * `GCP_SA_KEY` - Service account key. This key should be created and encoded as a Base64 string (eg. `cat my-key.json | base64` on macOS).

Service Account shall have following permissions ([IAM roles](https://cloud.google.com/iam/docs/understanding-roles)):
 * Storage Admin (roles/storage.admin)
 * Dataflow Admin (roles/dataflow.admin)
 * Artifact Registry writer (roles/artifactregistry.createOnPush)
 * Big Query Data Editor (roles/bigquery.dataEditor)
 * Service Account User (roles/iam.serviceAccountUser)

### Workflows

#### Build python source distribution and wheels - [build_wheels.yml](.github/workflows/build_wheels.yml)

| Job                                             | Description                                                                                                                                                                                                                                                        | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|-------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Check GCP variables                             | Checks that GCP variables are set. Jobs which required them depend on the output of this job.                                                                                                                                                                      | Yes              | Yes                   | Yes           | Yes/No                   |
| Build python source distribution                | Builds python source distribution and uploads it to artifacts. Artifacts from release branch are used in release process ([`build_release_candidate.sh`](release/src/main/scripts/build_release_candidate.sh))                                                     | Yes              | Yes                   | Yes           | -                        |
| Prepare GCS                                     | Clears target path on GCS if already exists.                                                                                                                                                                                                                       | -                | Yes                   | Yes           | Yes                      |
| Upload python source distribution to GCS bucket | Uploads python source distribution to GCS bucket for path unique for specific workflow run.                                                                                                                                                                        | -                | Yes                   | Yes           | Yes                      |
| Build python wheels on linux/macos/windows      | Builds python wheels on linux/macos/windows platform with usage of `cibuildwheel` and uploads it to artifacts. Artifacts from release branch are used in release process ( [ `build_release_candidate.sh` ](release/src/main/scripts/build_release_candidate.sh) ) | Yes              | Yes                   | Yes           | -                        |
| Upload python wheels to GCS bucket              | Uploads python wheels to GCS bucket for path unique for specific workflow run. Additionally uploads workflow run data.                                                                                                                                             | -                | Yes                   | Yes           | Yes                      |
| List files on Google Cloud Storage Bucket       | Lists files on GCS for verification purpose.                                                                                                                                                                                                                       | -                | Yes                   | Yes           | Yes                      |
| Branch repo nightly                             | Branch repo with `nightly-master` if build python source distribution and python wheels finished successfully.                                                                                                                                                     | -                | -                     | Yes           | -                        |

#### Python tests - [python_tests.yml](.github/workflows/python_tests.yml)

| Job                              | Description                                                                                                           | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|----------------------------------|-----------------------------------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Check GCP variables              | Checks that GCP variables are set. Jobs which required them depend on the output of this job.                         | Yes              | Yes                   | Yes           | Yes/No                   |
| Build python source distribution | Builds python source distribution and uploads it to artifacts. Artifacts are used in `Python Wordcount Dataflow` job. | -                | Yes                   | Yes           | Yes                      |
| Python Unit Tests                | Runs python unit tests.                                                                                               | Yes              | Yes                   | Yes           | -                        |
| Python Wordcount Direct Runner   | Runs python WordCount example with Direct Runner.                                                                     | Yes              | Yes                   | Yes           | -                        |
| Python Wordcount Dataflow        | Runs python WordCount example with DataFlow Runner.                                                                   | -                | Yes                   | Yes           | Yes                      |

#### Java tests - [java_tests.yml](.github/workflows/java_tests.yml)

| Job                          | Description                                                                                   | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|------------------------------|-----------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Check GCP variables          | Checks that GCP variables are set. Jobs which required them depend on the output of this job. | Yes              | Yes                   | Yes           | Yes/No                   |
| Java Unit Tests              | Runs Java unit tests.                                                                         | Yes              | Yes                   | Yes           | -                        |
| Java Wordcount Direct Runner | Runs Java WordCount example with Direct Runner.                                               | Yes              | Yes                   | Yes           | -                        |
| Java Wordcount Dataflow      | Runs Java WordCount example with DataFlow Runner.                                             | -                | Yes                   | Yes           | Yes                      |

### Release Preparation and Validation Workflows

#### Start Snapshot Build - [start_snapshot_build.yml](.github/workflows/start_snapshot_build.yml)
| Job                   | Description                                                             | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|-----------------------|-------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Start Snapshot Build  | Creates PR against apache:master and triggers a job to build a snapshot | No               | No                    | No            | No                       |

#### Choose RC Commit - [choose_rc_commit.yml](.github/workflows/choose_rc_commit.yml)

| Job              | Description                                                                                         | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|------------------|-----------------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Choose RC Commit | Chooses a commit to be the basis of a release candidate and pushes a new tagged commit for that RC. | No               | No                    | No            | No                       |

#### Cut Release Branch - [verify_release_build.yml](.github/workflows/cut_release_branch.yml)
| Job                   | Description                                                | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|-----------------------|------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Update Master         | Update Apache Beam master branch with next release version | No               | No                    | No            | No                       |
| Update Release Branch | Cut release branch for current development version         | No               | No                    | No            | No                       |

#### Verify Release Build - [verify_release_build.yml](.github/workflows/verify_release_build.yml)

| Job                          | Description                                                                                   | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|------------------------------|-----------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Verify Release Build         | Verifies full life cycle of Gradle Build and all PostCommit/PreCommit tests against Release Branch on CI.                   | No               | No                    | No            | No                       |

#### Git tag Release Version - [git_tag_released_version.yml](.github/workflows/git_tag_released_version.yml)

| Job                             | Description                                                                                                    | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|---------------------------------|----------------------------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Git Tag Release Version         | Create and push a new tag for the released version by copying the tag for the final release candidate.         | No               | No                    | No            | No                       |

#### Run RC Validation - [run_rc_validation.yml](.github/workflows/run_rc_validation.yml)

| Job                          | Description                                                                                   | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Requires GCP Credentials |
|------------------------------|-----------------------------------------------------------------------------------------------|------------------|-----------------------|---------------|--------------------------|
| Python Release Candidate     | Comment on PR to trigger Python ReleaseCandidate Jenkins job.                                 | No               | No                    | No            | No                       |
| Python XLang SQL Taxi        | Runs Python XLang SQL Taxi with DataflowRunner                                                | No               | No                    | No            | Yes                      |
| Python XLang Kafka           | Runs Python XLang Kafka Taxi with DataflowRunner                                              | No               | No                    | No            | Yes                      |
| Direct Runner Leaderboard    | Runs Python Leaderboard with DirectRunner                                                     | No               | No                    | No            | Yes                      |
| Direct Runner GameStats      | Runs Python GameStats with DirectRunner.                                                      | No               | No                    | No            | Yes                      |
| Dataflow Runner Leaderboard  | Runs Python Leaderboard with DataflowRunner                                                   | No               | No                    | No            | Yes                      |
| Dataflow Runner GameStats    | Runs Python GameStats with DataflowRunner                                                     | No               | No                    | No            | Yes                      |

### All migrated workflows run based on the following triggers

| Description | Pull Request Run | Direct Push/Merge Run | Scheduled Run | Workflow Dispatch |
|-------------|------------------|-----------------------|---------------|-------------------|
| PostCommit  | No               | Yes                   | Yes           | Yes               |
| PreCommit   | Yes              | Yes                   | Yes           | Yes               |

### PreCommit Workflows

| Workflow                                                                         | Description             | Requires GCP Credentials  |
|----------------------------------------------------------------------------------|-------------------------|---------------------------|
| [job-precommit-placeholder.yml](.github/workflows/job-precommit-placeholder.yml) | Description placeholder | Yes/No                    |

### PostCommit Workflows

| Workflow                                                                           | Description             | Requires GCP Credentials |
|------------------------------------------------------------------------------------|-------------------------|--------------------------|
| [job-postcommit-placeholder.yml](.github/workflows/job-postcommit-placeholder.yml) | Description placeholder | Yes/No                   |

### GitHub Action Tips

* All migrated workflows get executed on **pre-configured self-hosted** runners. For this reason, GCP credentials are **only** needed when running the workflows in a different runner.
* If you introduce changes to the workflow it is possible that your changes will not be present in the check run triggered in Pull Request.
In this case please attach link to the modified workflow run executed on your fork.
* Possible timeouts with macOS runner - existing issue: [(X) This check failed - sometimes happens on macOS runner #841](https://github.com/actions/virtual-environments/issues/841)
* [GitHub Actions Documentation](https://docs.github.com/en/actions)
