#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[4]


class VersionUpdateTest(unittest.TestCase):
  def setUp(self):
    self.directory = tempfile.TemporaryDirectory()
    self.addCleanup(self.directory.cleanup)
    self.root = pathlib.Path(self.directory.name)
    self.script = self.root / 'update_version.sh'
    shutil.copyfile(ROOT / 'update_version.sh', self.script)
    self.properties = self.root / 'gradle.properties'
    self.original = (
        '# fixture\nversion=2.75.1\nsdk_version=2.75.1\n'
        'javaVersion=11\nflink_versions=1.20,2.2\npython_versions=3.10,3.11\n')
    self.properties.write_text(self.original)

  def run_update(self, *arguments):
    return subprocess.run(
        ['bash', str(self.script), *arguments],
        cwd=self.root.parent,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False)

  def test_explicit_version_changes_only_java_version_properties(self):
    result = self.run_update('2.75.8')
    self.assertEqual(result.returncode, 0, result.stdout)
    self.assertEqual(
        self.properties.read_text(), self.original.replace('2.75.1', '2.75.8'))
    self.assertEqual(
        sorted(path.name for path in self.root.iterdir()),
        ['gradle.properties', 'update_version.sh'])

  def test_default_increments_patch(self):
    result = self.run_update()
    self.assertEqual(result.returncode, 0, result.stdout)
    self.assertEqual(
        self.properties.read_text(), self.original.replace('2.75.1', '2.75.2'))

  def test_repeated_version_is_idempotent(self):
    for _ in range(2):
      result = self.run_update('2.75.1')
      self.assertEqual(result.returncode, 0, result.stdout)
      self.assertEqual(self.properties.read_text(), self.original)

  def test_invalid_versions_and_unsupported_options_do_not_write(self):
    for value in ['2.75', '2.75.1-SNAPSHOT', '2.75.1;exit', '--python', '--git-add']:
      with self.subTest(value=value):
        result = self.run_update(value)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.properties.read_text(), self.original)

  def test_missing_or_duplicate_properties_do_not_write(self):
    for text in [
        self.original.replace('version=2.75.1\n', '', 1),
        self.original.replace('sdk_version=2.75.1\n', ''),
        self.original + 'version=2.75.2\n',
        self.original + 'sdk_version=2.75.2\n',
    ]:
      with self.subTest(text=text):
        self.properties.write_text(text)
        result = self.run_update('2.75.8')
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.properties.read_text(), text)


class PublicationMetadataTest(unittest.TestCase):
  def setUp(self):
    self.directory = tempfile.TemporaryDirectory()
    self.addCleanup(self.directory.cleanup)
    self.home = pathlib.Path(self.directory.name) / 'home'
    maven = self.home / '.m2'
    maven.mkdir(parents=True)
    self.ambient_settings = maven / 'settings.xml'
    self.ambient_xml = (
        '<settings><servers><server><id>github</id>'
        '<username>metadata-test</username>'
        '<password>${env.BEAM_METADATA_TEST_TOKEN}</password>'
        '</server></servers></settings>')
    self.ambient_settings.write_text(self.ambient_xml)
    self.settings = pathlib.Path(self.directory.name) / 'empty-settings.xml'
    self.settings.write_text('<settings/>')
    self.environment = os.environ.copy()
    self.environment.pop('BEAM_METADATA_TEST_TOKEN', None)
    self.environment['ARTIFACTORY_USER'] = 'publication-test'
    self.environment['ARTIFACTORY_KEY'] = 'not-a-real-key'
    self.gradle_home = (
        os.environ.get('GRADLE_USER_HOME') or
        str(pathlib.Path.home() / '.gradle'))

  def run_gradle(self, tasks, *arguments):
    command = [
        str(ROOT / 'gradlew'),
        *tasks,
        '-I',
        str(pathlib.Path(__file__).with_name('linkedin_publication.gradle')),
        '-Ppublishing',
        '-PnoSigning',
        '-PpublicationTestSettings=' + str(self.settings),
        '-Duser.home=' + str(self.home),
        '--gradle-user-home',
        self.gradle_home,
        '--no-daemon',
        '--no-build-cache',
        '--rerun-tasks',
        '--max-workers=2',
        '--console=plain',
        *arguments,
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=self.environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False)
    self.assertEqual(self.ambient_settings.read_text(), self.ambient_xml)
    return result

  def test_coordinates_versions_repositories_and_boms(self):
    base = re.search(
        r'^version=(.+)$',
        (ROOT / 'gradle.properties').read_text(),
        re.MULTILINE).group(1)
    modules = [
        ':sdks:java:core',
        ':sdks:java:extensions:avro',
        ':sdks:java:io:google-cloud-platform',
        ':runners:flink:1.20',
        ':runners:flink:2.2',
        ':runners:spark:3',
        ':runners:google-cloud-dataflow-java',
    ]
    modes = [
        (['-PisLinkedin', '-PisRelease'], 'com.linkedin.beam', base),
        (['-PisLinkedin'], 'com.linkedin.beam', base + '-SNAPSHOT'),
        (['-PisLinkedin', '-PisRelease', '-PisSnapshot'],
         'com.linkedin.beam',
         base + '-SNAPSHOT'),
        (['-PisLinkedin', '-PisRelease', '-PisSnapshot=false'],
         'com.linkedin.beam',
         base),
        (['-PisLinkedin', '-PisSnapshot', '-Pversion=' + base + '-SNAPSHOT'],
         'com.linkedin.beam',
         base + '-SNAPSHOT'),
        (['-PisLinkedin=false', '-PisSnapshot'], 'org.apache.beam', base),
        ([], 'org.apache.beam', base),
    ]
    for flags, group, version in modes:
      with self.subTest(flags=flags):
        result = self.run_gradle(
            [module + ':checkLinkedinPublication' for module in modules],
            '-PexpectedPublicationGroup=' + group,
            '-PexpectedPublicationVersion=' + version,
            *flags)
        self.assertEqual(result.returncode, 0, result.stdout)

  def test_auto_elr_publication_contract(self):
    base = re.search(
        r'^version=(.+)$',
        (ROOT / 'gradle.properties').read_text(),
        re.MULTILINE).group(1)
    for flags, group, version in [
        (['-PisLinkedin', '-PisRelease'], 'com.linkedin.beam', base),
        (['-PisLinkedin'], 'com.linkedin.beam', base + '-SNAPSHOT'),
        (['-PisLinkedin=false'], 'org.apache.beam', base),
    ]:
      with self.subTest(flags=flags):
        result = self.run_gradle(
            [':auto-elr:checkAutoElrPublication'],
            '-I',
            str(pathlib.Path(__file__).with_name('auto_elr_publication.gradle')),
            '-PexpectedPublicationGroup=' + group,
            '-PexpectedPublicationVersion=' + version,
            *flags)
        self.assertEqual(result.returncode, 0, result.stdout)

  def test_remote_publisher_is_explicit_and_guarded(self):
    base = re.search(
        r'^version=(.+)$',
        (ROOT / 'gradle.properties').read_text(),
        re.MULTILINE).group(1)
    result = self.run_gradle(
        [
            ':auto-elr:checkAutoElrPublication',
            ':auto-elr:checkLinkedinPublicationGuard',
        ],
        '-I',
        str(pathlib.Path(__file__).with_name('auto_elr_publication.gradle')),
        '-PexpectedPublicationGroup=com.linkedin.beam',
        '-PexpectedPublicationVersion=' + base,
        '-PpublicationTask=publishMavenJavaPublicationToLinkedin.jfrog.httpsRepository',
        '-PisLinkedin',
        '-PisRelease',
        '-PlinkedinPublish')
    self.assertEqual(result.returncode, 0, result.stdout)
    result = self.run_gradle(
        [':auto-elr:tasks'], '-PisLinkedin=false', '-PlinkedinPublish')
    self.assertNotEqual(result.returncode, 0, result.stdout)
    self.assertIn('-PlinkedinPublish requires -PisLinkedin', result.stdout)
    self.environment.update(
        GITHUB_ACTIONS='true',
        GITHUB_REPOSITORY='linkedin/beam',
        GITHUB_EVENT_NAME='pull_request',
        GITHUB_REF='refs/pull/1/merge')
    result = self.run_gradle(
        [':validateLinkedinPublication'],
        '-PisLinkedin', '-PisRelease', '-PlinkedinPublish',
        '-PlinkedinVersion=' + base)
    self.assertNotEqual(result.returncode, 0, result.stdout)
    self.assertIn('requires li_trunk or a li-release-* branch', result.stdout)

  def test_auto_elr_staging_order_and_failure(self):
    base = re.search(
        r'^version=(.+)$',
        (ROOT / 'gradle.properties').read_text(),
        re.MULTILINE).group(1)
    self.environment.pop('ARTIFACTORY_USER', None)
    self.environment.pop('ARTIFACTORY_KEY', None)
    task = 'publishMavenJavaPublicationToMavenLocal'
    marker = ':auto-elr:' + task
    for name, version, extra in [
        ('failure', base, ['-PpublicationFailure=:model:pipeline']),
        ('release', base, []),
        ('snapshot', base + '-SNAPSHOT', ['-PisSnapshot']),
    ]:
      with self.subTest(mode=name):
        repository = pathlib.Path(self.directory.name) / name
        events = pathlib.Path(self.directory.name) / (name + '-events.txt')
        result = self.run_gradle(
            [marker],
            '-I',
            str(pathlib.Path(__file__).with_name('auto_elr_publication.gradle')),
            '-PisLinkedin', '-PisRelease', '-PlinkedinPublish',
            '-Dmaven.repo.local=' + str(repository),
            '-PpublicationEvents=' + str(events),
            '--continue',
            *extra)
        marker_dir = repository / 'com/linkedin/beam/beam-auto-elr' / version
        lines = events.read_text().splitlines()
        if name == 'failure':
          self.assertNotEqual(result.returncode, 0, result.stdout)
          self.assertIn('Injected publication failure: :model:pipeline', result.stdout)
          self.assertNotIn('start\t' + marker, lines)
          self.assertFalse(marker_dir.exists())
          continue
        self.assertEqual(result.returncode, 0, result.stdout)
        marker_start = lines.index('start\t' + marker)
        completed = [
            line for line in lines[:marker_start] if line.startswith('done\t')
        ]
        self.assertEqual(len(completed), 30)
        self.assertEqual(lines[-1], 'done\t' + marker)

        namespace = {'m': 'http://maven.apache.org/POM/4.0.0'}
        pending = ['beam-auto-elr']
        visited = set()
        while pending:
          artifact = pending.pop()
          if artifact in visited:
            continue
          visited.add(artifact)
          directory = repository / 'com/linkedin/beam' / artifact / version
          pom = ET.parse(directory / (artifact + '-' + version + '.pom')).getroot()
          self.assertEqual(pom.findtext('m:groupId', namespaces=namespace), 'com.linkedin.beam')
          self.assertEqual(pom.findtext('m:version', namespaces=namespace), version)
          with zipfile.ZipFile(directory / (artifact + '-' + version + '.jar')) as jar:
            metadata = 'META-INF/maven/com.linkedin.beam/' + artifact + '/pom.xml'
            embedded = ET.fromstring(jar.read(metadata))
            self.assertEqual(
                embedded.findtext('m:version', namespaces=namespace), version)
          for dependency in pom.findall('m:dependencies/m:dependency', namespace):
            if dependency.findtext('m:groupId', namespaces=namespace) == 'com.linkedin.beam':
              self.assertEqual(
                  dependency.findtext('m:version', namespaces=namespace), version)
              pending.append(dependency.findtext('m:artifactId', namespaces=namespace))
        self.assertEqual(len(visited), 31)
        consumer = pathlib.Path(self.directory.name) / (name + '-consumer')
        consumer.mkdir()
        (consumer / 'settings.gradle').write_text("rootProject.name = 'auto-elr-consumer'\n")
        (consumer / 'build.gradle').write_text(f"""
plugins {{ id 'java' }}
repositories {{
  maven {{
    url '{repository.as_uri()}'
    content {{ includeGroup 'com.linkedin.beam' }}
  }}
  mavenCentral {{ content {{ excludeGroup 'com.linkedin.beam' }} }}
  maven {{
    url 'https://packages.confluent.io/maven/'
    content {{ includeGroup 'io.confluent' }}
  }}
}}
dependencies {{ implementation 'com.linkedin.beam:beam-auto-elr:{version}' }}
tasks.register('verifyResolution') {{
  doLast {{
    def artifacts = configurations.runtimeClasspath.resolvedConfiguration.resolvedArtifacts
    def fork = artifacts.findAll {{ it.moduleVersion.id.group == 'com.linkedin.beam' }}
    assert fork.size() == 31
    assert fork.every {{ it.moduleVersion.id.version == '{version}' }}
    assert artifacts.findAll {{ it.moduleVersion.id.group == 'org.apache.beam' }}
        .every {{ it.moduleVersion.id.name.startsWith('beam-vendor-') }}
  }}
}}
""")
        result = self.run_gradle(['-p', str(consumer), 'verifyResolution'])
        self.assertEqual(result.returncode, 0, result.stdout)


if __name__ == '__main__':
  unittest.main()
