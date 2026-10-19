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

import pathlib
import re
import shutil
import subprocess
import tempfile
import unittest

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
        command = [
            str(ROOT / 'gradlew'),
            *[module + ':checkLinkedinPublication' for module in modules],
            '-I',
            str(pathlib.Path(__file__).with_name('linkedin_publication.gradle')),
            '-Ppublishing',
            '-PnoSigning',
            '-PexpectedPublicationGroup=' + group,
            '-PexpectedPublicationVersion=' + version,
            '--no-daemon',
            '--no-build-cache',
            '--rerun-tasks',
            '--max-workers=2',
            '--console=plain',
            *flags,
        ]
        result = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False)
        self.assertEqual(result.returncode, 0, result.stdout)


if __name__ == '__main__':
  unittest.main()
