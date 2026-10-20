#!/usr/bin/env bash
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

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
python3 - "$SCRIPT_DIR/gradle.properties" "$@" <<'PY'
import argparse
import pathlib
import re
import sys

parser = argparse.ArgumentParser(
    prog="update_version.sh",
    description="Update the LinkedIn Java version, or increment its patch number.")
parser.add_argument("version", nargs="?", help="release version, for example 2.75.1")
args = parser.parse_args(sys.argv[2:])
properties = pathlib.Path(sys.argv[1])
text = properties.read_text(encoding="utf-8")

values = {}
for name in ("version", "sdk_version"):
    matches = re.findall(r"^" + name + r"=(.*)$", text, flags=re.MULTILINE)
    if len(matches) != 1:
        parser.error("gradle.properties must contain exactly one " + name + " entry")
    values[name] = matches[0]

target = args.version
if target is None:
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", values["version"]):
        parser.error("current version must have the form major.minor.patch")
    major, minor, patch = values["version"].split(".")
    target = f"{major}.{minor}.{int(patch) + 1}"
if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", target):
    parser.error("version must have the form major.minor.patch; use -PisSnapshot when building")

updated = text
for name in values:
    updated = re.sub(
        r"^" + name + r"=.*$",
        name + "=" + target,
        updated,
        flags=re.MULTILINE)
if updated != text:
    properties.write_text(updated, encoding="utf-8")
print("LinkedIn Java version: " + target)
PY
