# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os

import pytest
from pytest_rally.rally import RALLY_CONFIG_DIR


def pytest_addoption(parser):
    group = parser.getgroup("esrally")
    group.addoption(
        "--track-repository",
        action="store",
        default=os.path.join(RALLY_CONFIG_DIR, "benchmarks", "tracks", "default"),
        help="Path to a local track repository",
    )
    group.addoption("--track-revision", action="store", default="master", help="Track repository revision to test")
    group.addoption(
        "--track-test-directory",
        action="store",
        dest="track_test_dir",
        default="it",
        help=("Name of the directory containing the track repo's integration tests" "(default: `it`)"),
    )


def pytest_cmdline_main(config):
    if hasattr(config, "args"):
        repo = config.option.track_repository
        if repo not in config.args:
            config.args.append(f"{repo}/{config.option.track_test_dir}")
