#!/bin/bash

# Copyright 2020 Hyphenated Enterprises LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This build script implements the following best practices from the "Python on
# Docker Production Quickstart"
# (https://pythonspeed.com/products/productionquickstart/):
#
# * Use bash strict mode
# * Don't rely on the `latest` tag.
# * Warm up the build cache.
# * Warm up the build cache per-branch.
# * Record the build's version control revision and branch.
#
# There are many other best practices you may wish to implement; see the
# Quickstart for details.

# Best practice: Bash strict mode.
set -euo pipefail

# Image name (without tag, e.g. "yourregistry.example.com/yourorg/yourimage") is
# set to the first command-line argument:
IMAGE_NAME="$1"

# Get the Git commit and branch.
GIT_COMMIT=$(set -e && git rev-parse --short HEAD)
GIT_BRANCH=$(set -e && git rev-parse --abbrev-ref HEAD)

# Get the default Git branch; default to master if it can't figure it out. You
# may wish to edit this to some particular branch you want as the default; it
# will be used to prewarm the build cache for new branches.
GIT_DEFAULT_BRANCH=$(git rev-parse --abbrev-ref origin/HEAD || echo origin/master)
GIT_DEFAULT_BRANCH=$(basename "${GIT_DEFAULT_BRANCH}")

# Set two complete image names:
IMAGE_WITH_COMMIT="${IMAGE_NAME}:commit-${GIT_COMMIT}"
IMAGE_WITH_BRANCH="${IMAGE_NAME}:${GIT_BRANCH}"
IMAGE_WITH_DEFAULT_BRANCH="${IMAGE_NAME}:${GIT_DEFAULT_BRANCH}"
# Pull previous versions of the image, if any.
#
# Best practices:
# * Warm up the build cache, per-branch.
docker pull "${IMAGE_WITH_BRANCH}" || true
docker pull "${IMAGE_WITH_DEFAULT_BRANCH}" || true

# Build the image, giving it two names.
#
# Best practices:
# * Warm up the build cache, per-branch.
# * Don't rely on the `latest` tag.
# * Record the build's version control revision and branch.
docker image build \
       -t "${IMAGE_WITH_COMMIT}" \
       -t "${IMAGE_WITH_BRANCH}" \
       --cache-from "${IMAGE_WITH_BRANCH}" \
       --cache-from "${IMAGE_WITH_DEFAULT_BRANCH}" \
       --label "git-commit=${GIT_COMMIT}" \
       --label "git-branch=${GIT_BRANCH}" \
       --build-arg BUILDKIT_INLINE_CACHE=1 \
       -f nmdc_runtime/dagster.Dockerfile \
       .

# Push the newly created images.
docker push "${IMAGE_WITH_BRANCH}"
docker push "${IMAGE_WITH_COMMIT}"
