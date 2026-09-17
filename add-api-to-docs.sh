#!/usr/bin/env bash
#
# Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

export MAVEN_ARGS="-s .github/ci-settings.xml"

mvn $MAVEN_ARGS dans-build-resources:get-helper-script
mvn $MAVEN_ARGS initialize # To ensure API definition is downloaded

echo "Deploying Swagger UI and API definition..."
ARTIFACT_ID=$(mvn $MAVEN_ARGS help:evaluate -Dexpression=project.artifactId -q -DforceStdout)
sh target/add-swagger-ui.sh
cp target/openapi/${ARTIFACT_ID}-api.yml docs/api.yml
echo "DONE"
