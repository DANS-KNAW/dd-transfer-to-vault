/*
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.ttv.core.service;

import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.config.DataArchiveConfiguration;
import nl.knaw.dans.ttv.core.domain.ArchiveMetadata;

import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Pattern;

@Slf4j
public class ArchiveMetadataServiceImpl implements ArchiveMetadataService {

    private final ProcessRunner processRunner;
    private final DataArchiveConfiguration dataArchiveConfiguration;
    private final Pattern linePattern = Pattern.compile(
        "^.*\\.dmftar/(?<part>\\d+)/.*::: (?<algorithm>[a-zA-Z0-9_]+) (?<checksum>[^ ]+).*");

    public ArchiveMetadataServiceImpl(DataArchiveConfiguration dataArchiveConfiguration, ProcessRunner processRunner) {
        this.processRunner = processRunner;
        this.dataArchiveConfiguration = dataArchiveConfiguration;
    }

    @Override
    public ArchiveMetadata getArchiveMetadata(String id) throws IOException, InterruptedException {
        var path = id + ".dmftar";
        var remotePath = Path.of(dataArchiveConfiguration.getPath(), path);
        var command = new String[] {
            "ssh",
            getSshHost(),
            "find",
            remotePath.toString(),
            "-name",
            "*.chksum",
            "|",
            "xargs",
            "-l",
            "-I",
            "%",
            "sh",
            "-c",
            "'echo % ::: $(cat %)'"
        };

        var output = processRunner.run(command);

        if (output.getStatusCode() != 0) {
            throw new IOException(String.format(
                "Unable to get checksum list because process returned status code %s with message: %s",
                output.getStatusCode(), output.getStdout()));
        }

        var lines = output.getStdout().trim().split("\n");
        var result = new ArchiveMetadata();

        for (var line : lines) {
            var matcher = linePattern.matcher(line);

            if (matcher.matches()) {
                var part = new ArchiveMetadata.ArchiveMetadataPart();
                part.setChecksum(matcher.group("checksum"));
                part.setChecksumAlgorithm(matcher.group("algorithm"));
                part.setIdentifier(matcher.group("part"));

                result.getParts().add(part);
            }
        }

        return result;
    }

    private String getSshHost() {
        return String.format("%s@%s", this.dataArchiveConfiguration.getUser(), this.dataArchiveConfiguration.getHost());
    }
}
