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

import nl.knaw.dans.ttv.core.config.DataArchiveConfiguration;
import nl.knaw.dans.ttv.core.domain.ProcessResult;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public class TarCommandRunnerImpl implements TarCommandRunner {
    private final ProcessRunner processRunner;
    private final DataArchiveConfiguration dataArchiveConfiguration;

    public TarCommandRunnerImpl(DataArchiveConfiguration dataArchiveConfiguration, ProcessRunner processRunner) {
        this.processRunner = processRunner;
        this.dataArchiveConfiguration = dataArchiveConfiguration;
    }

    @Override
    public ProcessResult tarDirectory(Path path, String target) throws IOException, InterruptedException {
        Objects.requireNonNull(path, "path cannot be null");
        Objects.requireNonNull(target, "target cannot be null");

        var remotePath = Path.of(dataArchiveConfiguration.getPath(), target);
        var command = new String[] {
            "dmftar",
            "-c",
            "-f",
            String.format("%s:%s", getSshHost(), remotePath),
            ".",
        };

        return processRunner.run(command, path);
    }

    @Override
    public ProcessResult verifyPackage(String path) throws IOException, InterruptedException {
        Objects.requireNonNull(path, "path cannot be null");

        var remotePath = Path.of(dataArchiveConfiguration.getPath(), path);
        var command = new String[] {
            "ssh",
            getSshHost(),
            "dmftar",
            "--verify",
            "-f",
            remotePath.toString()
        };

        return processRunner.run(command);
    }

    @Override
    public ProcessResult deletePackage(String path) throws IOException, InterruptedException {
        var remotePath = Path.of(dataArchiveConfiguration.getPath(), path);
        var command = new String[] {
            "ssh",
            getSshHost(),
            "dmftar",
            "--delete-archive",
            "-f",
            remotePath.toString()
        };

        return processRunner.run(command);
    }

    @Override
    public ProcessResult getDmftarVersion() throws IOException, InterruptedException {
        var command = new String[] {
            "ssh",
            "-o", "BatchMode=yes",
            getSshHost(),
            "dmftar",
            "--version"
        };

        return processRunner.run(command);
    }

    private String getSshHost() {
        return String.format("%s@%s", this.dataArchiveConfiguration.getUser(), this.dataArchiveConfiguration.getHost());
    }
}
