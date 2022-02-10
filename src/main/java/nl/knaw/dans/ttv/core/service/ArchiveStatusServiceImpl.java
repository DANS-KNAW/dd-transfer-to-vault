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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ArchiveStatusServiceImpl implements ArchiveStatusService {
    private static final Logger log = LoggerFactory.getLogger(ArchiveStatusServiceImpl.class);

    private final ProcessRunner processRunner;
    private final DataArchiveConfiguration dataArchiveConfiguration;
    private final Pattern linePattern = Pattern.compile(
        "^(?<perms>[a-zA-Z\\-]+) +(?<uid>\\d+) +(?<uname>[a-zA-Z0-9]+) +(?<filesize>\\d+) +(?<timestamp>[0-9\\- :]+\\d) +\\((?<status>[A-Z/]+)\\) +(?<filename>.*)");

    public ArchiveStatusServiceImpl(DataArchiveConfiguration dataArchiveConfiguration, ProcessRunner processRunner) {
        this.processRunner = processRunner;
        this.dataArchiveConfiguration = dataArchiveConfiguration;
    }

    @Override
    public Map<String, FileStatus> getFileStatus(String id) throws IOException, InterruptedException {

        var host = getSshHost();
        var path = Path.of(dataArchiveConfiguration.getPath(), id + ".dmftar");

        var command = new String[] {
            "ssh",
            host,
            "find",
            path.toString(),
            "-type",
            "f",
            "|",
            "xargs",
            "dmls",
            "-lg"
        };

        var statusReport = new HashMap<String, FileStatus>();

        try {
            var result = processRunner.run(command);

            if (result.getStatusCode() != 0) {
                throw new IOException(String.format("invalid status code, expected 0, got %s", result.getStatusCode()));
            }

            for (var line : result.getStdout().split("\n")) {
                var matcher = linePattern.matcher(line);

                if (matcher.matches()) {
                    var status = matcher.group("status");
                    var filename = matcher.group("filename");

                    statusReport.put(filename, FileStatus.fromString(status));
                }
            }
        }
        catch (IOException | InterruptedException e) {
            log.error("error communicating with archive server", e);
            throw e;
        }

        return statusReport;
    }

    private String getSshHost() {
        return String.format("%s@%s", this.dataArchiveConfiguration.getUser(), this.dataArchiveConfiguration.getHost());
    }

}
