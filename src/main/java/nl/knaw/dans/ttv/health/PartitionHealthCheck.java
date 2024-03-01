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

package nl.knaw.dans.ttv.health;

import com.codahale.metrics.health.HealthCheck;
import nl.knaw.dans.ttv.config.DdTransferToVaultConfig;
import nl.knaw.dans.ttv.core.service.FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;

public class PartitionHealthCheck extends HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(PartitionHealthCheck.class);

    private final DdTransferToVaultConfig configuration;
    private final FileService fileService;

    public PartitionHealthCheck(DdTransferToVaultConfig configuration, FileService fileService) {
        this.configuration = configuration;
        this.fileService = fileService;
    }

    @Override
    protected Result check() throws Exception {
        var healthy = true;
        var paths = new ArrayList<Path>();
        paths.add(configuration.getExtractMetadata().getInbox());

        log.debug("Checking if all paths are on the same partition");
        var fileSystems = new HashSet<FileStore>();

        for (var path : paths) {
            try {
                var fs = fileService.getFileStore(path);
                log.trace("FileStore for path '{}' is {}", path, fs);
                fileSystems.add(fs);
            }
            catch (IOException e) {
                log.error("Unable to get file store for path '{}'", path, e);
                healthy = false;
            }
        }

        if (fileSystems.size() == 1) {
            log.debug("All paths are on the same partition");
        }
        else {
            log.error("Not all paths are on the same filesystem, found the following filesystems");

            for (var fs : fileSystems) {
                log.info("Filesystem: {}", fs);
            }

            healthy = false;
        }

        if (healthy) {
            return Result.healthy();
        }
        else {
            return Result.unhealthy("Not all paths are on the same partition");
        }
    }
}
