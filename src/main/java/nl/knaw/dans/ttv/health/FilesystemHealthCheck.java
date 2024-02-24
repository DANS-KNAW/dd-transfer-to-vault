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
import nl.knaw.dans.ttv.core.config.DdTransferToVaultConfig;
import nl.knaw.dans.ttv.core.service.FileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;

public class FilesystemHealthCheck extends HealthCheck {
    private static final Logger log = LoggerFactory.getLogger(FilesystemHealthCheck.class);

    private final DdTransferToVaultConfig configuration;
    private final FileService fileService;

    public FilesystemHealthCheck(DdTransferToVaultConfig configuration, FileService fileService) {
        this.configuration = configuration;
        this.fileService = fileService;
    }

    @Override
    protected Result check() throws Exception {
        log.debug("Checking if all paths are readable and writeable");
        var healthy = true;
        var paths = new ArrayList<Path>();
        paths.add(configuration.getExtractMetadata().getInbox());

        for (var path : paths) {
            var canRead = fileService.canRead(path);
            var canWrite = fileService.canWrite(path);

            if (canRead && canWrite) {
                log.debug("Path '{}' exists and is readable", path);
            }
            else {
                if (!canRead) {
                    log.error("Path '{}' is not readable", path);
                }
                if (!canWrite) {
                    log.error("Path '{}' is not writeable", path);
                }

                healthy = false;
            }
        }

        if (healthy) {
            return Result.healthy();
        }
        else {
            return Result.unhealthy("Not all paths are readable and writeable");
        }
    }
}
