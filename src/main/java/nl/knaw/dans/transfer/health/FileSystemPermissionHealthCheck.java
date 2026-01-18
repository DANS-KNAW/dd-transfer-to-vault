/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.transfer.health;

import com.codahale.metrics.health.HealthCheck;
import nl.knaw.dans.transfer.config.NbnRegistrationConfig;
import nl.knaw.dans.transfer.config.TransferConfig;
import nl.knaw.dans.transfer.core.FileService;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Health check for file system permissions:
 * <li>
 *     <ul>All configured directories must be writable by the current user.</ul>
 *     <ul>All configured directories must be on the same file system.</ul>
 * </li>
 *
 */
public class FileSystemPermissionHealthCheck extends HealthCheck {
    private final TransferConfig transferConfig;
    private final NbnRegistrationConfig nbnRegistrationConfig;
    private final FileService fileService;

    public FileSystemPermissionHealthCheck(TransferConfig transferConfig, NbnRegistrationConfig nbnRegistrationConfig, FileService fileService) {
        this.transferConfig = transferConfig;
        this.nbnRegistrationConfig = nbnRegistrationConfig;
        this.fileService = fileService;
    }

    @Override
    protected Result check() {
        var result = Result.builder();
        var isValid = true;
        var allPaths = Stream.of(
            transferConfig.getExtractMetadata().getInbox().getPath(),
            transferConfig.getExtractMetadata().getOutbox().getProcessed(),
            transferConfig.getExtractMetadata().getOutbox().getFailed(),
            transferConfig.getExtractMetadata().getOutbox().getRejected(),
            transferConfig.getSendToVault().getInbox().getPath(),
            transferConfig.getSendToVault().getOutbox().getProcessed(),
            transferConfig.getSendToVault().getOutbox().getFailed(),
            transferConfig.getSendToVault().getDataVault().getCurrentBatchWorkingDir(),
            transferConfig.getSendToVault().getDataVault().getBatchRoot(),
            nbnRegistrationConfig.getInbox().getPath(),
            nbnRegistrationConfig.getOutbox().getProcessed(),
            nbnRegistrationConfig.getOutbox().getFailed(),
            transferConfig.getCollectDve().getInbox().getPath(),
            transferConfig.getCollectDve().getProcessed()
        ).collect(Collectors.toSet());

        for (var path : allPaths) {
            if (!fileService.canWriteTo(path)) {
                result.withDetail(path.toString(), "Path is not writable");
                isValid = false;
            }
        }

        if (!fileService.isSameFileSystem(allPaths)) {
            var p = allPaths.stream().map(Path::toString).sorted().collect(Collectors.joining(", "));
            result.withDetail(p, "Paths are not all on the same file system: " + p);
            isValid = false;
        }

        if (isValid) {
            return result.healthy().build();
        }
        else {
            var failedPaths = result.build().getDetails().keySet();
            return result.unhealthy().withMessage("File system conditions are invalid: " + String.join(", ", failedPaths)).build();
        }
    }
}
