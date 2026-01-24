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
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.transfer.config.TransferConfig;
import nl.knaw.dans.transfer.core.FileService;

import java.nio.file.Path;
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
@Slf4j
public class FileSystemPermissionsHealthCheck extends HealthCheck {
    private final TransferConfig transferConfig;
    private final FileService fileService;

    public FileSystemPermissionsHealthCheck(TransferConfig transferConfig, FileService fileService) {
        this.transferConfig = transferConfig;
        this.fileService = fileService;
    }

    @Override
    protected Result check() {
        var result = Result.builder();
        var isValid = true;
        var sameFileSystemPaths = Stream.of(
            transferConfig.getExtractMetadata().getInbox().getPath(),
            transferConfig.getExtractMetadata().getOutbox().getProcessed(),
            transferConfig.getExtractMetadata().getOutbox().getFailed(),
            transferConfig.getExtractMetadata().getOutbox().getRejected(),
            transferConfig.getSendToVault().getInbox().getPath(),
            transferConfig.getSendToVault().getOutbox().getProcessed(),
            transferConfig.getSendToVault().getOutbox().getFailed(),
            transferConfig.getSendToVault().getDataVault().getCurrentBatchWorkingDir(),
            transferConfig.getSendToVault().getDataVault().getBatchRoot()
            // N.B. the transfer inboxes are on an NFS mount, so they are not on the same file system by design!!
        ).collect(Collectors.toSet());

        var accessibleDirectories = Sets.union(sameFileSystemPaths, Stream.of(
            transferConfig.getCollectDve().getInbox().getPath(),
            transferConfig.getCollectDve().getProcessed(),
            transferConfig.getNbnRegistration().getOutbox()
        ).collect(Collectors.toSet()));

        for (var path : accessibleDirectories) {
            /*
             * Retrying, because in some cases (currently only transferConfig.getSendToVault().getDataVault().getCurrentBatchWorkingDir())
             * the directory may briefly not exist when it is being recreated after sending off a batch.
             */
            if (!fileService.exists(path, 5, 1000)) {
                result.withDetail(path.toString(), "Path does not exist");
                log.error("Path does not exist: " + path);
                isValid = false;
                continue;
            }
            if (!fileService.canWriteTo(path)) {
                result.withDetail(path.toString(), "Path is not writable");
                log.error("Path is not writable: " + path);
                isValid = false;
            }
            if (!fileService.canReadFrom(path)) {
                result.withDetail(path.toString(), "Path is not readable");
                log.error("Path is not readable: " + path);
                isValid = false;
            }
        }

        if (!fileService.isSameFileSystem(sameFileSystemPaths)) {
            var p = sameFileSystemPaths.stream().map(Path::toString).sorted().collect(Collectors.joining(", "));
            result.withDetail(p, "Paths are not all on the same file system: " + p);
            log.error("Paths are not all on the same file system: " + p);
            isValid = false;
        }

        if (isValid) {
            return result.healthy().build();
        }
        else {
            var details = result.build().getDetails();

            return result.unhealthy().withMessage("File system conditions are invalid: " + details).build();
        }
    }
}
