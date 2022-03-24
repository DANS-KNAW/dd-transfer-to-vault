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
package nl.knaw.dans.ttv.core;

import nl.knaw.dans.ttv.core.service.ArchiveStatusService;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.VaultCatalogService;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.openapi.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class ConfirmArchivedTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConfirmArchivedTask.class);

    private final Tar tar;
    private final TransferItemService transferItemService;
    private final ArchiveStatusService archiveStatusService;
    private final FileService fileService;
    private final Path workingDir;
    private final VaultCatalogService vaultCatalogService;

    public ConfirmArchivedTask(Tar tar, TransferItemService transferItemService, ArchiveStatusService archiveStatusService, FileService fileService,
        Path workingDir, VaultCatalogService vaultCatalogService) {
        this.transferItemService = transferItemService;
        this.archiveStatusService = archiveStatusService;
        this.fileService = fileService;
        this.workingDir = workingDir;
        this.tar = tar;
        this.vaultCatalogService = vaultCatalogService;
    }

    @Override
    public void run() {
        log.info("Running confirm archive task {}", this);

        var tarId = tar.getTarUuid();

        try {
            var fileStatus = archiveStatusService.getFileStatus(tarId);
            var completelyArchived = isCompletelyArchived(fileStatus);

            if (completelyArchived) {
                log.info("All files in tar archive '{}' have been archived to tape", tarId);
                transferItemService.updateTarToArchived(tar);

                try {
                    log.info("Cleaning workdir files and folders for tar archive '{}'", tarId);
                    var targetPath = workingDir.resolve(tarId);
                    fileService.deleteDirectory(targetPath);
                }
                catch (IOException e) {
                    log.error("Unable to cleanup TAR OCFL repository in directory '{}/{}'", workingDir, tarId, e);
                }
            }
            else {
                log.info("Some files in tar archive '{}' have not yet been archived to tape", tarId);
                transferItemService.resetTarToArchiving(tar);
            }

            vaultCatalogService.addOrUpdateTar(tar);
        }
        catch (IOException | InterruptedException | ApiException e) {
            log.error("An error occurred while checking archiving status", e);

            // in case it fails to check, still set the transfer status to OCFLTARCREATED and reset the checking flag
            transferItemService.resetTarToArchiving(tar);
        }
    }

    public boolean isCompletelyArchived(Map<String, ArchiveStatusService.FileStatus> statusMap) {
        for (var entry : statusMap.entrySet()) {
            log.debug("File entry '{}' has status {}", entry.getKey(), entry.getValue());

            var archived = entry.getValue().equals(ArchiveStatusService.FileStatus.OFFLINE)
                || entry.getValue().equals(ArchiveStatusService.FileStatus.DUAL);

            if (!archived) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        return "ConfirmArchiveTask{" +
            "tarId='" + tar.getTarUuid() + '\'' +
            ", workingDir='" + workingDir + '\'' +
            '}';
    }
}
