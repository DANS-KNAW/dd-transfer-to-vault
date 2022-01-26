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
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.TransferItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class ConfirmArchivedTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConfirmArchivedTask.class);

    private final String tarId;
    private final TransferItemService transferItemService;
    private final ArchiveStatusService archiveStatusService;
    private final OcflRepositoryService ocflRepositoryService;
    private final String workingDir;
    private final String dataArchiveRoot;

    public ConfirmArchivedTask(String tarId, TransferItemService transferItemService, ArchiveStatusService archiveStatusService, OcflRepositoryService ocflRepositoryService,
        String workingDir, String dataArchiveRoot) {
        this.transferItemService = transferItemService;
        this.archiveStatusService = archiveStatusService;
        this.ocflRepositoryService = ocflRepositoryService;
        this.workingDir = workingDir;
        this.dataArchiveRoot = dataArchiveRoot;
        this.tarId = tarId;
    }

    @Override
    public void run() {
        log.info("running confirm archive task {}", this);

        try {
            var fileStatus = archiveStatusService.getFileStatus(tarId);
            var completelyArchived = isCompletelyArchived(fileStatus);

            if (completelyArchived) {
                log.info("all files in tar archive '{}' have been archived to tape", tarId);
                transferItemService.updateCheckingProgressResults(tarId, TransferItem.TransferStatus.CONFIRMEDARCHIVED);

                try {
                    ocflRepositoryService.cleanupRepository(Path.of(workingDir), tarId);
                } catch (IOException e) {
                    log.error("unable to cleanup TAR OCFL repository in directory '{}/{}'", workingDir, tarId);
                }
            } else {
                log.info("some files in tar archive '{}' have not yet been archived to tape", tarId);
                transferItemService.updateCheckingProgressResults(tarId, TransferItem.TransferStatus.OCFLTARCREATED);
            }
        }
        catch (IOException | InterruptedException e) {
            log.error("an error occurred while checking archiving status", e);

            // in case it fails to check, still set the transfer status to OCFLTARCREATED and reset the checking flag
            transferItemService.updateCheckingProgressResults(tarId, TransferItem.TransferStatus.OCFLTARCREATED);
        }
    }

    public boolean isCompletelyArchived(Map<String, ArchiveStatusService.FileStatus> statusMap) {
        for (var entry : statusMap.entrySet()) {
            log.debug("file entry '{}' has status {}", entry.getKey(), entry.getValue());

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
            "tarId='" + tarId + '\'' +
            ", workingDir='" + workingDir + '\'' +
            ", dataArchiveRoot='" + dataArchiveRoot + '\'' +
            '}';
    }
}
