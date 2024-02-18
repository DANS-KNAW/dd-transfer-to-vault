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
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class ConfirmArchivedTaskCreator implements Job {
    private static final Logger log = LoggerFactory.getLogger(ConfirmArchivedTaskCreator.class);

    @Override
    public void execute(JobExecutionContext context) {
        var dataMap = context.getMergedJobDataMap();
        var params = (ConfirmArchivedTaskCreatorParameters) dataMap.get("params");

        run(params);
    }

    void run(ConfirmArchivedTaskCreatorParameters params) {
        var transferItemService = params.getTransferItemService();
        var workingDir = params.getWorkingDir();
        var archiveStatusService = params.getArchiveStatusService();
        var fileService = params.getFileService();
        var executorService = params.getExecutorService();
        var vaultCatalogService = params.getVaultCatalogService();

        var tars = transferItemService.stageAllTarsToBeConfirmed();

        for (var tar : tars) {
            var task = new ConfirmArchivedTask(tar, transferItemService, archiveStatusService, fileService, workingDir, vaultCatalogService);
            log.debug("Executing task {}", task);
            executorService.execute(task);
        }
    }

    public static class ConfirmArchivedTaskCreatorParameters {
        private final TransferItemService transferItemService;
        private final Path workingDir;
        private final ArchiveStatusService archiveStatusService;
        private final FileService fileService;
        private final ExecutorService executorService;
        private final VaultCatalogClient vaultCatalogClient;

        public ConfirmArchivedTaskCreatorParameters(TransferItemService transferItemService, Path workingDir, ArchiveStatusService archiveStatusService,
            FileService fileService, ExecutorService executorService, VaultCatalogClient vaultCatalogClient) {
            this.transferItemService = transferItemService;
            this.workingDir = workingDir;
            this.archiveStatusService = archiveStatusService;
            this.fileService = fileService;
            this.executorService = executorService;
            this.vaultCatalogClient = vaultCatalogClient;
        }

        public VaultCatalogClient getVaultCatalogService() {
            return vaultCatalogClient;
        }

        public TransferItemService getTransferItemService() {
            return transferItemService;
        }

        public Path getWorkingDir() {
            return workingDir;
        }

        public ArchiveStatusService getArchiveStatusService() {
            return archiveStatusService;
        }

        public FileService getFileService() {
            return fileService;
        }

        public ExecutorService getExecutorService() {
            return executorService;
        }

    }
}
