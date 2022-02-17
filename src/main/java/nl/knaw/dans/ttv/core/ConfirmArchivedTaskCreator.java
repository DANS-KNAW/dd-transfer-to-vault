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
        var ocflRepositoryService = params.getOcflRepositoryService();
        var executorService = params.getExecutorService();

        var tars = transferItemService.stageAllTarsToBeConfirmed();

        for (var tar : tars) {
            var task = new ConfirmArchivedTask(tar, transferItemService, archiveStatusService, ocflRepositoryService, workingDir);
            log.debug("Executing task {}", task);
            executorService.execute(task);
        }
    }

    public static class ConfirmArchivedTaskCreatorParameters {
        private final TransferItemService transferItemService;
        private final Path workingDir;
        private final ArchiveStatusService archiveStatusService;
        private final OcflRepositoryService ocflRepositoryService;
        private final ExecutorService executorService;

        public ConfirmArchivedTaskCreatorParameters(TransferItemService transferItemService, Path workingDir, ArchiveStatusService archiveStatusService,
            OcflRepositoryService ocflRepositoryService, ExecutorService executorService) {
            this.transferItemService = transferItemService;
            this.workingDir = workingDir;
            this.archiveStatusService = archiveStatusService;
            this.ocflRepositoryService = ocflRepositoryService;
            this.executorService = executorService;
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

        public OcflRepositoryService getOcflRepositoryService() {
            return ocflRepositoryService;
        }

        public ExecutorService getExecutorService() {
            return executorService;
        }

    }
}
