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
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class ConfirmArchivedTaskCreator implements Job {
    private static final Logger log = LoggerFactory.getLogger(ConfirmArchivedTaskCreator.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        var dataMap = context.getMergedJobDataMap();
        var transferItemService = (TransferItemService) dataMap.get("transferItemService");
        var workingDir = (String) dataMap.get("workingDir");
        var dataArchiveRoot = (String) dataMap.get("dataArchiveRoot");
        var archiveStatusService = (ArchiveStatusService) dataMap.get("archiveStatusService");
        var ocflRepositoryService = (OcflRepositoryService) dataMap.get("ocflRepositoryService");
        var executorService = (ExecutorService) dataMap.get("executorService");

        var tarIds = transferItemService.findAllTarsToBeConfirmed();

        for (var tarId: tarIds) {
            var task = new ConfirmArchivedTask(tarId, transferItemService, archiveStatusService, ocflRepositoryService, workingDir, dataArchiveRoot);
            log.debug("executing task {}", task);
            executorService.execute(task);
        }
    }
}
