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

import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ttv.core.service.ArchiveStatusService;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.VaultCatalogService;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class ConfirmArchivedTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(ConfirmArchivedTaskManager.class);
    private final String schedule;
    private final Path workingDir;
    private final ExecutorService executorService;
    private final TransferItemService transferItemService;
    private final ArchiveStatusService archiveStatusService;
    private final FileService fileService;
    private final VaultCatalogService vaultCatalogService;
    private Scheduler scheduler;

    public ConfirmArchivedTaskManager(String schedule, Path workingDir,
        ExecutorService executorService, TransferItemService transferItemService, ArchiveStatusService archiveStatusService, FileService fileService,
        VaultCatalogService vaultCatalogService) {

        this.workingDir = workingDir;
        this.executorService = executorService;
        this.schedule = schedule;
        this.transferItemService = transferItemService;
        this.archiveStatusService = archiveStatusService;
        this.fileService = fileService;
        this.vaultCatalogService = vaultCatalogService;
    }

    @Override
    public void start() throws Exception {
        log.info("Verifying archive status");
        verifyArchives();

        this.scheduler = createScheduler();

        log.debug("Configuring JobDataMap for cron-based tasks");
        var params = new ConfirmArchivedTaskCreator.ConfirmArchivedTaskCreatorParameters(
            transferItemService, workingDir, archiveStatusService, fileService, executorService,
            vaultCatalogService);
        var jobData = new JobDataMap(Map.of("params", params));

        var job = JobBuilder.newJob(ConfirmArchivedTaskCreator.class)
            .withIdentity("job", "group")
            .setJobData(jobData)
            .build();

        var trigger = TriggerBuilder.newTrigger()
            .withIdentity("trigger", "group")
            .withSchedule(CronScheduleBuilder.cronSchedule(this.schedule))
            .build();

        log.info("Scheduling ConfirmArchivedTaskCreator with schedule '{}'", this.schedule);

        this.scheduler.scheduleJob(job, trigger);
        this.scheduler.start();

        log.info("Trigger ConfirmArchivedTaskCreator task immediately after startup");
        this.scheduler.triggerJob(job.getKey(), jobData);
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping scheduler");
        this.scheduler.shutdown();
        this.executorService.shutdownNow();
    }

    void verifyArchives() {
        var inProgress = transferItemService.findTarsByConfirmInProgress();

        for (var tar : inProgress) {
            log.warn("Found TAR with confirmCheckInProgress, either the application was interrupted during a check or there is an error: {}", tar);
            log.info("Resetting tar status for TAR {}", tar);
            transferItemService.resetTarToArchiving(tar);
        }
    }

    Scheduler createScheduler() throws SchedulerException {
        return new StdSchedulerFactory().getScheduler();
    }
}
