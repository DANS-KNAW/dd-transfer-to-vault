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
import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TransferItem;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class OcflTarTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(OcflTarTaskManager.class);

    private final ExecutorService executorService;
    private final Path inboxPath;
    private final Path workDir;
    private final String vaultPath;
    private final long inboxThreshold;
    private final InboxWatcherFactory inboxWatcherFactory;
    private final FileService fileService;
    private final OcflRepositoryService ocflRepositoryService;
    private final TransferItemService transferItemService;
    private final TarCommandRunner tarCommandRunner;
    private final long pollingInterval;
    private final int maxRetries;
    private final Duration retryInterval;
    private final List<Duration> retrySchedule;
    private final ArchiveMetadataService archiveMetadataService;
    private InboxWatcher inboxWatcher;
    private Scheduler retryScheduler;

    public OcflTarTaskManager(Path inboxPath, Path workDir, String vaultPath, long inboxThreshold, long pollingInterval, int maxRetries, Duration retryInterval, List<Duration> retrySchedule,
        ExecutorService executorService,
        InboxWatcherFactory inboxWatcherFactory, FileService fileService, OcflRepositoryService ocflRepositoryService, TransferItemService transferItemService, TarCommandRunner tarCommandRunner,
        ArchiveMetadataService archiveMetadataService) {
        this.vaultPath = vaultPath;
        this.retryInterval = retryInterval;
        this.executorService = executorService;
        this.inboxPath = inboxPath;
        this.workDir = workDir;
        this.inboxThreshold = inboxThreshold;
        this.inboxWatcherFactory = inboxWatcherFactory;
        this.fileService = fileService;
        this.ocflRepositoryService = ocflRepositoryService;
        this.transferItemService = transferItemService;
        this.tarCommandRunner = tarCommandRunner;
        this.pollingInterval = pollingInterval;
        this.archiveMetadataService = archiveMetadataService;
        this.maxRetries = maxRetries;
        this.retrySchedule = retrySchedule;
    }

    @Override
    public void start() throws Exception {
        // verify inbox first
        log.info("Verifying inbox '{}'", this.inboxPath);
        verifyInbox();

        log.info("Starting up retry-scheduler for failed archives");
        startRetryScheduler();

        // start up file watcher
        log.info("Starting watch on inbox '{}'", this.inboxPath);

        this.inboxWatcher = inboxWatcherFactory.getInboxWatcher(this.inboxPath, null, (file, ds) -> {
            log.debug("Received InboxWatcher event for file '{}'", file);
            this.onNewItemInInbox(file);
        }, pollingInterval);

        this.inboxWatcher.start();
    }

    @Override
    public void stop() throws Exception {
        this.inboxWatcher.stop();
        this.executorService.shutdownNow();
        this.retryScheduler.shutdown();
    }

    void verifyInbox() throws IOException {
        var tars = transferItemService.findTarsByStatusTarring();

        for (var tar : tars) {
            log.info("Resetting status for TAR {}", tar);
            moveInboxFilesToWorkdir(tar);
        }
    }

    void startRetryScheduler() throws SchedulerException {
        this.retryScheduler = createScheduler();

        log.info("Configuring JobDataMap for cron-based tasks");
        var jobParams = new OcflTarRetryTaskCreator.TaskRetryTaskCreatorParameters(
            transferItemService, workDir, tarCommandRunner, archiveMetadataService, executorService, maxRetries, retrySchedule, ocflRepositoryService
        );
        var jobData = new JobDataMap(Map.of("params", jobParams));

        var job = JobBuilder.newJob(OcflTarRetryTaskCreator.class)
            .withIdentity("tarRetryTask", "createocfltar")
            .setJobData(jobData)
            .build();

        var schedule = SimpleScheduleBuilder.simpleSchedule()
            .withIntervalInSeconds((int) retryInterval.getSeconds())
            .repeatForever();

        var trigger = TriggerBuilder.newTrigger()
            .withIdentity("tarRetryTaskTrigger", "createocfltar")
            .withSchedule(schedule)
            .build();

        log.info("Scheduling TarRetryTaskCreator with interval {}", retryInterval);

        this.retryScheduler.scheduleJob(job, trigger);
        this.retryScheduler.start();
    }

    void onNewItemInInbox(File file) {
        log.info("New item in inbox, filename is {}", file);
        log.info("Checking total size of inbox located at {}", inboxPath);

        try {
            var totalSize = fileService.getPathSize(inboxPath);
            var uuid = UUID.randomUUID().toString();
            log.debug("Total size of inbox is {} bytes, threshold is {} bytes", totalSize, inboxThreshold);

            if (totalSize >= inboxThreshold) {
                log.info("Threshold reached, moving files to workdir; size of inbox is {} bytes, threshold is {} bytes", totalSize, inboxThreshold);
                createTarArchive(uuid);
                startTarringTask(uuid);
            }
        }
        catch (IOException e) {
            log.error("Error calculating file size for path '{}'", inboxPath, e);
        }
    }

    void startTarringTask(String uuid) {
        var repoPath = Path.of(workDir.toString(), uuid);
        var task = new OcflTarTask(transferItemService, uuid, repoPath, tarCommandRunner, archiveMetadataService, ocflRepositoryService, maxRetries);

        log.info("Starting OcflTarTask {}", task);
        executorService.execute(task);
    }

    void createTarArchive(String uuid) throws IOException {
        // create a tar record with all COLLECTED TransferItem's in it
        var tarArchive = transferItemService.createTarArchiveWithAllMetadataExtractedTransferItems(uuid, vaultPath);

        moveInboxFilesToWorkdir(tarArchive);
    }

    void moveInboxFilesToWorkdir(Tar tarArchive) throws IOException {
        // move to workdir
        var targetDirBase = workDir.resolve(tarArchive.getTarUuid());
        var targetDirDve = targetDirBase.resolve("dve");

        fileService.ensureDirectoryExists(targetDirDve);

        for (var transferItem : tarArchive.getTransferItems()) {
            // the file could be in the inbox or in the working directory
            // in the first case, it has to be moved first
            // in the second case it does not need to be moved
            var filename = Path.of(transferItem.getDveFilePath()).getFileName();

            var expectedPath = inboxPath.resolve(filename);
            var targetPath = targetDirDve.resolve(filename);

            if (fileService.exists(expectedPath)) {
                log.info("Moving DVE file '{}' to workdir location '{}'", expectedPath, tarArchive);

                transferItemService.moveTransferItem(transferItem, TransferItem.TransferStatus.TARRING, targetPath);
                fileService.moveFile(expectedPath, targetPath);
            } else {
                log.warn("File '{}' cannot be found for TransferItem {}", expectedPath, transferItem);
            }
        }
    }

    Scheduler createScheduler() throws SchedulerException {
        return new StdSchedulerFactory().getScheduler();
    }
}
