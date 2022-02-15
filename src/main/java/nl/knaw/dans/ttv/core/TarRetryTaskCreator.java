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

import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.Tar;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ExecutorService;

public class TarRetryTaskCreator implements Job {
    private static final Logger log = LoggerFactory.getLogger(TarRetryTaskCreator.class);

    private final Duration[] RETRY_DELAYS = new Duration[] {
        Duration.of(1, ChronoUnit.MINUTES),
        Duration.of(1, ChronoUnit.HOURS),
        Duration.of(8, ChronoUnit.HOURS),
        Duration.of(24, ChronoUnit.HOURS),
        Duration.of(48, ChronoUnit.HOURS),
        Duration.of(72, ChronoUnit.HOURS),
    };

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        var dataMap = context.getMergedJobDataMap();
        var transferItemService = (TransferItemService) dataMap.get("transferItemService");
        var workDir = (Path) dataMap.get("workDir");
        var tarCommandRunner = (TarCommandRunner) dataMap.get("tarCommandRunner");
        var archiveMetadataService = (ArchiveMetadataService) dataMap.get("archiveMetadataService");
        var executorService = (ExecutorService) dataMap.get("executorService");

        // get a list of Tars that need to be retried
        var tars = transferItemService.findTarsToBeRetried();

        log.debug("Checking TAR's to retry, found {} candidates", tars.size());
        for (var tar : tars) {
            if (!shouldRetry(tar)) {
                log.debug("Tar {} is not ready for retry yet", tar);
                continue;
            }

            log.info("Setting TAR {} to archiving in progress", tar);
            transferItemService.setArchivingInProgress(tar.getTarUuid());

            // check if tar should be retried again
            var task = new TarTask(transferItemService, tar.getTarUuid(),
                workDir, tarCommandRunner, archiveMetadataService);

            log.info("Starting TarTask {}", task);
            executorService.execute(task);
        }
    }

    boolean shouldRetry(Tar tar) {
        var attempt = tar.getTransferAttempt();
        var threshold = calculateThreshold(attempt, RETRY_DELAYS);
        var now = LocalDateTime.now();

        log.trace("Comparing date {} and {}", tar.getCreated(), now);
        var offset = Duration.between(tar.getCreated(), now);

        var result = offset.compareTo(threshold) >= 0;
        log.trace("Comparing offset {} and {}, result is {}", offset, threshold, result);
        return result;
    }

    Duration calculateThreshold(int attempt, Duration[] retryDelays) {
        return attempt >= retryDelays.length
            ? retryDelays[retryDelays.length - 1].multipliedBy((attempt + 2) - retryDelays.length)
            : retryDelays[attempt];
    }
}
