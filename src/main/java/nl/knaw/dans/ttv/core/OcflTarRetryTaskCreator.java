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
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.Tar;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class OcflTarRetryTaskCreator implements Job {
    private static final Logger log = LoggerFactory.getLogger(OcflTarRetryTaskCreator.class);

    @Override
    public void execute(JobExecutionContext context) {
        var dataMap = context.getMergedJobDataMap();
        var params = (TaskRetryTaskCreatorParameters) dataMap.get("params");

        run(params);
    }

    void run(TaskRetryTaskCreatorParameters params) {
        var transferItemService = params.getTransferItemService();
        var workDir = params.getWorkDir();
        var tarCommandRunner = params.getTarCommandRunner();
        var archiveMetadataService = params.getArchiveMetadataService();
        var executorService = params.getExecutorService();
        var maxRetries = params.getMaxRetries();
        var retryIntervals = params.getRetryIntervals();
        var ocflRepositoryService = params.getOcflRepositoryService();
        var vaultCatalogService = params.getVaultCatalogService();

        // get a list of Tars that need to be retried
        var tars = transferItemService.findTarsToBeRetried();

        log.debug("Checking TAR's to retry, found {} candidates", tars.size());
        for (var tar : tars) {
            if (!shouldRetry(tar, retryIntervals)) {
                log.debug("Tar {} is not ready for retry yet", tar);
                continue;
            }

            log.info("Setting TAR {} to archiving in progress", tar);
            transferItemService.setArchivingInProgress(tar.getTarUuid());

            var repoPath = workDir.resolve(tar.getTarUuid());

            // check if tar should be retried again
            var task = new OcflTarTask(transferItemService, tar.getTarUuid(),
                repoPath, tarCommandRunner, archiveMetadataService, ocflRepositoryService, vaultCatalogService, maxRetries);

            log.info("Starting TarTask {}", task);
            executorService.execute(task);
        }
    }

    boolean shouldRetry(Tar tar, List<Duration> retryIntervals) {
        var attempt = tar.getTransferAttempt();
        var threshold = calculateThreshold(attempt, retryIntervals);
        var now = LocalDateTime.now();

        log.trace("Comparing date {} and {}", tar.getCreated(), now);
        var offset = Duration.between(tar.getCreated(), now);

        var result = offset.compareTo(threshold) >= 0;
        log.trace("Comparing offset {} and {}, result is {}", offset, threshold, result);
        return result;
    }

    Duration calculateThreshold(int attempt, List<Duration> retryDelays) {
        return attempt >= retryDelays.size()
            ? retryDelays.get(retryDelays.size() - 1).multipliedBy((attempt + 2) - retryDelays.size())
            : retryDelays.get(attempt);
    }

    public static class TaskRetryTaskCreatorParameters {
        private TransferItemService transferItemService;
        private Path workDir;
        private TarCommandRunner tarCommandRunner;
        private ArchiveMetadataService archiveMetadataService;
        private ExecutorService executorService;
        private OcflRepositoryService ocflRepositoryService;
        private VaultCatalogRepository vaultCatalogRepository;
        private int maxRetries;
        private List<Duration> retryIntervals;
        public TaskRetryTaskCreatorParameters(TransferItemService transferItemService, Path workDir, TarCommandRunner tarCommandRunner,
            ArchiveMetadataService archiveMetadataService, ExecutorService executorService, int maxRetries, List<Duration> retryIntervals, OcflRepositoryService ocflRepositoryService,
            VaultCatalogRepository vaultCatalogRepository) {
            this.transferItemService = transferItemService;
            this.workDir = workDir;
            this.tarCommandRunner = tarCommandRunner;
            this.archiveMetadataService = archiveMetadataService;
            this.executorService = executorService;
            this.maxRetries = maxRetries;
            this.retryIntervals = retryIntervals;
            this.ocflRepositoryService = ocflRepositoryService;
            this.vaultCatalogRepository = vaultCatalogRepository;
        }

        public VaultCatalogRepository getVaultCatalogService() {
            return vaultCatalogRepository;
        }

        public void setVaultCatalogService(VaultCatalogRepository vaultCatalogRepository) {
            this.vaultCatalogRepository = vaultCatalogRepository;
        }

        public OcflRepositoryService getOcflRepositoryService() {
            return ocflRepositoryService;
        }

        public void setOcflRepositoryService(OcflRepositoryService ocflRepositoryService) {
            this.ocflRepositoryService = ocflRepositoryService;
        }

        public TransferItemService getTransferItemService() {
            return transferItemService;
        }

        public void setTransferItemService(TransferItemService transferItemService) {
            this.transferItemService = transferItemService;
        }

        public Path getWorkDir() {
            return workDir;
        }

        public void setWorkDir(Path workDir) {
            this.workDir = workDir;
        }

        public TarCommandRunner getTarCommandRunner() {
            return tarCommandRunner;
        }

        public void setTarCommandRunner(TarCommandRunner tarCommandRunner) {
            this.tarCommandRunner = tarCommandRunner;
        }

        public ArchiveMetadataService getArchiveMetadataService() {
            return archiveMetadataService;
        }

        public void setArchiveMetadataService(ArchiveMetadataService archiveMetadataService) {
            this.archiveMetadataService = archiveMetadataService;
        }

        public ExecutorService getExecutorService() {
            return executorService;
        }

        public void setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public List<Duration> getRetryIntervals() {
            return retryIntervals;
        }

        public void setRetryIntervals(List<Duration> retryIntervals) {
            this.retryIntervals = retryIntervals;
        }
    }
}
