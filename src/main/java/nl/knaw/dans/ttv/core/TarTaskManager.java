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

import edu.wisc.library.ocfl.api.OcflRepository;
import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

public class TarTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(TarTaskManager.class);

    private final ExecutorService executorService;
    private final Path inboxPath;
    private final Path workDir;
    private final long inboxThreshold;
    private final InboxWatcherFactory inboxWatcherFactory;
    private final FileService fileService;
    private final OcflRepositoryService ocflRepositoryService;
    private final TransferItemService transferItemService;
    private final TarCommandRunner tarCommandRunner;
    private final long pollingInterval;
    private final ArchiveMetadataService archiveMetadataService;
    private InboxWatcher inboxWatcher;

    public TarTaskManager(Path inboxPath, Path workDir, long inboxThreshold, long pollingInterval, ExecutorService executorService,
        InboxWatcherFactory inboxWatcherFactory,
        FileService fileService, OcflRepositoryService ocflRepositoryService, TransferItemService transferItemService, TarCommandRunner tarCommandRunner,
        ArchiveMetadataService archiveMetadataService) {
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
    }

    @Override
    public void start() throws Exception {
        // start up file watcher
        log.info("starting watch on inbox {}", this.inboxPath);

        this.inboxWatcher = inboxWatcherFactory.getInboxWatcher(this.inboxPath, null, (file, ds) -> {
            log.trace("received InboxWatcher event for file {}", file);
            this.onNewItemInInbox(file);
        }, pollingInterval);

        this.inboxWatcher.start();
    }

    public void onNewItemInInbox(File file) {
        log.info("New item in inbox, filename is {}", file);
        log.info("Checking total size of inbox located at {}", inboxPath);

        try {
            var totalSize = fileService.getPathSize(inboxPath);
            var uuid = UUID.randomUUID();
            log.debug("Total size of inbox is {}, threshold is {}", totalSize, inboxThreshold);

            if (totalSize >= inboxThreshold) {
                log.info("Threshold reached, creating OCFL repo; size of inbox is {} bytes, threshold is {} bytes", totalSize, inboxThreshold);
                var ocflRepo = createOcflRepo(uuid);
                moveAllInboxFilesToOcflRepo(ocflRepo, uuid);
                startTarringTask(uuid);
            }
        }
        catch (IOException e) {
            log.error("error calculating file size", e);
        }
    }

    private void startTarringTask(UUID uuid) {
        var repoPath = Path.of(workDir.toString(), uuid.toString());
        var task = new TarTask(transferItemService, uuid, repoPath, tarCommandRunner, archiveMetadataService);

        executorService.execute(task);
    }

    public void moveAllInboxFilesToOcflRepo(OcflRepository ocflRepository, UUID uuid) throws IOException {
        // create a tar record with all COLLECTED TransferItem's in it
        var tarArchive = transferItemService.createTarArchiveWithAllCollectedTransferItems(uuid);

        // import them into the OCFL repo
        for (var transferItem : tarArchive.getTransferItems()) {
            var objectId = ocflRepositoryService.importTransferItem(ocflRepository, transferItem);
            transferItem.setAipTarEntryName(objectId);
        }

        // persist items
        transferItemService.save(tarArchive);

        // close repository
        ocflRepositoryService.closeOcflRepository(ocflRepository);
    }

    public OcflRepository createOcflRepo(UUID uuid) throws IOException {
        return ocflRepositoryService.createRepository(workDir, uuid.toString());
    }

    @Override
    public void stop() throws Exception {
        this.inboxWatcher.stop();
    }
}
