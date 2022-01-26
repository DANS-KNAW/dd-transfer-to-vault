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
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.TransferItem;
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
    private final String inboxPath;
    private final String workDir;
    private final long inboxThreshold;
    private final String tarCommand;
    private final String dataArchiveRoot;
    private final InboxWatcherFactory inboxWatcherFactory;
    private final FileService fileService;
    private final OcflRepositoryService ocflRepositoryService;
    private final TransferItemService transferItemService;
    private final TarCommandRunner tarCommandRunner;
    private InboxWatcher inboxWatcher;

    public TarTaskManager(String inboxPath, String workDir, long inboxThreshold, String tarCommand, String dataArchiveRoot, ExecutorService executorService, InboxWatcherFactory inboxWatcherFactory,
        FileService fileService, OcflRepositoryService ocflRepositoryService, TransferItemService transferItemService, TarCommandRunner tarCommandRunner) {
        this.executorService = executorService;
        this.inboxPath = inboxPath;
        this.workDir = workDir;
        this.inboxThreshold = inboxThreshold;
        this.tarCommand = tarCommand;
        this.dataArchiveRoot = dataArchiveRoot;
        this.inboxWatcherFactory = inboxWatcherFactory;
        this.fileService = fileService;
        this.ocflRepositoryService = ocflRepositoryService;
        this.transferItemService = transferItemService;
        this.tarCommandRunner = tarCommandRunner;
    }

    @Override
    public void start() throws Exception {
        // start up file watcher
        log.info("starting watch on inbox {}", this.inboxPath);

        this.inboxWatcher = inboxWatcherFactory.getInboxWatcher(Path.of(this.inboxPath), null, (file, ds) -> {
            log.trace("received InboxWatcher event for file {}", file);
            this.onNewItemInInbox(file);
        }, 1000);

        this.inboxWatcher.start();
    }

    public void onNewItemInInbox(File file) {
        log.info("new item in inbox, filename is {}", file);
        log.info("checking total size of inbox located at {}", inboxPath);

        try {
            var totalSize = fileService.getPathSize(Path.of(inboxPath));
            var uuid = UUID.randomUUID();
            log.debug("total size of inbox is {}, threshold is {}", totalSize, inboxThreshold);

            if (totalSize >= inboxThreshold) {
                log.trace("threshold reached, creating OCFL repo");
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
        var repoPath = Path.of(workDir, uuid.toString());
        var task = new TarTask(transferItemService, uuid, repoPath, dataArchiveRoot, tarCommand, tarCommandRunner);

        executorService.execute(task);
    }

    public void moveAllInboxFilesToOcflRepo(OcflRepository ocflRepository, UUID uuid) throws IOException {
        // find all transfer items that we can deal with
        var transferItems = transferItemService.findByStatus(TransferItem.TransferStatus.COLLECTED);

        for (var transferItem : transferItems) {
            var objectId = ocflRepositoryService.importTransferItem(ocflRepository, transferItem);

            transferItem.setTransferStatus(TransferItem.TransferStatus.TARRING);
            transferItem.setAipTarEntryName(objectId);
            transferItem.setAipsTar(uuid.toString());
        }

        // persist items
        transferItemService.saveAll(transferItems);

        // close repository
        ocflRepositoryService.closeOcflRepository(ocflRepository);
    }

    public OcflRepository createOcflRepo(UUID uuid) throws IOException {
        return ocflRepositoryService.createRepository(Path.of(workDir), uuid.toString());
    }

    @Override
    public void stop() throws Exception {
        this.inboxWatcher.stop();
    }
}
