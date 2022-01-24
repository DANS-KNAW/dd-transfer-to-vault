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

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import io.dropwizard.hibernate.UnitOfWork;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class TarTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(TarTaskManager.class);

    private final SessionFactory sessionFactory;
    private final TransferItemDAO transferItemDAO;
    private final ExecutorService executorService;
    private final String inboxPath;
    private final String workDir;
    private final long inboxThreshold;
    private final String tarCommand;
    private final String dataArchiveRoot;
    private final OcflRepositoryFactory ocflRepositoryFactory;
    private InboxWatcher inboxWatcher;

    public TarTaskManager(
        String inboxPath,
        String workDir,
        long inboxThreshold,
        String tarCommand,
        String dataArchiveRoot,
        TransferItemDAO transferItemDAO,
        SessionFactory sessionFactory,
        ExecutorService executorService,
        OcflRepositoryFactory ocflRepositoryFactory
    ) {
        this.sessionFactory = sessionFactory;
        this.transferItemDAO = transferItemDAO;
        this.executorService = executorService;
        this.inboxPath = inboxPath;
        this.workDir = workDir;
        this.inboxThreshold = inboxThreshold;
        this.tarCommand = tarCommand;
        this.dataArchiveRoot = dataArchiveRoot;
        this.ocflRepositoryFactory = ocflRepositoryFactory;
    }

    @Override
    public void start() throws Exception {
        // start up file watcher
        log.info("starting watch on inbox {}", this.inboxPath);

        this.inboxWatcher = new InboxWatcher(Path.of(this.inboxPath), null, (file, ds) -> {
            log.trace("received InboxWatcher event for file {}", file);
            this.onNewItemInInbox(file);
        }, 1000);

        this.inboxWatcher.start();
    }

    public void onNewItemInInbox(File file) {
        log.info("new item in inbox, filename is {}", file);
        log.info("checking total size of inbox located at {}", inboxPath);

        try {
            var totalSize = calculateFileSize(Path.of(inboxPath));
            var uuid = UUID.randomUUID();
            log.debug("total size of inbox is {}, threshold is {}", totalSize, inboxThreshold);

            if (totalSize >= 1) {
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

        var task = new UnitOfWorkAwareProxyFactory("UnitOfWorkProxy", this.sessionFactory)
            .create(TarTask.class,
                new Class[] { TransferItemDAO.class, UUID.class, Path.class, String.class, String.class },
                new Object[] { transferItemDAO, uuid, repoPath, dataArchiveRoot, tarCommand });

        executorService.execute(task);
    }

    @UnitOfWork
    public void moveAllInboxFilesToOcflRepo(OcflRepository ocflRepository, UUID uuid) throws IOException {

        // TODO this is highly inefficient, fix later
        var transferItems = Files.list(Path.of(inboxPath)).map(path -> {
            var item = transferItemDAO.findByDvePath(path.toString());
            log.trace("TransferItem for path {}: {}", path, item);
            return item;
        }).collect(Collectors.toList());

        for (var transferItem : transferItems) {
            String bagId = Objects.requireNonNull(transferItem.getBagId(), "Bag ID can't be null" + transferItem.onError());
            String objectId = bagId.substring(0, 9) + bagId.substring(9, 11) + "/" + bagId.substring(11, 13) + "/" + bagId.substring(13, 15) + "/" + bagId.substring(15);
            Path source = Objects.requireNonNull(Path.of(transferItem.getDveFilePath()), "dveFilePath can't be null" + transferItem.onError());

            log.debug("adding object to ocfl repository with bagId {}, objectId {} and source path {}", bagId, objectId, source);
            ocflRepository.putObject(ObjectVersionId.head(objectId), source, new VersionInfo().setMessage("initial commit"), OcflOption.MOVE_SOURCE);

            transferItem.setTransferStatus(TransferItem.TransferStatus.TARRING);
            transferItem.setAipTarEntryName(objectId);
            transferItem.setAipsTar(uuid.toString());
        }

        ocflRepository.close();
    }

    public OcflRepository createOcflRepo(UUID uuid) throws IOException {
        var newPath = Files.createDirectory(Path.of(workDir, uuid.toString()));
        var newPathWorkdir = Files.createDirectory(Path.of(workDir, uuid.toString() + "-wd"));

        return ocflRepositoryFactory.createRepository(newPath, newPathWorkdir);
    }

    public long calculateFileSize(Path startingPath) throws IOException {
        return Files.walk(startingPath).filter(Files::isRegularFile).map(path -> {
            try {
                var size = Files.size(path);
                log.trace("file size for file {} is {} bytes", path, size);
                return size;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }).reduce(0L, Long::sum);
    }

    @Override
    public void stop() throws Exception {
        this.inboxWatcher.stop();
    }
}
