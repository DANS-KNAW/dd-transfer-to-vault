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

import io.dropwizard.hibernate.UnitOfWork;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

public class TarThresholdManager implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TarThresholdManager.class);

    private final BlockingQueue<TarEventQueueMessage> eventQueue;
    private final TransferItemDAO transferItemDAO;
    private final OcflRepositoryManager ocflRepositoryManager;
    private final String inboxPath;
    private final SessionFactory sessionFactory;
    private final ExecutorService executorService;

    private final long inboxThreshold;
    private final String workDir;
    private final String dataArchiveRoot;
    private final String tarCommand;

    public TarThresholdManager(String inboxPath, BlockingQueue<TarEventQueueMessage> eventQueue, SessionFactory sessionFactory, TransferItemDAO transferItemDAO,
        OcflRepositoryManager ocflRepositoryManager, ExecutorService executorService, long inboxThreshold, String workDir, String dataArchiveRoot, String tarCommand) {
        this.inboxPath = inboxPath;
        this.eventQueue = eventQueue;
        this.transferItemDAO = transferItemDAO;
        this.ocflRepositoryManager = ocflRepositoryManager;
        this.sessionFactory = sessionFactory;
        this.executorService = executorService;
        this.inboxThreshold = inboxThreshold;
        this.workDir = workDir;
        this.dataArchiveRoot = dataArchiveRoot;
        this.tarCommand = tarCommand;
    }

    @Override
    public void run() {
        while (true) {
            try {
                var event = this.eventQueue.take();

                log.debug("received event on eventQueue with value {}", event);

                switch (event) {
                    case STOP:
                        return;

                    case CHECK:
                        this.checkArchiveStatus();
                        break;
                }

            }
            catch (InterruptedException | IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    @UnitOfWork
    private void checkArchiveStatus() throws IOException {
        var repositorySize = calculateFileSize(Path.of(inboxPath));
        var repositoryMaxAge = 1L; // in seconds

        log.debug("repository size is {} bytes, age is {} seconds", repositorySize, repositoryMaxAge);

        // TODO make this actually check the right things
        if (repositorySize > inboxThreshold || repositoryMaxAge > 7 * 100) {
            log.info("thresholds reached, starting tar archiving task");

            var task = new UnitOfWorkAwareProxyFactory("UnitOfWorkProxy", this.sessionFactory)
                .create(TarTask.class,
                    new Class[] { TransferItemDAO.class, OcflRepositoryManager.class, String.class, String.class, String.class, String.class },
                new Object[] { transferItemDAO, ocflRepositoryManager, inboxPath, workDir, dataArchiveRoot, tarCommand });

            log.trace("new task created: {}", task);
            executorService.execute(task);
        }
    }

    private long calculateFileSize(Path startingPath) throws IOException {
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
}
