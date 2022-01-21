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

import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

public class TarThresholdWatcher implements Managed {
    private static final Logger log = LoggerFactory.getLogger(TarThresholdWatcher.class);

    private final BlockingQueue<TarEventQueueMessage> eventQueue = new LinkedBlockingQueue<>();
    private final OcflRepositoryManager ocflRepositoryManager;
    private final SessionFactory sessionFactory;
    private final TransferItemDAO transferItemDAO;
    private final ExecutorService executorService;
    private final String inboxPath;
    private final String workDir;
    private final long inboxThreshold;
    private final String tarCommand;
    private final String dataArchiveRoot;
    private CronCallbackTask cronCallbackTask;

    public TarThresholdWatcher(
        String inboxPath,
        String workDir,
        long inboxThreshold,
        String tarCommand,
        String dataArchiveRoot,
        OcflRepositoryManager ocflRepositoryManager,
        TransferItemDAO transferItemDAO,
        SessionFactory sessionFactory,
        ExecutorService executorService
    ) {
        this.ocflRepositoryManager = ocflRepositoryManager;
        this.sessionFactory = sessionFactory;
        this.transferItemDAO = transferItemDAO;
        this.executorService = executorService;
        this.inboxPath = inboxPath;
        this.workDir = workDir;
        this.inboxThreshold = inboxThreshold;
        this.tarCommand = tarCommand;
        this.dataArchiveRoot = dataArchiveRoot;
    }

    @Override
    public void start() throws Exception {
        // start up file watcher

        // start up timer
        // TODO set a proper schedule
        this.cronCallbackTask = new CronCallbackTask("* * * * *");
        this.cronCallbackTask.addCallback(() -> {
            // just tell the other process to check if it needs to do something
            log.debug("received cron callback message");
            this.eventQueue.add(TarEventQueueMessage.CHECK);
        });

        this.cronCallbackTask.start();

        // start the thread watching for events from the queue
        var tarThresholdManager = new UnitOfWorkAwareProxyFactory("UnitOfWorkProxy", this.sessionFactory)
            .create(TarThresholdManager.class,
                new Class[] { String.class, BlockingQueue.class, SessionFactory.class, TransferItemDAO.class, OcflRepositoryManager.class, ExecutorService.class, long.class, String.class,
                    String.class, String.class },
                new Object[] { inboxPath, eventQueue, sessionFactory, transferItemDAO, ocflRepositoryManager, executorService, inboxThreshold, workDir, dataArchiveRoot, tarCommand });

        // dont run it on the executorService as that one is reserved for the tarring tasks
        new Thread(tarThresholdManager).start();

        this.eventQueue.add(TarEventQueueMessage.CHECK);
    }

    @Override
    public void stop() throws Exception {
        this.eventQueue.add(TarEventQueueMessage.STOP);
        this.cronCallbackTask.stop();
    }
}
