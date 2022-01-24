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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.hibernate.UnitOfWorkAwareProxyFactory;
import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ttv.config.CollectConfiguration;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.hibernate.SessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class CollectTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(CollectTaskManager.class);
    private final List<CollectConfiguration.InboxEntry> inboxes;
    private final Path outbox;
    private final SessionFactory sessionFactory;
    private final ExecutorService executorService;
    private final ObjectMapper objectMapper;
    private final TransferItemDAO transferItemDAO;
    private List<InboxWatcher> inboxWatchers;

    public CollectTaskManager(List<CollectConfiguration.InboxEntry> inboxes, String outbox, SessionFactory sessionFactory, ExecutorService executorService, ObjectMapper objectMapper,
        TransferItemDAO transferItemDAO) {
        // TODO add Objects.requiresNonNull etc
        this.inboxes = inboxes;
        this.outbox = Path.of(outbox);
        this.executorService = executorService;
        this.sessionFactory = sessionFactory;
        this.objectMapper = objectMapper;
        this.transferItemDAO = transferItemDAO;
    }

    @Override
    public void start() throws Exception {
        // scan inboxes
        log.info("scanning all inboxes for initial state");

        this.inboxWatchers = inboxes.stream().map(entry -> {
            try {
                return new InboxWatcher(Path.of(entry.getPath()), entry.getName(), this::onFileAdded, 500);
            }
            catch (Exception e) {
                log.error("unable to create InboxWatcher", e);
            }
            return null;
        }).collect(Collectors.toList());

        for (var inboxWatcher : this.inboxWatchers) {
            if (inboxWatcher != null) {
                inboxWatcher.start();
            }
        }
    }

    private void onFileAdded(File file, String datastationName) {
        if (file.isFile() && file.getName().endsWith(".zip")) {
            CollectTask transferTask = new UnitOfWorkAwareProxyFactory("UnitOfWorkProxy", sessionFactory)
                .create(CollectTask.class,
                    new Class[] { Path.class, Path.class, String.class, ObjectMapper.class, TransferItemDAO.class },
                    new Object[] { file.toPath(), outbox, datastationName, objectMapper, transferItemDAO });

            executorService.execute(transferTask);
        }
    }

    @Override
    public void stop() throws Exception {
        for (var inboxWatcher : this.inboxWatchers) {
            if (inboxWatcher != null) {
                inboxWatcher.stop();
            }
        }
    }
}
