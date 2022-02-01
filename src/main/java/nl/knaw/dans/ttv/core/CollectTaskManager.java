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
import nl.knaw.dans.ttv.core.config.CollectConfiguration;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class CollectTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(CollectTaskManager.class);
    private final List<CollectConfiguration.InboxEntry> inboxes;
    private final Path workDir;
    private final Path outbox;
    private final long pollingInterval;
    private final ExecutorService executorService;
    private final TransferItemService transferItemService;
    private final TransferItemMetadataReader metadataReader;
    private final FileService fileService;
    private final InboxWatcherFactory inboxWatcherFactory;
    private List<InboxWatcher> inboxWatchers;

    public CollectTaskManager(List<CollectConfiguration.InboxEntry> inboxes, Path workDir, Path outbox, long pollingInterval, ExecutorService executorService,
        TransferItemService transferItemService, TransferItemMetadataReader metadataReader, FileService fileService, InboxWatcherFactory inboxWatcherFactory) {

        this.inboxes = Objects.requireNonNull(inboxes);
        this.outbox = Objects.requireNonNull(outbox);
        this.pollingInterval = pollingInterval;
        this.executorService = Objects.requireNonNull(executorService);
        this.transferItemService = Objects.requireNonNull(transferItemService);
        this.metadataReader = Objects.requireNonNull(metadataReader);
        this.fileService = Objects.requireNonNull(fileService);
        this.inboxWatcherFactory = Objects.requireNonNull(inboxWatcherFactory);
        this.workDir = Objects.requireNonNull(workDir);
    }

    @Override
    public void start() throws Exception {
        // scan inboxes
        log.info("creating InboxWatcher's for configured inboxes");

        this.inboxWatchers = inboxes.stream().map(entry -> {
            log.debug("creating InboxWatcher for {}", entry);

            try {
                return inboxWatcherFactory.getInboxWatcher(
                    entry.getPath(), entry.getName(), this::onFileAdded, this.pollingInterval
                );
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

    public void onFileAdded(File file, String datastationName) {
        if (file.isFile() && file.getName().toLowerCase(Locale.ROOT).endsWith(".zip")) {
            var transferTask = new CollectTask(
                file.toPath(), workDir, outbox, datastationName, transferItemService, metadataReader, fileService
            );

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
