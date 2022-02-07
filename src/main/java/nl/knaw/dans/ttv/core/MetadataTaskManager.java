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
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

public class MetadataTaskManager implements Managed {
    private static final Logger log = LoggerFactory.getLogger(MetadataTaskManager.class);
    private final Path inbox;
    private final Path outbox;
    private final long pollingInterval;
    private final ExecutorService executorService;
    private final TransferItemService transferItemService;
    private final TransferItemMetadataReader metadataReader;
    private final FileService fileService;
    private final InboxWatcherFactory inboxWatcherFactory;
    private InboxWatcher inboxWatcher;

    public MetadataTaskManager(Path inbox, Path outbox, long pollingInterval, ExecutorService executorService,
        TransferItemService transferItemService, TransferItemMetadataReader metadataReader, FileService fileService, InboxWatcherFactory inboxWatcherFactory) {

        this.inbox = Objects.requireNonNull(inbox);
        this.outbox = Objects.requireNonNull(outbox);
        this.pollingInterval = pollingInterval;
        this.executorService = Objects.requireNonNull(executorService);
        this.transferItemService = Objects.requireNonNull(transferItemService);
        this.metadataReader = Objects.requireNonNull(metadataReader);
        this.fileService = Objects.requireNonNull(fileService);
        this.inboxWatcherFactory = Objects.requireNonNull(inboxWatcherFactory);
    }

    @Override
    public void start() throws Exception {
        // scan inboxes
        log.info("creating InboxWatcher's for configured inboxes");

        this.inboxWatcher = inboxWatcherFactory.getInboxWatcher(inbox, null, this::onFileAdded, pollingInterval);
        this.inboxWatcher.start();
    }

    public void onFileAdded(File file, String datastationName) {
        if (file.isFile() && file.getName().toLowerCase(Locale.ROOT).endsWith(".zip")) {
            var transferTask = new MetadataTask(
                file.toPath(), outbox, transferItemService, metadataReader, fileService
            );

            executorService.execute(transferTask);
        }
    }

    @Override
    public void stop() throws Exception {
        this.inboxWatcher.stop();
    }
}
