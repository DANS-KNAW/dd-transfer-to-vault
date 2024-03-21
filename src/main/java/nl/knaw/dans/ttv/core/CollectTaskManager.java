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
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.knaw.dans.ttv.config.CollectConfig;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class CollectTaskManager implements Managed {
    @NonNull
    private final List<CollectConfig.InboxEntry> inboxes;

    @NonNull
    private final Path outbox;
    private final long pollingInterval;

    @NonNull
    private final ExecutorService executorService;

    @NonNull
    private final TransferItemService transferItemService;

    @NonNull
    private final TransferItemMetadataReader metadataReader;

    @NonNull
    private final FileService fileService;

    @NonNull
    private final InboxWatcherFactory inboxWatcherFactory;
    private List<InboxWatcher> inboxWatchers;

    @Override
    public void start() throws Exception {
        log.debug("Creating InboxWatchers for configured inboxes");

        this.inboxWatchers = inboxes.stream().map(entry -> {
            log.debug("Creating InboxWatcher for {}", entry);

            try {
                return inboxWatcherFactory.createInboxWatcher(
                    entry.getPath(), entry.getName(), this::onFileAdded, this.pollingInterval
                );
            }
            catch (Exception e) {
                log.warn("Unable to create InboxWatcher {}. It will be ignored.", entry.getName(), e);
            }
            return null;
        }).collect(Collectors.toList());

        for (var inboxWatcher : this.inboxWatchers) {
            if (inboxWatcher != null) {
                log.debug("Starting InboxWatcher {}", inboxWatcher);
                inboxWatcher.start();
            }
        }
    }

    public void onFileAdded(File file, String datastationName) {
        log.debug("Received file creation event for file '{}' and datastation name '{}'", file, datastationName);
        try {
            var path = fileService.addCreationTimeToFileName(file.toPath());
            file = path.toFile();
        }
        catch (IOException e) {
            log.error("Unable to add creation time to file name", e);
            try {
                fileService.rejectFile(file.toPath(), e);
            }
            catch (IOException error) {
                log.error("Tried to move file to dead-letter box, but failed", error);
                return;
            }
        }

        if (FilenameUtils.getExtension(file.getName()).toLowerCase(Locale.ROOT).equals("zip")) {
            var collectTask = new CollectTask(
                file.toPath(), outbox, datastationName, transferItemService, metadataReader, fileService
            );

            log.debug("Executing task {}", collectTask);
            executorService.execute(collectTask);
        }
    }

    @Override
    public void stop() throws Exception {
        log.debug("Shutting down CollectTaskManager");

        for (var inboxWatcher : this.inboxWatchers) {
            if (inboxWatcher != null) {
                log.debug("Stopping InboxWatcher {}", inboxWatcher);
                inboxWatcher.stop();
            }
        }

        this.executorService.shutdownNow();
    }
}
