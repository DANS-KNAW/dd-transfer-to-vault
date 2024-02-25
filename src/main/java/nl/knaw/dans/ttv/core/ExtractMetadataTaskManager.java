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
import nl.knaw.dans.ttv.client.VaultCatalogClient;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.TransferItemValidator;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

@RequiredArgsConstructor
@Slf4j
public class ExtractMetadataTaskManager implements Managed {

    @NonNull
    private final Path inbox;

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

    @NonNull
    private final TransferItemValidator transferItemValidator;

    @NonNull
    private final VaultCatalogClient vaultCatalogClient;

    private InboxWatcher inboxWatcher;

    @Override
    public void start() throws Exception {
        log.info("creating InboxWatcher's for configured inboxes");

        this.inboxWatcher = inboxWatcherFactory.getInboxWatcher(inbox, null, this::onFileAdded, pollingInterval);
        this.inboxWatcher.start();
    }

    public void onFileAdded(File file, String datastationName) {
        log.debug("Received file creation event for file '{}'", file);
        if (FilenameUtils.getExtension(file.getName()).toLowerCase(Locale.ROOT).equals("zip")) {
            var metadataTask = new ExtractMetadataTask(
                file.toPath(), outbox, transferItemService, metadataReader, fileService,
                transferItemValidator, vaultCatalogClient);

            log.debug("Executing task {}", metadataTask);
            executorService.execute(metadataTask);
        }
    }

    @Override
    public void stop() throws Exception {
        log.debug("Shutting down MetadataTaskManager");

        this.inboxWatcher.stop();
        this.executorService.shutdownNow();
    }
}
