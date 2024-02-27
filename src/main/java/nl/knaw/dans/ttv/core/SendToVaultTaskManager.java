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
import nl.knaw.dans.datavault.client.resources.DefaultApi;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.TransferItemDao;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@RequiredArgsConstructor
public class SendToVaultTaskManager implements Managed {
    @NonNull
    private final Path inbox;

    @NonNull
    private final Path vaultInbox;
    private final long pollingInterval;

    @NonNull
    private final FileService fileService;

    @NonNull
    private final TransferItemService transferItemService;

    @NonNull
    private final TransferItemMetadataReader metadataReader;

    @NonNull
    private final Path currentBatchPath;

    private final long threshold;

    @NonNull
    private final DefaultApi vaultApi;

    @NonNull
    private final InboxWatcherFactory inboxWatcherFactory;

    private final ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
        Thread thread = new Thread(r);
        thread.setName("send-to-vault-task-worker");
        return thread;
    });

    private InboxWatcher inboxWatcher;

    @Override
    public void start() throws Exception {
        log.debug("creating InboxWatcher for inbox");
        inboxWatcher = inboxWatcherFactory.getInboxWatcher(inbox, null, (path, datastationName) -> {
            log.debug("File added: {}", path);
            executor.execute(new SendToVaultTask(
                fileService,
                transferItemService,
                metadataReader,
                path.toPath(),
                currentBatchPath,
                threshold,
                vaultInbox,
                vaultApi));
        }, pollingInterval);
        inboxWatcher.start();
    }

    @Override
    public void stop() throws Exception {
        log.debug("Stopping InboxWatcher for inbox");
        inboxWatcher.stop();
        executor.shutdown();
    }
}

