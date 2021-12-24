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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.ExecutorService;

public class InboxWatcher implements Managed {

    private static final Logger log = LoggerFactory.getLogger(InboxWatcher.class);

    private final Inbox inbox;
    private final ExecutorService executorService;
    private final WatchService watchService;

    private boolean running = true;

    public InboxWatcher(Inbox inbox, ExecutorService executorService) throws IOException {
        this.inbox = inbox;
        this.executorService = executorService;
        this.watchService = FileSystems.getDefault().newWatchService();
    }

    private void startWatchService() throws IOException, InterruptedException {
        inbox.getDatastationInbox().register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        WatchKey key;
        while (running) {
            key = watchService.take();
            key.pollEvents().stream()
                    .map(watchEvent -> ((Path) watchEvent.context()))
                    .filter(path -> path.getFileName().toString().endsWith(".zip"))
                    .map(path -> inbox.createTransferItemTask(inbox.getDatastationInbox().resolve(path.getFileName())))
                            .forEach(executorService::execute);
            key.reset();
        }
        watchService.close();
    }

    @Override
    public void start() throws Exception {
        try {
            startWatchService();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
            throw new InvalidTransferItemException(e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        //TODO?
        // Something like this? (Haven't tested it)
        running = false;
    }
}
