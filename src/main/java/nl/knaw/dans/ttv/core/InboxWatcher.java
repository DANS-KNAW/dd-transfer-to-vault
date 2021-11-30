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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class InboxWatcher implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(InboxWatcher.class);

    private final Inbox inbox;
    private final ExecutorService executorService;
    private final WatchService watchService;

    public InboxWatcher(Inbox inbox, ExecutorService executorService, WatchService watchService) {
        this.inbox = inbox;
        this.executorService = executorService;
        this.watchService = watchService;
    }

    private void startWatchService() throws IOException, InterruptedException {
        inbox.getPath().register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

        WatchKey key;
        while (true) {
            key = watchService.take();
            key.pollEvents().stream()
                    .map(watchEvent -> ((Path) watchEvent.context()))
                    .filter(path -> path.getFileName().toString().endsWith(".zip"))
                    .map(path -> inbox.createTransferItemTask(inbox.getPath().resolve(path.getFileName())))
                            .forEach(executorService::execute);
            key.reset();
        }
    }

    @Override
    public void run() {
        try {
            startWatchService();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }
}
