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
import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
    private FileAlterationMonitor monitor;

    private final ExecutorService executorService;

    public InboxWatcher(Inbox inbox, ExecutorService executorService) throws IOException {
        this.inbox = inbox;
        this.executorService = executorService;
    }

    private void startFileAlterationMonitor() throws Exception {
        FileAlterationObserver observer = new FileAlterationObserver(inbox.getDatastationInbox().toFile());
        monitor = new FileAlterationMonitor(500);
        FileAlterationListener listener = new FileAlterationListenerAdaptor() {
            @Override
            public void onFileCreate(File file) {
                if (file.isFile() && file.getName().endsWith(".zip")) {
                    Task task = inbox.createTransferItemTask(Path.of(file.getAbsolutePath()));
                    executorService.execute(task);
                }
            }
        };
        observer.addListener(listener);
        monitor.addObserver(observer);
        monitor.start();
    }

    @Override
    public void start() throws Exception {
        try {
            startFileAlterationMonitor();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
            throw new InvalidTransferItemException(e.getMessage());
        }
    }

    @Override
    public void stop() throws Exception {
        monitor.stop();
    }
}
