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
package nl.knaw.dans.ttv.core.service;

import io.dropwizard.lifecycle.Managed;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public class InboxWatcher extends FileAlterationListenerAdaptor implements Managed {

    private static final Logger log = LoggerFactory.getLogger(InboxWatcher.class);
    private final Path path;
    private final Callback callback;
    private final int interval;
    private final String datastationName;
    private FileAlterationMonitor monitor;

    public InboxWatcher(Path path, String datastationName, Callback callback, int interval) {
        this.path = Objects.requireNonNull(path, "InboxWatcher path must not be null");
        this.datastationName = datastationName;
        this.callback = Objects.requireNonNull(callback, "InboxWatcher callback must not be null");
        this.interval = Objects.requireNonNullElse(interval, 500);
    }

    @Override
    public void onFileCreate(File file) {
        // see if file is a direct descendant of path
        // if not, ignore it
        var filePath = file.toPath().toAbsolutePath();

        log.debug("Checking if file '{}' is a child of '{}'", filePath, this.path.toAbsolutePath());

        if (!filePath.startsWith(this.path.toAbsolutePath())) {
            log.warn("File found in non-root directory, ignoring");
            return;
        }

        this.callback.onFileCreate(file, datastationName);
    }

    private void startFileAlterationMonitor() throws Exception {
        FileAlterationObserver observer = new FileAlterationObserver(path.toFile());
        observer.addListener(this);

        monitor = new FileAlterationMonitor(this.interval);
        monitor.addObserver(observer);
        monitor.start();
    }

    @Override
    public void start() throws Exception {
        try {
            // initial scan
            log.info("Scanning path '{}' for first run", this.path);
            scanExistingFiles();

            log.info("Starting file alteration monitor for path '{}'", this.path);
            startFileAlterationMonitor();
        }
        catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
            throw new InvalidTransferItemException(e.getMessage());
        }
    }

    private void scanExistingFiles() throws IOException {
        Files.list(this.path).forEach(f -> onFileCreate(f.toFile()));
    }

    @Override
    public void stop() throws Exception {
        monitor.stop();
    }

    @Override
    public String toString() {
        return "InboxWatcher{" +
            "path=" + path +
            ", interval=" + interval +
            ", datastationName='" + datastationName + '\'' +
            '}';
    }

    public interface Callback {
        void onFileCreate(File file, String datastationName);
    }
}
