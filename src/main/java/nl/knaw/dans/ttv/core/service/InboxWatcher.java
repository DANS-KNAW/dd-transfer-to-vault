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
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

@Slf4j
public class InboxWatcher extends FileAlterationListenerAdaptor implements Managed {

    private final Path path;

    private final Callback callback;
    private final int interval;
    private final String datastationName;
    private FileAlterationMonitor monitor;

    public InboxWatcher(@NotNull Path path, @NotNull String datastationName, @NotNull Callback callback, @Min(1) int interval) {
        this.path = path;
        this.datastationName = datastationName;
        this.callback = callback;
        this.interval = interval;
    }

    @Override
    public void start() throws Exception {
        log.info("Starting InboxWatcher for path '{}'", this.path);

        try {
            log.info("Starting file alteration monitor for path '{}'", this.path);
            startFileAlterationMonitor();
        }
        catch (IOException e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void stop() throws Exception {
        monitor.stop();
    }

    @Override
    public void onFileCreate(File file) {
        this.callback.onFileCreate(file, datastationName);
    }

    private void startFileAlterationMonitor() throws Exception {
        var filters = FileFilterUtils.and(
            FileFilterUtils.fileFileFilter(),
            new IOFileFilter() {

                @Override
                public boolean accept(File file) {
                    var filePath = file.toPath();
                    return filePath.toAbsolutePath().getParent().equals(path.toAbsolutePath());
                }

                @Override
                public boolean accept(File dir, String name) {
                    return dir.toPath().toAbsolutePath().equals(path.toAbsolutePath());
                }
            }
        );

        var observer = new NonInitializedFileAlterationObserver(path.toFile(), filters);
        observer.addListener(this);

        monitor = new FileAlterationMonitor(this.interval);
        monitor.addObserver(observer);
        monitor.start();
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

    public static class NonInitializedFileAlterationObserver extends FileAlterationObserver {

        public NonInitializedFileAlterationObserver(File file, IOFileFilter filters) {
            super(file, filters);
        }

        public void checkAndNotify() {
            super.checkAndNotify();
        }

        @Override
        public void initialize() {
            // do nothing
        }
    }
}
