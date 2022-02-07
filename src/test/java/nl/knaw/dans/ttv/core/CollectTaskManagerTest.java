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

import nl.knaw.dans.ttv.core.config.CollectConfiguration;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.fail;

class CollectTaskManagerTest {
    private TransferItemService transferItemService;
    private TransferItemMetadataReader transferItemMetadataReader;
    private FileService fileService;
    private ExecutorService executorService;
    private InboxWatcherFactory inboxWatcherFactory;
    private List<CollectConfiguration.InboxEntry> inboxes;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.transferItemMetadataReader = Mockito.mock(TransferItemMetadataReader.class);
        this.fileService = Mockito.mock(FileService.class);
        this.executorService = Mockito.mock(ExecutorService.class);
        this.inboxWatcherFactory = Mockito.mock(InboxWatcherFactory.class);
        this.inboxes = List.of(
            createInboxEntry("Datastation 1", "data/inbox/1"),
            createInboxEntry("Datastation 2", "data/inbox/2")
        );

    }

    private CollectConfiguration.InboxEntry createInboxEntry(String name, String value) {
        var entry = new CollectConfiguration.InboxEntry();
        entry.setName(name);
        entry.setPath(Path.of(value));
        return entry;
    }

    @Test
    void createsInboxWatchersOnStart() {
        var manager = new CollectTaskManager(inboxes, Path.of("data/outbox/"), 100, executorService, transferItemService, transferItemMetadataReader, fileService,
            inboxWatcherFactory);

        try {
            manager.start();

            Mockito.verify(inboxWatcherFactory, Mockito.times(1)).getInboxWatcher(
                Mockito.eq(Path.of("data/inbox/1")),
                Mockito.eq("Datastation 1"),
                Mockito.any(),
                Mockito.eq(100L));

            Mockito.verify(inboxWatcherFactory, Mockito.times(1)).getInboxWatcher(
                Mockito.eq(Path.of("data/inbox/2")),
                Mockito.eq("Datastation 2"),
                Mockito.any(),
                Mockito.eq(100L));
        }
        catch (Exception e) {
            fail(e);
        }
    }

    @Test
    void onZipFileAdded() {
        var manager = new CollectTaskManager(inboxes, Path.of("data/outbox/"), 150L, executorService, transferItemService, transferItemMetadataReader, fileService,
            inboxWatcherFactory);
        var file = Mockito.mock(File.class);
        Mockito.when(file.isFile()).thenReturn(true);
        Mockito.when(file.getName()).thenReturn("some_file.zip");

        manager.onFileAdded(file, "ds1");

        Mockito.verify(executorService, Mockito.times(1)).execute(Mockito.any());
    }

    @Test
    void onNonZipFileAdded() {
        var manager = new CollectTaskManager(inboxes, Path.of("data/outbox/"), 100L, executorService, transferItemService, transferItemMetadataReader, fileService,
            inboxWatcherFactory);
        var file = Mockito.mock(File.class);
        Mockito.when(file.isFile()).thenReturn(true);
        Mockito.when(file.getName()).thenReturn("some_file.exe");

        manager.onFileAdded(file, "ds1");

        Mockito.verifyNoInteractions(executorService);
    }

}
