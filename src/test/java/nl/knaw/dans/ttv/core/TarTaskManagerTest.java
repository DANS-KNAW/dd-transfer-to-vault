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

import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

class TarTaskManagerTest {
    private TransferItemService transferItemService;
    private TransferItemMetadataReader transferItemMetadataReader;
    private FileService fileService;
    private ExecutorService executorService;
    private InboxWatcherFactory inboxWatcherFactory;
    private OcflRepositoryService ocflRepositoryService;
    private TarCommandRunner tarCommandRunner;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.transferItemMetadataReader = Mockito.mock(TransferItemMetadataReader.class);
        this.fileService = Mockito.mock(FileService.class);
        this.executorService = Mockito.mock(ExecutorService.class);
        this.inboxWatcherFactory = Mockito.mock(InboxWatcherFactory.class);
        this.ocflRepositoryService = Mockito.mock(OcflRepositoryService.class);
        this.tarCommandRunner = Mockito.mock(TarCommandRunner.class);
    }

    @Test
    void start() {
    }

    /**
     * Test that the size of the inbox is greater than the threshold, and all required actions are executed, plus a task has been started to handle it
     */
    @Test
    void onNewItemInInbox() {
        var manager = new TarTaskManager(
            "data/inbox", "data/workdir", 50, "dmf", "tst@account.com:",
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner);

        try {
            var transferItems = List.of(
                new TransferItem("pid1", 1, 0, "path1", LocalDateTime.now(), TransferItem.TransferStatus.COLLECTED),
                new TransferItem("pid2", 1, 0, "path2", LocalDateTime.now(), TransferItem.TransferStatus.COLLECTED)
            );

            Mockito.when(fileService.getPathSize(Path.of("data/inbox")))
                .thenReturn(100L);

            Mockito.when(transferItemService.findByStatus(TransferItem.TransferStatus.COLLECTED))
                .thenReturn(transferItems);

            manager.onNewItemInInbox(new File("test.zip"));

            Mockito.verify(fileService).getPathSize(Path.of("data/inbox"));
            Mockito.verify(ocflRepositoryService).createRepository(Mockito.eq(Path.of("data/workdir")), Mockito.any());
            Mockito.verify(ocflRepositoryService).importTransferItem(
                Mockito.any(),
                Mockito.eq(transferItems.get(0))
            );
            Mockito.verify(ocflRepositoryService).importTransferItem(
                Mockito.any(),
                Mockito.eq(transferItems.get(1))
            );
            Mockito.verify(transferItemService).saveAll(transferItems);
            assertEquals(TransferItem.TransferStatus.TARRING, transferItems.get(0).getTransferStatus());
            assertEquals(TransferItem.TransferStatus.TARRING, transferItems.get(1).getTransferStatus());

            assertNotNull(transferItems.get(0).getAipsTar());
            assertNotNull(transferItems.get(1).getAipsTar());
            assertEquals(transferItems.get(0).getAipsTar(), transferItems.get(1).getAipsTar());

            Mockito.verify(executorService).execute(Mockito.any());
        }
        catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void testThresholdIsNotReached() {
        var manager = new TarTaskManager(
            "data/inbox", "data/workdir", 50, "dmf", "tst@account.com:",
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner);

        try {
            var transferItems = List.of(
                new TransferItem("pid1", 1, 0, "path1", LocalDateTime.now(), TransferItem.TransferStatus.COLLECTED),
                new TransferItem("pid2", 1, 0, "path2", LocalDateTime.now(), TransferItem.TransferStatus.COLLECTED)
            );

            Mockito.when(fileService.getPathSize(Path.of("data/inbox")))
                .thenReturn(25L);

            manager.onNewItemInInbox(new File("test.zip"));

            Mockito.verifyNoInteractions(ocflRepositoryService);
            Mockito.verifyNoInteractions(transferItemService);
            assertEquals(TransferItem.TransferStatus.COLLECTED, transferItems.get(0).getTransferStatus());
            assertEquals(TransferItem.TransferStatus.COLLECTED, transferItems.get(1).getTransferStatus());

        }
        catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void moveAllInboxFilesToOcflRepo() {
    }

    @Test
    void createOcflRepo() {
    }

}
