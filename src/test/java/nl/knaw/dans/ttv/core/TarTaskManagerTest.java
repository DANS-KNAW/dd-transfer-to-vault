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

import edu.wisc.library.ocfl.api.OcflRepository;
import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TarTaskManagerTest {
    private TransferItemService transferItemService;
    private FileService fileService;
    private ExecutorService executorService;
    private InboxWatcherFactory inboxWatcherFactory;
    private OcflRepositoryService ocflRepositoryService;
    private TarCommandRunner tarCommandRunner;
    private ArchiveMetadataService archiveMetadataService;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.fileService = Mockito.mock(FileService.class);
        this.executorService = Mockito.mock(ExecutorService.class);
        this.inboxWatcherFactory = Mockito.mock(InboxWatcherFactory.class);
        this.ocflRepositoryService = Mockito.mock(OcflRepositoryService.class);
        this.tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        this.archiveMetadataService = Mockito.mock(ArchiveMetadataService.class);
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
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L,
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService);

        try {
            var transferItems = List.of(
                new TransferItem("pid1", 1, 0, "path1", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED),
                new TransferItem("pid2", 1, 0, "path2", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED)
            );

            var tar = new Tar();
            tar.setTransferItems(transferItems);

            Mockito.when(fileService.getPathSize(Path.of("data/inbox")))
                .thenReturn(100L);

            Mockito.when(transferItemService.createTarArchiveWithAllMetadataExtractedTransferItems(Mockito.any(), Mockito.eq("some-path")))
                .thenReturn(tar);

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
            Mockito.verify(transferItemService).save(Mockito.any());
            //            assertEquals(TransferItem.TransferStatus.TARRING, transferItems.get(0).getTransferStatus());
            //            assertEquals(TransferItem.TransferStatus.TARRING, transferItems.get(1).getTransferStatus());

            //            assertNotNull(transferItems.get(0).getAipsTar());
            //            assertNotNull(transferItems.get(1).getAipsTar());
            //            assertEquals(transferItems.get(0).getAipsTar(), transferItems.get(1).getAipsTar());

            Mockito.verify(executorService).execute(Mockito.any());
        }
        catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void testThresholdIsNotReached() throws IOException {
        var manager = new TarTaskManager(
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L,
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService);

        var transferItems = List.of(
            new TransferItem("pid1", 1, 0, "path1", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED),
            new TransferItem("pid2", 1, 0, "path2", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED)
        );

        Mockito.when(fileService.getPathSize(Path.of("data/inbox")))
            .thenReturn(25L);

        manager.onNewItemInInbox(new File("test.zip"));

        Mockito.verifyNoInteractions(ocflRepositoryService);
        Mockito.verifyNoInteractions(transferItemService);
        assertEquals(TransferItem.TransferStatus.METADATA_EXTRACTED, transferItems.get(0).getTransferStatus());
        assertEquals(TransferItem.TransferStatus.METADATA_EXTRACTED, transferItems.get(1).getTransferStatus());
    }

    @Test
    void verifyInbox() {
        var manager = new TarTaskManager(
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L,
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService);

        var transferItems = List.of(
            new TransferItem("pid1", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.TARRING),
            new TransferItem("pid2", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.TARRING)
        );
        var tar = new Tar(UUID.randomUUID().toString(), Tar.TarStatus.TARRING, false);
        tar.setTransferItems(transferItems);
        var tars = List.of(tar);

        var ocflRepo = Mockito.mock(OcflRepository.class);
        Mockito.when(ocflRepo.containsObject(Mockito.any()))
            .thenReturn(true, false);

        Mockito.when(ocflRepositoryService.getObjectIdForTransferItem(Mockito.any()))
            .thenReturn("id1", "id2");

        Mockito.when(ocflRepositoryService.openRepository(Mockito.any(), Mockito.any())).thenReturn(ocflRepo);
        Mockito.when(transferItemService.findTarsByStatusTarring()).thenReturn(tars);

        manager.verifyInbox();

        // assert the entry names are set correctly
        assertEquals("id1", transferItems.get(0).getAipTarEntryName());
        assertEquals("id2", transferItems.get(1).getAipTarEntryName());

        // assert the missing entry has been imported
        Mockito.verify(ocflRepositoryService).importTransferItem(ocflRepo, transferItems.get(1));

        // assert the tar has been persisted to the database
        Mockito.verify(transferItemService).save(tar);
    }

    @Test
    void testVerificationAndInboxWatchersAreStarted() throws Exception {
        var manager = new TarTaskManager(
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L,
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService);

        var manager1 = Mockito.spy(manager);
        Mockito.doNothing().when(manager1).verifyInbox();

        var inboxWatcher = Mockito.mock(InboxWatcher.class);
        Mockito.when(inboxWatcherFactory.getInboxWatcher(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
            .thenReturn(inboxWatcher);

        manager1.start();

        Mockito.verify(manager1).verifyInbox();
        Mockito.verify(inboxWatcher).start();
    }
}
