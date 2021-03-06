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

import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.InboxWatcher;
import nl.knaw.dans.ttv.core.service.InboxWatcherFactory;
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.VaultCatalogService;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;

class OcflTarTaskManagerTest {
    private TransferItemService transferItemService;
    private FileService fileService;
    private ExecutorService executorService;
    private InboxWatcherFactory inboxWatcherFactory;
    private OcflRepositoryService ocflRepositoryService;
    private TarCommandRunner tarCommandRunner;
    private ArchiveMetadataService archiveMetadataService;
    private VaultCatalogService vaultCatalogService;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.fileService = Mockito.mock(FileService.class);
        this.executorService = Mockito.mock(ExecutorService.class);
        this.inboxWatcherFactory = Mockito.mock(InboxWatcherFactory.class);
        this.ocflRepositoryService = Mockito.mock(OcflRepositoryService.class);
        this.tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        this.archiveMetadataService = Mockito.mock(ArchiveMetadataService.class);
        this.vaultCatalogService = Mockito.mock(VaultCatalogService.class);
    }

    /**
     * Test that the size of the inbox is greater than the threshold, and all required actions are executed, plus a task has been started to handle it
     */
    @Test
    void onNewItemInInbox() throws SchedulerException, IOException {
        var manager = Mockito.spy(new OcflTarTaskManager(
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L, 10, Duration.ofMinutes(1), List.of(),
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService, vaultCatalogService));

        var transferItems = List.of(
            new TransferItem("pid1", 1, 0, "path1", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED),
            new TransferItem("pid2", 1, 0, "path2", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED)
        );

        var tar = new Tar("tarid", Tar.TarStatus.TARRING, false);
        tar.setTransferItems(transferItems);

        var scheduler = Mockito.mock(Scheduler.class);
        Mockito.when(manager.createScheduler()).thenReturn(scheduler);

        Mockito.when(fileService.getPathSize(Path.of("data/inbox")))
            .thenReturn(100L);

        Mockito.when(transferItemService.createTarArchiveWithAllMetadataExtractedTransferItems(Mockito.any(), Mockito.eq("some-path")))
            .thenReturn(tar);

        manager.onNewItemInInbox(new File("test.zip"));

        Mockito.verify(fileService).getPathSize(Path.of("data/inbox"));
//        Mockito.verify(ocflRepositoryService).createRepository(Mockito.eq(Path.of("data/workdir")), Mockito.any());
//        Mockito.verify(ocflRepositoryService).importTransferItem(
//            Mockito.any(),
//            Mockito.eq(transferItems.get(0))
//        );
//        Mockito.verify(ocflRepositoryService).importTransferItem(
//            Mockito.any(),
//            Mockito.eq(transferItems.get(1))
//        );
//        Mockito.verify(transferItemService).save(Mockito.any());

        Mockito.verify(executorService).execute(Mockito.any());

    }

    @Test
    void testThresholdIsNotReached() throws IOException, SchedulerException {
        var manager = Mockito.spy(new OcflTarTaskManager(
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L, 10, Duration.ofMinutes(1), List.of(),
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService, vaultCatalogService));

        var transferItems = List.of(
            new TransferItem("pid1", 1, 0, "path1", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED),
            new TransferItem("pid2", 1, 0, "path2", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED)
        );

        var scheduler = Mockito.mock(Scheduler.class);
        Mockito.when(manager.createScheduler()).thenReturn(scheduler);

        Mockito.when(fileService.getPathSize(Path.of("data/inbox")))
            .thenReturn(25L);

        manager.onNewItemInInbox(new File("test.zip"));

        Mockito.verifyNoInteractions(ocflRepositoryService);
        Mockito.verifyNoInteractions(transferItemService);
        assertEquals(TransferItem.TransferStatus.METADATA_EXTRACTED, transferItems.get(0).getTransferStatus());
        assertEquals(TransferItem.TransferStatus.METADATA_EXTRACTED, transferItems.get(1).getTransferStatus());
    }

    @Test
    void verifyInbox() throws SchedulerException, IOException {
        var manager = Mockito.spy(new OcflTarTaskManager(
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L, 10, Duration.ofMinutes(1), List.of(),
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService, vaultCatalogService));

        var scheduler = Mockito.mock(Scheduler.class);
        Mockito.when(manager.createScheduler()).thenReturn(scheduler);

        // note that the second file has already been moved to the working directory and should not be moved again
        var transferItems = List.of(
            new TransferItem("pid1", 1, 0, "data/inbox/1.zip", LocalDateTime.now(), TransferItem.TransferStatus.TARRING),
            new TransferItem("pid2", 1, 0, "data/workdir/tarid/dve/2.zip", LocalDateTime.now(), TransferItem.TransferStatus.TARRING)
        );
        var tar = new Tar("tarid", Tar.TarStatus.TARRING, false);
        tar.setTransferItems(transferItems);
        var tars = List.of(tar);

        Mockito.when(transferItemService.findTarsByStatusTarring()).thenReturn(tars);

        Mockito.when(fileService.moveFile(Mockito.any(), Mockito.any()))
                .thenReturn(Path.of("data/workdir/tarid/dve/1.zip"));
        Mockito.when(fileService.exists(Mockito.any())).thenReturn(true, false);

        manager.verifyInbox();

        Mockito.verify(fileService).ensureDirectoryExists(Mockito.eq(Path.of("data/workdir/tarid/dve")));
        Mockito.verify(fileService).moveFile(Mockito.eq(Path.of("data/inbox/1.zip")), Mockito.eq(Path.of("data/workdir/tarid/dve/1.zip")));
        Mockito.verify(fileService).exists(Mockito.eq(Path.of("data/inbox/1.zip")));
        Mockito.verify(fileService).exists(Mockito.eq(Path.of("data/inbox/2.zip")));
        Mockito.verifyNoMoreInteractions(fileService);

        Mockito.verify(transferItemService).moveTransferItem(transferItems.get(0), TransferItem.TransferStatus.TARRING, Path.of("data/workdir/tarid/dve/1.zip"));

    }

    @Test
    void testVerificationAndInboxWatchersAreStarted() throws Exception {
        var manager = Mockito.spy(new OcflTarTaskManager(
            Path.of("data/inbox"), Path.of("data/workdir"), "some-path", 50, 100L, 10, Duration.ofMinutes(1), List.of(),
            executorService, inboxWatcherFactory, fileService, ocflRepositoryService, transferItemService,
            tarCommandRunner, archiveMetadataService, vaultCatalogService));

        var scheduler = Mockito.mock(Scheduler.class);
        Mockito.when(manager.createScheduler()).thenReturn(scheduler);

        Mockito.doNothing().when(manager).verifyInbox();

        var inboxWatcher = Mockito.mock(InboxWatcher.class);
        Mockito.when(inboxWatcherFactory.getInboxWatcher(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyLong()))
            .thenReturn(inboxWatcher);

        manager.start();

        Mockito.verify(manager).verifyInbox();
        Mockito.verify(inboxWatcher).start();
    }
}
