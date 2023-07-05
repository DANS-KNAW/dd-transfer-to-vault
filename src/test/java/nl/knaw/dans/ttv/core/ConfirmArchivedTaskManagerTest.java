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

import nl.knaw.dans.ttv.core.service.ArchiveStatusService;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.Tar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.quartz.Scheduler;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;

class ConfirmArchivedTaskManagerTest {
    private TransferItemService transferItemService;
    private ExecutorService executorService;
    private FileService fileService;
    private ArchiveStatusService archiveStatusService;
    private VaultCatalogRepository vaultCatalogRepository;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.executorService = Mockito.mock(ExecutorService.class);
        this.fileService = Mockito.mock(FileService.class);
        this.archiveStatusService = Mockito.mock(ArchiveStatusService.class);
        this.vaultCatalogRepository = Mockito.mock(VaultCatalogRepository.class);
    }

    @Test
    void start() throws Exception {
        var manager = Mockito.spy(new ConfirmArchivedTaskManager(
            "0 0/15 * * * ?",
            Path.of("path"),
            executorService,
            transferItemService,
            archiveStatusService,
            fileService,
            vaultCatalogRepository));

        var scheduler = Mockito.mock(Scheduler.class);
        Mockito.when(manager.createScheduler()).thenReturn(scheduler);

        manager.start();

        Mockito.verify(scheduler).scheduleJob(Mockito.any(), Mockito.any());
        Mockito.verify(scheduler).start();
        Mockito.verify(scheduler).triggerJob(Mockito.any(), Mockito.any());
    }

    @Test
    void verifyArchives() {
        var manager = Mockito.spy(new ConfirmArchivedTaskManager(
            "0 0/15 * * * ?",
            Path.of("path"),
            executorService,
            transferItemService,
            archiveStatusService,
            fileService,
            vaultCatalogRepository));

        var tar = new Tar();

        Mockito.when(transferItemService.findTarsByConfirmInProgress())
            .thenReturn(List.of(tar));

        manager.verifyArchives();

        Mockito.verify(transferItemService).resetTarToArchiving(tar);
    }

}
