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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

class ConfirmArchivedTaskTest {

    private TransferItemService transferItemService;
    private ArchiveStatusService archiveStatusService;
    private FileService fileService;
    private VaultCatalogRepository vaultCatalogRepository;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.archiveStatusService = Mockito.mock(ArchiveStatusService.class);
        this.fileService = Mockito.mock(FileService.class);
        this.vaultCatalogRepository = Mockito.mock(VaultCatalogRepository.class);
    }

    @Test
    void testCompletelyArchived() throws IOException, InterruptedException {
        var tar = new Tar("test1", Tar.TarStatus.OCFLTARCREATED, false);
        var path = Path.of("workingdir");
        var task = new ConfirmArchivedTask(tar, transferItemService, archiveStatusService, fileService, path, vaultCatalogRepository);

        var fileStatus = Map.of(
            "file1", ArchiveStatusService.FileStatus.DUAL,
            "file2", ArchiveStatusService.FileStatus.OFFLINE
        );

        Mockito.when(archiveStatusService.getFileStatus("test1"))
            .thenReturn(fileStatus);

        task.run();

        Mockito.verify(transferItemService).updateTarToArchived(Mockito.any());
        Mockito.verify(fileService).deleteDirectory(Path.of("workingdir", "test1"));
        Mockito.verify(vaultCatalogRepository).registerTar(Mockito.any());
    }

    @Test
    void testPartiallyArchived() throws IOException, InterruptedException {
        var tar = new Tar("test1", Tar.TarStatus.OCFLTARCREATED, false);
        var path = Path.of("workingdir");
        var task = new ConfirmArchivedTask(tar, transferItemService, archiveStatusService, fileService, path, vaultCatalogRepository);

        var fileStatus = Map.of(
            "file1", ArchiveStatusService.FileStatus.DUAL,
            "file2", ArchiveStatusService.FileStatus.REGULAR
        );

        Mockito.when(archiveStatusService.getFileStatus("test1"))
            .thenReturn(fileStatus);

        task.run();

        Mockito.verify(transferItemService).resetTarToArchiving(Mockito.any());
        Mockito.verify(fileService, Mockito.times(0))
            .deleteDirectory(Mockito.any());
        Mockito.verifyNoInteractions(vaultCatalogRepository);
    }

    @Test
    void testOnError() throws IOException, InterruptedException {
        var tar = new Tar("test1", Tar.TarStatus.OCFLTARCREATED, false);
        var path = Path.of("workingdir");
        var task = new ConfirmArchivedTask(tar, transferItemService, archiveStatusService, fileService, path, vaultCatalogRepository);

        Mockito.when(archiveStatusService.getFileStatus("test1"))
            .thenThrow(IOException.class);

        task.run();

        Mockito.verify(transferItemService).resetTarToArchiving(Mockito.any());
        Mockito.verify(fileService, Mockito.times(0))
            .deleteDirectory(Mockito.any());

        Mockito.verifyNoInteractions(vaultCatalogRepository);
    }

    @Test
    void testOnCleanupErrorShouldNotThrowErrors() throws IOException, InterruptedException {
        var tar = new Tar("test1", Tar.TarStatus.OCFLTARCREATED, false);
        var path = Path.of("workingdir");
        var task = new ConfirmArchivedTask(tar, transferItemService, archiveStatusService, fileService, path, vaultCatalogRepository);

        var fileStatus = Map.of(
            "file1", ArchiveStatusService.FileStatus.DUAL,
            "file2", ArchiveStatusService.FileStatus.OFFLINE
        );

        Mockito.when(archiveStatusService.getFileStatus("test1"))
            .thenReturn(fileStatus);

        Mockito.doThrow(IOException.class)
            .when(fileService).deleteDirectory(Mockito.any());

        task.run();

        Mockito.verify(fileService).deleteDirectory(Mockito.any());
    }
}
