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

import io.ocfl.api.OcflRepository;
import nl.knaw.dans.ttv.client.ApiException;
import nl.knaw.dans.ttv.core.domain.ProcessResult;
import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

class OcflTarTaskTest {

    private TransferItemService transferItemService;
    private TarCommandRunner tarCommandRunner;
    private ArchiveMetadataService archiveMetadataService;
    private OcflRepositoryService ocflRepositoryService;
    private VaultCatalogRepository vaultCatalogRepository;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        this.archiveMetadataService = Mockito.mock(ArchiveMetadataService.class);
        this.ocflRepositoryService = Mockito.mock(OcflRepositoryService.class);
        this.vaultCatalogRepository = Mockito.mock(VaultCatalogRepository.class);

    }

    @Test
    void run() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new OcflTarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService, ocflRepositoryService, vaultCatalogRepository, 1);
        var result = new ProcessResult(0, "OK");
        var tar = new Tar();
        Mockito.when(transferItemService.getTarById(Mockito.any()))
            .thenReturn(Optional.of(tar));

        Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
            .thenThrow(IOException.class)
            .thenReturn(result);
        Mockito.when(tarCommandRunner.deletePackage(Mockito.any()))
            .thenThrow(IOException.class);

        Mockito.when(transferItemService.updateTarToCreated(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(tar));

        task.run();

        Mockito.verify(tarCommandRunner).tarDirectory(
            Path.of("data/inbox", uuid, "ocfl-repo"),
            uuid + ".dmftar"
        );

        Mockito.verify(transferItemService).updateTarToCreated(Mockito.eq(uuid), Mockito.any());

        Mockito.verify(vaultCatalogRepository, Mockito.times(2)).registerTar(Mockito.any());
    }

    @Test
    void runWithExistingValidRemoteArchive() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new OcflTarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService, ocflRepositoryService, vaultCatalogRepository, 1);
        var tar = new Tar();
        Mockito.when(transferItemService.getTarById(Mockito.any()))
            .thenReturn(Optional.of(tar));

        var result = new ProcessResult(0, "OK");
        Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.deletePackage(Mockito.any()))
            .thenThrow(IOException.class);

        task.run();

        Mockito.verify(tarCommandRunner, Mockito.times(0)).tarDirectory(Mockito.any(), Mockito.any());
        Mockito.verify(tarCommandRunner, Mockito.times(0)).deletePackage(Mockito.any());
        Mockito.verify(transferItemService).updateTarToCreated(Mockito.eq(uuid), Mockito.any());

    }

    @Test
    void runWithExistingInvalidRemoteArchive() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new OcflTarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService, ocflRepositoryService, vaultCatalogRepository, 1);
        var tar = new Tar();
        Mockito.when(transferItemService.getTarById(Mockito.any()))
            .thenReturn(Optional.of(tar));

        var result = new ProcessResult(0, "OK");
        Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
            .thenThrow(IOException.class)
            .thenReturn(result);
        Mockito.when(tarCommandRunner.deletePackage(Mockito.any()))
            .thenReturn(result);

        task.run();

        Mockito.verify(tarCommandRunner, Mockito.times(1)).tarDirectory(Mockito.any(), Mockito.any());
        Mockito.verify(tarCommandRunner, Mockito.times(1)).deletePackage(Mockito.any());
        Mockito.verify(transferItemService).updateTarToCreated(Mockito.eq(uuid), Mockito.any());

    }

    @Test
    void runWithFailedCommand() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new OcflTarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService, ocflRepositoryService, vaultCatalogRepository, 1);
        var tar = new Tar();
        Mockito.when(transferItemService.getTarById(Mockito.any()))
            .thenReturn(Optional.of(tar));

        var result = new ProcessResult(1, "NOT OK");
        Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.deletePackage(Mockito.any()))
            .thenReturn(result);
        task.run();

        Mockito.verify(tarCommandRunner).tarDirectory(
            Path.of("data/inbox", uuid, "ocfl-repo"),
            uuid + ".dmftar"
        );

        Mockito.verify(transferItemService).setArchiveAttemptFailed(Mockito.any(), Mockito.eq(true), Mockito.anyInt());
    }

    /**
     * Tests if the OcflTarTask::importTransferItemsIntoRepository method imports items into the repo, and only those that are not yet imported.
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    void doesInitialImport() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new OcflTarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService, ocflRepositoryService, vaultCatalogRepository, 1);
        var tar = new Tar();

        var ocflRepository = Mockito.mock(OcflRepository.class);
        var transferItems = List.of(
            TransferItem.builder()
                .datasetPid("pid1")
                .dveFilePath("path/to1.zip")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build(),
            TransferItem.builder()
                .datasetPid("pid2")
                .dveFilePath("path/to2.zip")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build()
        );

        Mockito.when(ocflRepositoryService.openRepository(Mockito.any())).thenReturn(ocflRepository);
        tar.setTransferItems(transferItems);

        task.importTransferItemsIntoRepository(tar);

        Mockito.verify(ocflRepositoryService).importTransferItem(ocflRepository, transferItems.get(0));
    }

    @Test
    void throwsExceptions() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new OcflTarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService, ocflRepositoryService, vaultCatalogRepository, 1);

        Mockito.when(transferItemService.getTarById(Mockito.any()))
            .thenReturn(Optional.empty());

        Assertions.assertThrows(InvalidTarException.class, task::createArchive);

    }

}
