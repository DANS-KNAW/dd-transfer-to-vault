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

import nl.knaw.dans.ttv.core.dto.ProcessResult;
import nl.knaw.dans.ttv.core.service.ArchiveMetadataService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

class TarTaskTest {

    private TransferItemService transferItemService;
    private TarCommandRunner tarCommandRunner;
    private ArchiveMetadataService archiveMetadataService;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        this.archiveMetadataService = Mockito.mock(ArchiveMetadataService.class);
    }

    @Test
    void run() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new TarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService);

        var result = new ProcessResult(0, "OK");
        Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
            .thenThrow(IOException.class)
            .thenReturn(result);
        Mockito.when(tarCommandRunner.deletePackage(Mockito.any()))
            .thenThrow(IOException.class);

        task.run();

        Mockito.verify(tarCommandRunner).tarDirectory(
            Path.of("data/inbox", uuid),
            uuid + ".dmftar"
        );

        Mockito.verify(transferItemService).updateTarToCreated(Mockito.eq(uuid), Mockito.any());

    }

    @Test
    void runWithExistingValidRemoteArchive() throws IOException, InterruptedException {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364").toString();
        var path = Path.of("data/inbox", uuid);
        var task = new TarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService);

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
        var task = new TarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService);

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
        var task = new TarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService);

        var result = new ProcessResult(1, "NOT OK");
        Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
            .thenReturn(result);
        Mockito.when(tarCommandRunner.deletePackage(Mockito.any()))
            .thenReturn(result);
        task.run();

        Mockito.verify(tarCommandRunner).tarDirectory(
            Path.of("data/inbox", uuid),
            uuid + ".dmftar"
        );

        Mockito.verify(transferItemService).resetTarToTarring(Mockito.any(), Mockito.eq(true));
    }
}
