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

import static org.junit.jupiter.api.Assertions.fail;

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
    void run() {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364");
        var path = Path.of("data/inbox", uuid.toString());
        var task = new TarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService);

        try {
            var result = new ProcessResult(0, "OK");
            Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
                .thenReturn(result);
            Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
                .thenReturn(result);

            task.run();

            Mockito.verify(tarCommandRunner).tarDirectory(
                Path.of("data/inbox", uuid.toString()),
                uuid + ".dmftar"
            );

            Mockito.verify(transferItemService).updateTarToCreated(Mockito.eq(uuid.toString()), Mockito.any());
        }
        catch (IOException | InterruptedException e) {
            fail(e);
        }
    }

    @Test
    void runWithFailedCommand() {
        var uuid = UUID.fromString("82fa8591-b7e7-4efc-821e-addacb0cb364");
        var path = Path.of("data/inbox", uuid.toString());
        var task = new TarTask(transferItemService, uuid, path, tarCommandRunner, archiveMetadataService);

        try {
            var result = new ProcessResult(1, "NOT OK");
            Mockito.when(tarCommandRunner.tarDirectory(Mockito.any(), Mockito.any()))
                .thenReturn(result);
            Mockito.when(tarCommandRunner.verifyPackage(Mockito.any()))
                .thenReturn(result);

            task.run();

            Mockito.verify(tarCommandRunner).tarDirectory(
                Path.of("data/inbox", uuid.toString()),
                uuid + ".dmftar"
            );

            Mockito.verifyNoInteractions(transferItemService);
        }
        catch (IOException | InterruptedException e) {
            fail(e);
        }
    }
}
