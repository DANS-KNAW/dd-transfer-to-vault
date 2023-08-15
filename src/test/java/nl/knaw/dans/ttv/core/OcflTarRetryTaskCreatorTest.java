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
import nl.knaw.dans.ttv.core.service.OcflRepositoryService;
import nl.knaw.dans.ttv.core.service.TarCommandRunner;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.*;

class OcflTarRetryTaskCreatorTest {
    private TransferItemService transferItemService;
    private ExecutorService executorService;
    private TarCommandRunner tarCommandRunner;
    private ArchiveMetadataService archiveMetadataService;
    private OcflRepositoryService ocflRepositoryService;
    private VaultCatalogRepository vaultCatalogRepository;
    private Path workDir;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.executorService = Mockito.mock(ExecutorService.class);
        this.tarCommandRunner = Mockito.mock(TarCommandRunner.class);
        this.archiveMetadataService = Mockito.mock(ArchiveMetadataService.class);
        this.workDir = Path.of("workdir");
        this.ocflRepositoryService = Mockito.mock(OcflRepositoryService.class);
        this.vaultCatalogRepository = Mockito.mock(VaultCatalogRepository.class);
    }

    @Test
    void run() {
        var intervals = List.of(
            Duration.of(1, ChronoUnit.HOURS),
            Duration.of(8, ChronoUnit.HOURS),
            Duration.of(24, ChronoUnit.HOURS)
        );
        var params = new OcflTarRetryTaskCreator.TaskRetryTaskCreatorParameters(
            transferItemService, workDir, tarCommandRunner, archiveMetadataService, executorService, 5, intervals, ocflRepositoryService,
            vaultCatalogRepository);

        var tar = new Tar();
        tar.setCreated(OffsetDateTime.now().minus(20, ChronoUnit.HOURS));
        tar.setTransferAttempt(0);
        tar.setTarUuid("test1");

        Mockito.when(transferItemService.findTarsToBeRetried()).thenReturn(List.of(tar));

        var creator = new OcflTarRetryTaskCreator();
        creator.run(params);

        // it should have started up a task because the Tar is within range
        Mockito.verify(executorService).execute(Mockito.any());
    }

    @Test
    void runWithoutCandidates() {
        var intervals = List.of(
            Duration.of(1, ChronoUnit.HOURS),
            Duration.of(8, ChronoUnit.HOURS),
            Duration.of(24, ChronoUnit.HOURS)
        );
        var params = new OcflTarRetryTaskCreator.TaskRetryTaskCreatorParameters(
            transferItemService, workDir, tarCommandRunner, archiveMetadataService, executorService, 5, intervals, ocflRepositoryService,
            vaultCatalogRepository);

        var tar = new Tar();
        tar.setCreated(OffsetDateTime.now().minus(20, ChronoUnit.HOURS));
        tar.setTransferAttempt(2);
        tar.setTarUuid("test1");

        Mockito.when(transferItemService.findTarsToBeRetried()).thenReturn(List.of(tar));

        var creator = new OcflTarRetryTaskCreator();
        creator.run(params);

        // it should have started up a task because the Tar is within range
        Mockito.verifyNoInteractions(executorService);
    }

    @Test
    void shouldRetry() {
        var creator = new OcflTarRetryTaskCreator();
        var intervals = List.of(
            Duration.of(1, ChronoUnit.HOURS),
            Duration.of(8, ChronoUnit.HOURS),
            Duration.of(24, ChronoUnit.HOURS)
        );

        var tar = new Tar();
        tar.setCreated(OffsetDateTime.now().minus(20, ChronoUnit.HOURS));
        tar.setTransferAttempt(0);
        tar.setTarUuid("test1");

        assertTrue(creator.shouldRetry(tar, intervals));

        tar.setTransferAttempt(1);
        assertTrue(creator.shouldRetry(tar, intervals));

        tar.setTransferAttempt(2);
        assertFalse(creator.shouldRetry(tar, intervals));

        tar.setTransferAttempt(3);
        assertFalse(creator.shouldRetry(tar, intervals));
    }

    @Test
    void calculateThreshold() {
        var creator = new OcflTarRetryTaskCreator();
        var intervals = List.of(
            Duration.of(1, ChronoUnit.HOURS),
            Duration.of(8, ChronoUnit.HOURS),
            Duration.of(24, ChronoUnit.HOURS)
        );

        assertEquals("PT1H", creator.calculateThreshold(0, intervals).toString());
        assertEquals("PT8H", creator.calculateThreshold(1, intervals).toString());
        assertEquals("PT24H", creator.calculateThreshold(2, intervals).toString());
        assertEquals("PT48H", creator.calculateThreshold(3, intervals).toString());
    }
}
