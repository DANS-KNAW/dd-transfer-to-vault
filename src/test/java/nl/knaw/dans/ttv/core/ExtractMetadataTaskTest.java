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
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.TransferItemValidator;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ExtractMetadataTaskTest {

    private TransferItemService transferItemService;
    private TransferItemMetadataReader transferItemMetadataReader;
    private VaultCatalogRepository vaultCatalogRepository;
    private FileService fileService;
    private TransferItemValidator transferItemValidator;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.transferItemMetadataReader = Mockito.mock(TransferItemMetadataReader.class);
        this.vaultCatalogRepository = Mockito.mock(VaultCatalogRepository.class);
        this.fileService = Mockito.mock(FileService.class);
        this.transferItemValidator = Mockito.mock(TransferItemValidator.class);
    }

    @Test
    void testAllFilesAreMoved() throws IOException {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService, transferItemValidator, vaultCatalogRepository);

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(
                TransferItem.builder()
                    .datasetPid("pid")
                    .dveFilePath("path")
                    .creationTime(LocalDateTime.now())
                    .transferStatus(TransferItem.TransferStatus.COLLECTED)
                    .build()));

        task.run();

        Mockito.verify(fileService).moveFile(filePath, Path.of("data/outbox/doi-10-5072-dar-kxteqtv1.0.zip"));
    }

    @Test
    void testMetadataIsUpdated() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService, transferItemValidator, vaultCatalogRepository);
        var transferItem = TransferItem.builder()
            .datasetPid("pid")
            .dveFilePath("path")
            .creationTime(LocalDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        Mockito.when(transferItemService.addMetadata(Mockito.any(), Mockito.any()))
            .thenReturn(transferItem);

        task.run();

        Mockito.verify(transferItemService).addMetadata(
            Mockito.eq(transferItem),
            Mockito.any()
        );

        Mockito.verify(transferItemService).moveTransferItem(
            Mockito.eq(transferItem),
            Mockito.eq(TransferItem.TransferStatus.METADATA_EXTRACTED),
            Mockito.any()
        );
    }

    @Test
    void testMetadataExtractedStatusIsAlsoHandled() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService, transferItemValidator, vaultCatalogRepository);
        var transferItem = TransferItem.builder()
            .datasetPid("pid")
            .dveFilePath("path")
            .creationTime(LocalDateTime.now())
            .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        Mockito.when(transferItemService.addMetadata(Mockito.any(), Mockito.any()))
            .thenReturn(transferItem);

        task.run();

        Mockito.verify(transferItemService).addMetadata(
            Mockito.eq(transferItem),
            Mockito.any()
        );

        Mockito.verify(transferItemService).moveTransferItem(
            Mockito.eq(transferItem),
            Mockito.eq(TransferItem.TransferStatus.METADATA_EXTRACTED),
            Mockito.any()
        );
    }

    @ParameterizedTest
    @EnumSource(value = TransferItem.TransferStatus.class, mode = EnumSource.Mode.EXCLUDE, names = {"METADATA_EXTRACTED", "COLLECTED"})
    void testInvalidStates(TransferItem.TransferStatus status) {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService, transferItemValidator, vaultCatalogRepository);
        var transferItem = TransferItem.builder()
            .datasetPid("pid")
            .dveFilePath("path")
            .creationTime(LocalDateTime.now())
            .transferStatus(status)
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        assertThrows(InvalidTransferItemException.class, () -> task.processFile(filePath));
    }

    @ParameterizedTest
    @EnumSource(value = TransferItem.TransferStatus.class, mode = EnumSource.Mode.EXCLUDE, names = {"METADATA_EXTRACTED", "COLLECTED"})
    void testInvalidStatesOnRun(TransferItem.TransferStatus status) throws IOException {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService, transferItemValidator, vaultCatalogRepository);
        var transferItem = TransferItem.builder()
            .datasetPid("pid")
            .dveFilePath("path")
            .creationTime(LocalDateTime.now())
            .transferStatus(status)
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        task.run();

        Mockito.verify(fileService).rejectFile(Mockito.any(), Mockito.any());
    }
}
