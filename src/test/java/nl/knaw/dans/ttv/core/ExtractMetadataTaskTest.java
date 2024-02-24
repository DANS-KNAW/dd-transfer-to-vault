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

import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.core.service.TransferItemValidator;
import nl.knaw.dans.ttv.util.TestTransferItemMetadataReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExtractMetadataTaskTest {

    private TransferItemService transferItemService;
    private VaultCatalogClient vaultCatalogClient;
    private FileService fileService;
    private TransferItemValidator transferItemValidator;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.vaultCatalogClient = Mockito.mock(VaultCatalogClient.class);
        this.fileService = Mockito.mock(FileService.class);
        this.transferItemValidator = Mockito.mock(TransferItemValidator.class);
    }

    @Test
    void testAllFilesAreMoved() throws IOException {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0-ttv1.zip");
        var outbox = Path.of("data/outbox");

        var metadataReader = new TestTransferItemMetadataReader(
            null,
            new FilesystemAttributes(OffsetDateTime.now(), 1234L, "abc"),
            FileContentAttributes.builder().build()
        );

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, metadataReader, fileService, transferItemValidator, vaultCatalogClient);
        var transferItem = TransferItem.builder()
            .id(123L)
            .dveFilename("identifier")
            .dataversePid("pid")
            .dveFilePath("path")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        task.run();

        Mockito.verify(transferItemService).moveTransferItem(
            Mockito.eq(transferItem),
            Mockito.eq(TransferItem.TransferStatus.METADATA_EXTRACTED),
            Mockito.eq(outbox.resolve(transferItem.getCanonicalFilename()))
        );
    }

    @Test
    void testMetadataIsUpdated() throws Exception {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0-ttv1.zip");
        var outbox = Path.of("data/outbox");
        var metadataReader = new TestTransferItemMetadataReader(
            null,
            new FilesystemAttributes(OffsetDateTime.now(), 1234L, "abc"),
            FileContentAttributes.builder().build()
        );

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, metadataReader, fileService, transferItemValidator, vaultCatalogClient);
        var transferItem = TransferItem.builder()
            .id(1L)
            .dveFilename("identifier")
            .dataversePid("pid")
            .dveFilePath("path")
            .creationTime(OffsetDateTime.now())
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
    void testMetadataExtractedStatusIsAlsoHandled() throws Exception {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0-ttv1.zip");
        var outbox = Path.of("data/outbox");
        var metadataReader = new TestTransferItemMetadataReader(
            null,
            new FilesystemAttributes(OffsetDateTime.now(), 1234L, "abc"),
            FileContentAttributes.builder().build()
        );
        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, metadataReader, fileService, transferItemValidator, vaultCatalogClient);
        var transferItem = TransferItem.builder()
            .id(1L)
            .dveFilename("identifier")
            .dataversePid("pid")
            .dveFilePath("path")
            .creationTime(OffsetDateTime.now())
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
    @EnumSource(value = TransferItem.TransferStatus.class, mode = EnumSource.Mode.EXCLUDE, names = { "METADATA_EXTRACTED", "COLLECTED" })
    void testInvalidStates(TransferItem.TransferStatus status) {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0-ttv1.zip");
        var outbox = Path.of("data/outbox");

        var metadataReader = new TestTransferItemMetadataReader(
            null, null, null
        );

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, metadataReader, fileService, transferItemValidator, vaultCatalogClient);
        var transferItem = TransferItem.builder()
            .dataversePid("pid")
            .dveFilePath("path")
            .creationTime(OffsetDateTime.now())
            .transferStatus(status)
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        assertThatThrownBy(() -> task.processFile(filePath))
            .isInstanceOf(InvalidTransferItemException.class)
            .hasMessageContaining("TransferItem already exists but with an unexpected status");
    }

    @ParameterizedTest
    @EnumSource(value = TransferItem.TransferStatus.class, mode = EnumSource.Mode.EXCLUDE, names = { "METADATA_EXTRACTED", "COLLECTED" })
    void testInvalidStatesOnRun(TransferItem.TransferStatus status) throws Exception {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0-ttv1.zip");
        var outbox = Path.of("data/outbox");
        var metadataReader = new TestTransferItemMetadataReader(
            null, null, null
        );

        var task = new ExtractMetadataTask(filePath, outbox, transferItemService, metadataReader, fileService, transferItemValidator, vaultCatalogClient);
        var transferItem = TransferItem.builder()
            .dataversePid("pid")
            .dveFilePath("path")
            .creationTime(OffsetDateTime.now())
            .transferStatus(status)
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        task.run();

        Mockito.verify(fileService).rejectFile(Mockito.any(), Mockito.any());
    }
}
