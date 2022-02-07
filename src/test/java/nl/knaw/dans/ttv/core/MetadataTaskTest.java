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

class MetadataTaskTest {

    private TransferItemService transferItemService;
    private TransferItemMetadataReader transferItemMetadataReader;
    private FileService fileService;

    @BeforeEach
    void setUp() {
        this.transferItemService = Mockito.mock(TransferItemService.class);
        this.transferItemMetadataReader = Mockito.mock(TransferItemMetadataReader.class);
        this.fileService = Mockito.mock(FileService.class);
    }

    @Test
    void testAllFilesAreMoved() throws IOException {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new MetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService);

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.CREATED)));

        task.run();

        Mockito.verify(fileService).moveFile(filePath, Path.of("data/outbox/doi-10-5072-dar-kxteqtv1.0.zip"));
    }

    @Test
    void testMetadataIsUpdated() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new MetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService);
        var transferItem = new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.CREATED);

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        task.run();

        Mockito.verify(transferItemService).addMetadataAndMoveFile(
            Mockito.eq(transferItem),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(TransferItem.TransferStatus.COLLECTED),
            Mockito.eq(Path.of("data/outbox/doi-10-5072-dar-kxteqtv1.0.zip"))
        );
    }

    @Test
    void testCollectedStatusIsAlsoHandled() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new MetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService);
        var transferItem = new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.COLLECTED);

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        task.run();

        Mockito.verify(transferItemService).addMetadataAndMoveFile(
            Mockito.eq(transferItem),
            Mockito.any(),
            Mockito.any(),
            Mockito.eq(TransferItem.TransferStatus.COLLECTED),
            Mockito.eq(Path.of("data/outbox/doi-10-5072-dar-kxteqtv1.0.zip"))
        );
    }

    @ParameterizedTest
    @EnumSource(value = TransferItem.TransferStatus.class, mode = EnumSource.Mode.EXCLUDE, names = { "COLLECTED", "CREATED" })
    void testInvalidStates(TransferItem.TransferStatus status) {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");

        var task = new MetadataTask(filePath, outbox, transferItemService, transferItemMetadataReader, fileService);
        var transferItem = new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), status);

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        assertThrows(InvalidTransferItemException.class, () -> {
            task.processFile(filePath);
        });
    }
}
