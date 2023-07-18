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

import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.TransferItem;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.fail;

class CollectTaskTest {

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
    void run_should_move_all_files_to_correct_location() throws IOException, InvalidTransferItemException {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);

        Mockito.when(transferItemMetadataReader.getAssociatedXmlFile(Mockito.any()))
            .thenReturn(Optional.of(Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.xml")));

        var transferItem = TransferItem.builder()
            .id(16L)
            .datasetIdentifier("doi-10-5072-dar-kxteqtv1.0")
            .doi("pid1")
            .dveFilePath("path/to1.zip")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .build();

        Mockito.when(transferItemService.createTransferItem(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
            .thenReturn(transferItem);

        task.run();

        // check that xml file was removed
        Mockito.verify(fileService).deleteFile(Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.xml"));

        Mockito.verify(transferItemService)
            .createTransferItem(Mockito.eq("dsname"), Mockito.any(), Mockito.any(), Mockito.any());

        Mockito.verify(transferItemService)
            .moveTransferItem(
                Mockito.any(),
                Mockito.eq(TransferItem.TransferStatus.COLLECTED),
                Mockito.eq(filePath),
                Mockito.eq(outbox)
            );
    }

    @Test
    void processFile_should_not_allow_duplicate_files_if_version_is_in_filename_and_checksum_match_and_status_is_not_COLLECTED() throws Exception {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0-ttv1.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);

        var filenameAttributes = FilenameAttributes.builder()
            .identifier("identifier")
            .dveFilePath(filePath.toString())
            .internalId(1L)
            .build();

        var filesystemAttributes = new FilesystemAttributes(OffsetDateTime.now(), 1234L, "abc");

        var existingTransferItem = TransferItem.builder()
            .id(1L)
            .datasetIdentifier("identifier")
            .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
            .bagChecksum("abc")
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(filenameAttributes))
            .thenReturn(Optional.of(existingTransferItem));

        Mockito.when(transferItemMetadataReader.getFilenameAttributes(filePath))
            .thenReturn(filenameAttributes);

        Mockito.when(transferItemMetadataReader.getFilesystemAttributes(filePath))
            .thenReturn(filesystemAttributes);

        Assertions.assertThatThrownBy(() -> task.processFile(filePath))
            .isInstanceOf(InvalidTransferItemException.class)
            .hasMessageContaining("TransferItem exists already");
    }

    @Test
    void processFile_should_reuse_file_if_status_is_COLLECTED() throws Exception {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0-ttv1.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);

        var filenameAttributes = FilenameAttributes.builder()
            .identifier("identifier")
            .dveFilePath(filePath.toString())
            .internalId(1L)
            .build();

        var filesystemAttributes = new FilesystemAttributes(OffsetDateTime.now(), 1234L, "abc");

        var existingTransferItem = TransferItem.builder()
            .id(1L)
            .datasetIdentifier("identifier")
            .transferStatus(TransferItem.TransferStatus.COLLECTED)
            .bagChecksum("abc")
            .build();

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(filenameAttributes))
            .thenReturn(Optional.of(existingTransferItem));

        Mockito.when(transferItemMetadataReader.getFilenameAttributes(filePath))
            .thenReturn(filenameAttributes);

        Mockito.when(transferItemMetadataReader.getFilesystemAttributes(filePath))
            .thenReturn(filesystemAttributes);

        task.processFile(filePath);

        Mockito.verify(transferItemService)
            .moveTransferItem(
                Mockito.eq(existingTransferItem),
                Mockito.eq(TransferItem.TransferStatus.COLLECTED),
                Mockito.eq(filePath),
                Mockito.eq(outbox)
            );
    }

    @Test
    void cleanUpXmlFile_calls_deleteFile() throws Exception {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);
        var testPath = Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.zip");

        Mockito.when(transferItemMetadataReader.getAssociatedXmlFile(filePath))
            .thenReturn(Optional.of(testPath));

        task.cleanUpXmlFile(filePath);
        Mockito.verify(fileService).deleteFile(testPath);
    }

    @Test
    void cleanUpXmlFile_does_not_throw_exception_if_file_doesnt_exist() throws IOException {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);
        var testPath = Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.zip");

        Mockito.when(transferItemMetadataReader.getAssociatedXmlFile(filePath))
            .thenReturn(Optional.of(testPath));

        Mockito.when(fileService.deleteFile(Mockito.any()))
            .thenThrow(IOException.class);

        assertDoesNotThrow(() -> task.cleanUpXmlFile(filePath));
    }

    @Test
    void moveFileToOutbox() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);

        var transferItem = TransferItem.builder()
            .id(15L)
            .datasetIdentifier("doi-10-5072-dar-kxteqtv1.0")
            .build();

        try {
            task.moveFileToOutbox(transferItem, filePath, outbox);
            Mockito.verify(transferItemService).moveTransferItem(transferItem, TransferItem.TransferStatus.COLLECTED, filePath, outbox);
        }
        catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void run_should_call_rejectFile_in_case_of_IOException() throws Exception {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);
        var transferItem = TransferItem.builder()
            .doi("pid1")
            .dveFilePath("path/to1.zip")
            .creationTime(OffsetDateTime.now())
            .bagChecksum("abc")
            .transferStatus(TransferItem.TransferStatus.TARRING)
            .build();

        var filenameAttributes = FilenameAttributes.builder()
            .identifier("identifier")
            .dveFilePath(filePath.toString())
            .internalId(1L)
            .build();

        var filesystemAttributes = new FilesystemAttributes(OffsetDateTime.now(), 1234L, "abc");

        Mockito.when(transferItemMetadataReader.getFilenameAttributes(filePath))
            .thenReturn(filenameAttributes);

        Mockito.when(transferItemMetadataReader.getFilesystemAttributes(filePath))
            .thenReturn(filesystemAttributes);

        Mockito.doThrow(IOException.class)
            .when(fileService).rejectFile(Mockito.any(), Mockito.any());

        Mockito.when(transferItemService.getTransferItemByFilenameAttributes(Mockito.any()))
            .thenReturn(Optional.of(transferItem));

        assertDoesNotThrow(task::run);
        Mockito.verify(fileService, Mockito.times(1)).rejectFile(Mockito.any(), Mockito.any());
    }

}
