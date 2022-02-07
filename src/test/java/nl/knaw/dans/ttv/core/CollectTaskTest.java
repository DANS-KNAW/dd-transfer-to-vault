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

import nl.knaw.dans.ttv.core.dto.FilenameAttributes;
import nl.knaw.dans.ttv.core.service.FileService;
import nl.knaw.dans.ttv.core.service.TransferItemMetadataReader;
import nl.knaw.dans.ttv.core.service.TransferItemService;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Optional;

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
    void checkAllFilesAreMoved() throws IOException, InvalidTransferItemException {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");
        var workDir = Path.of("data/workdir");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);

        Mockito.when(transferItemMetadataReader.getAssociatedXmlFile(Mockito.any()))
            .thenReturn(Optional.of(Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.xml")));

        Mockito.when(transferItemService.createTransferItem(Mockito.any(), Mockito.any()))
            .thenReturn(new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.CREATED));

        task.run();

        // check that file was moved from inbox to workdir
        Mockito.verify(fileService).moveFileAtomically(filePath, Path.of("data/outbox/doi-10-5072-dar-kxteqtv1.0.zip"));

        // check that xml file was removed
        Mockito.verify(fileService).deleteFile(Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.xml"));

        Mockito.verify(transferItemService)
            .createTransferItem(Mockito.eq("dsname"), Mockito.any());

        Mockito.verify(transferItemService)
            .moveTransferItem(Mockito.any(), Mockito.eq(TransferItem.TransferStatus.CREATED),
                Mockito.eq(Path.of("data/outbox/doi-10-5072-dar-kxteqtv1.0.zip")));
    }

    @Test
    void createTransferItem() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);

        try {
            var filenameAttributes = new FilenameAttributes();

            Mockito.when(transferItemMetadataReader.getFilenameAttributes(filePath))
                .thenReturn(filenameAttributes);

            task.createOrGetTransferItem(filePath);

            Mockito.verify(transferItemService).createTransferItem("dsname", filenameAttributes);
            Mockito.verify(transferItemMetadataReader).getFilenameAttributes(filePath);
        }
        catch (Exception e) {
            fail(e);
        }
    }

    @Test
    void cleanUpXmlFile() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var workDir = Path.of("data/workdir");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);
        var testPath = Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.zip");

        Mockito.when(transferItemMetadataReader.getAssociatedXmlFile(filePath))
            .thenReturn(Optional.of(testPath));

        try {
            task.cleanUpXmlFile(filePath);
            Mockito.verify(fileService).deleteFile(testPath);
        }
        catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void moveFileToOutbox() {
        var filePath = Path.of("data/inbox/doi-10-5072-dar-kxteqtv1.0.zip");
        var workDir = Path.of("data/workdir");
        var outbox = Path.of("data/outbox");
        var datastationName = "dsname";

        var task = new CollectTask(filePath, outbox, datastationName, transferItemService, transferItemMetadataReader, fileService);
        //        var testPath = Path.of("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.zip");

        var transferItem = new TransferItem();

        try {
            var targetFile = Path.of("data/outbox/doi-10-5072-dar-kxteqtv1.0.zip");
            task.moveFileToOutbox(transferItem, filePath, outbox);
            Mockito.verify(fileService).moveFileAtomically(filePath, targetFile);
            Mockito.verify(transferItemService).moveTransferItem(transferItem, TransferItem.TransferStatus.CREATED, targetFile);
        }
        catch (IOException e) {
            fail(e);
        }
    }
}
