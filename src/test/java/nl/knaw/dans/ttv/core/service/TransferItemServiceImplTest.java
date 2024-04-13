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
package nl.knaw.dans.ttv.core.service;

import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import nl.knaw.dans.ttv.core.TransferItem;
import nl.knaw.dans.ttv.core.domain.FileContentAttributes;
import nl.knaw.dans.ttv.core.domain.FilenameAttributes;
import nl.knaw.dans.ttv.core.domain.FilesystemAttributes;
import nl.knaw.dans.ttv.db.TransferItemDao;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.OffsetDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class TransferItemServiceImplTest {

    TransferItemDao transferItemDao;
    FileService fileService;
    FilenameAttributes filenameAttributes;
    FilesystemAttributes filesystemAttributes;
    FileContentAttributes fileContentAttributes;

    @BeforeEach
    void setUp() {
        transferItemDao = Mockito.mock(TransferItemDao.class);
        fileService = Mockito.mock(FileService.class);

        filenameAttributes = FilenameAttributes
            .builder()
            .dveFilePath("some/file.zip")
            .dveFilename("pid")
            .build();

        filesystemAttributes = new FilesystemAttributes(OffsetDateTime.now(), 1234L, "abc123");

        fileContentAttributes = FileContentAttributes.builder()
            .bagId("bag id")
            .nbn("nbn value")
            .metadata("{}")
            .filePidToLocalPath("string\nline2\n")
            .dataversePidVersion("5.3")
            .build();
    }

    /**
     * test that all properties are set correctly
     */
    @Test
    void createTransferItemPartial() {

        var transferItemService = new TransferItemServiceImpl(transferItemDao);

        try {
            var transferItem = transferItemService.createTransferItem("datastation name", filenameAttributes, filesystemAttributes);

            Assertions.assertEquals(TransferItem.TransferStatus.COLLECTED, transferItem.getTransferStatus());
            assertEquals("datastation name", transferItem.getDatastation());
            assertEquals(filesystemAttributes.getCreationTime(), transferItem.getCreationTime());
            assertEquals("abc123", transferItem.getBagSha256Checksum());
            assertEquals(1234L, transferItem.getBagSize());

            assertNull(transferItem.getDataversePidVersion());
            assertNull(transferItem.getBagId());
            assertNull(transferItem.getNbn());
            assertNull(transferItem.getMetadata());
            assertNull(transferItem.getFilePidToLocalPath());

            Mockito.verify(transferItemDao).save(transferItem);
        }
        catch (InvalidTransferItemException e) {
            fail(e);
        }
    }

     @Test
    void createDuplicateTransferItemPartial() {
        var existing = TransferItem.builder()
            .dveFilename("pid")
            .bagSha256Checksum(filesystemAttributes.getChecksum())
            .build();

        Mockito.when(transferItemDao.findByIdentifier("pid"))
            .thenReturn(Optional.of(existing));

        var transferItemService = getTransferItemService();

        assertThrows(InvalidTransferItemException.class, () ->
            transferItemService.createTransferItem("name", filenameAttributes, filesystemAttributes)
        );
    }

    @Test
    void getTransferItemByFilenameAttributes() {
        var transferItemService = getTransferItemService();
        var attributes = FilenameAttributes.builder()
            .dveFilePath("path")
            .dveFilename("pid")
            .build();

        transferItemService.getTransferItemByFilenameAttributes(attributes);

        Mockito.verify(transferItemDao).findByIdentifier("pid");
    }

    @Test
    void addMetadataAndMoveFile() {
        var transferItemService = getTransferItemService();
        var attributes = FileContentAttributes.builder()
            .dataversePidVersion("1.0")
            .bagId("id")
            .nbn("nbn")
            .metadata("{}")
            .filePidToLocalPath("a  b")
            .otherId("otherId")
            .otherIdVersion("otherIdVersion")
            .dataSupplier("swordClient")
            .swordToken("swordToken")
            .build();

        var transferItem = new TransferItem();

        Mockito.when(transferItemDao.save(transferItem))
            .thenReturn(transferItem);

        var result = transferItemService.addMetadata(
            transferItem,
            attributes
        );

        assertEquals("1.0", result.getDataversePidVersion());
        assertEquals("id", result.getBagId());
        assertEquals("nbn", result.getNbn());
        assertEquals("otherId", result.getOtherId());
        assertEquals("otherIdVersion", result.getOtherIdVersion());
        assertEquals("swordToken", result.getSwordToken());
        assertEquals("swordClient", result.getDataSupplier());
        assertNull(result.getBagSha256Checksum());
        assertEquals("{}", result.getMetadata());
        assertEquals("a  b", result.getFilePidToLocalPath());
    }

    @Test
    void getTransferItemByFilenameAttributes_should_return_matching_TransferItem_if_ID_is_set() throws Exception {
        var transferItemService = getTransferItemService();
        var attributes = FilenameAttributes.builder()
            .ocflObjectVersionNumber(1)
            .dveFilePath("path")
            .dveFilename("pid")
            .build();

        var transferItem = TransferItem.builder()
            .id(1L)
            .dataversePid("pid")
            .dveFilePath("path")
            .build();

        Mockito.when(transferItemDao.findById(1L))
            .thenReturn(Optional.of(transferItem));
        Mockito.when(transferItemDao.findByIdentifier("pid"))
            .thenReturn(Optional.of(transferItem));

        var result = transferItemService.getTransferItemByFilenameAttributes(attributes);

        assertThat(result).isNotEmpty();
    }

    @Test
    void getTransferItemByFilenameAttributes_should_return_TransferItem_if_ID_is_null() throws Exception {
        var transferItemService = getTransferItemService();
        var attributes = FilenameAttributes.builder()
            .dveFilePath("path")
            .dveFilename("pid")
            .build();

        var transferItem = TransferItem.builder()
            .id(1L)
            .dataversePid("pid")
            .dveFilePath("path")
            .build();

        Mockito.when(transferItemDao.findByIdentifier("pid"))
            .thenReturn(Optional.of(transferItem));

        var result = transferItemService.getTransferItemByFilenameAttributes(attributes);

        assertThat(result).isNotEmpty();
    }

    TransferItemService getTransferItemService() {
        return new TransferItemServiceImpl(transferItemDao);
    }
}
