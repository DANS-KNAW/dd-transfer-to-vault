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
import nl.knaw.dans.ttv.core.dto.FileContentAttributes;
import nl.knaw.dans.ttv.core.dto.FilenameAttributes;
import nl.knaw.dans.ttv.core.dto.FilesystemAttributes;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class TransferItemServiceImplTest {

    TransferItemDAO transferItemDao;
    FilenameAttributes filenameAttributes;
    FilesystemAttributes filesystemAttributes;
    FileContentAttributes fileContentAttributes;

    @BeforeEach
    void setUp() {
        transferItemDao = Mockito.mock(TransferItemDAO.class);

        filenameAttributes = new FilenameAttributes();
        filenameAttributes.setDveFilePath("some/file.zip");
        filenameAttributes.setDatasetPid("pid");
        filenameAttributes.setVersionMajor(5);
        filenameAttributes.setVersionMinor(3);

        filesystemAttributes = new FilesystemAttributes();
        filesystemAttributes.setCreationTime(LocalDateTime.now());
        filesystemAttributes.setBagChecksum("check");
        filesystemAttributes.setBagSize(1234L);

        fileContentAttributes = new FileContentAttributes();
        fileContentAttributes.setBagId("bag id");
        fileContentAttributes.setNbn("nbn value");
        fileContentAttributes.setOaiOre(new byte[] { 1, 2 });
        fileContentAttributes.setPidMapping(new byte[] { 3, 4 });
        fileContentAttributes.setDatasetVersion("dv version");
    }

    /**
     * test that all properties are set correctly
     */
    @Test
    void createTransferItem() {

        var transferItemService = new TransferItemServiceImpl(transferItemDao);

        try {
            var transferItem = transferItemService.createTransferItem("datastation name", filenameAttributes, filesystemAttributes, fileContentAttributes);

            Assertions.assertEquals(TransferItem.TransferStatus.EXTRACT, transferItem.getTransferStatus());
            assertNotNull(transferItem.getQueueDate());
            assertEquals("datastation name", transferItem.getDatasetDvInstance());

            assertEquals(filesystemAttributes.getCreationTime(), transferItem.getCreationTime());
            assertEquals("check", transferItem.getBagChecksum());
            assertEquals(1234L, transferItem.getBagSize());

            assertEquals("dv version", transferItem.getDatasetVersion());
            assertEquals("bag id", transferItem.getBagId());
            assertEquals("nbn value", transferItem.getNbn());
            assertEquals(2, transferItem.getOaiOre().length);
            assertEquals(2, transferItem.getPidMapping().length);
            assertEquals(1, transferItem.getOaiOre()[0]);
            assertEquals(2, transferItem.getOaiOre()[1]);
            assertEquals(3, transferItem.getPidMapping()[0]);
            assertEquals(4, transferItem.getPidMapping()[1]);

            Mockito.verify(transferItemDao).save(transferItem);
        }
        catch (InvalidTransferItemException e) {
            fail(e);
        }
    }

    @Test
    void createDuplicateTransferItem() {
        Mockito.when(transferItemDao.findByDatasetPidAndVersion("pid", 5, 3))
            .thenReturn(Optional.of(new TransferItem()));

        var transferItemService = new TransferItemServiceImpl(transferItemDao);

        assertThrows(InvalidTransferItemException.class, () -> {
            transferItemService.createTransferItem("datastation name", filenameAttributes, filesystemAttributes, fileContentAttributes);
        });
    }

    @Test
    void moveTransferItem() {
        var newPath = Path.of("new/path.zip");
        var newStatus = TransferItem.TransferStatus.COLLECTED;
        var transferItem = new TransferItem();
        var transferItemService = new TransferItemServiceImpl(transferItemDao);

        transferItem = transferItemService.moveTransferItem(transferItem, newStatus, newPath);

        assertEquals("new/path.zip", transferItem.getDveFilePath());
        assertEquals(TransferItem.TransferStatus.COLLECTED, transferItem.getTransferStatus());
    }
}
