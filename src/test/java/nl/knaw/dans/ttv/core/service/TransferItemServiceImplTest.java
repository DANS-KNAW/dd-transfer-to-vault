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
import nl.knaw.dans.ttv.core.dto.ArchiveMetadata;
import nl.knaw.dans.ttv.core.dto.FileContentAttributes;
import nl.knaw.dans.ttv.core.dto.FilenameAttributes;
import nl.knaw.dans.ttv.core.dto.FilesystemAttributes;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TarDAO;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class TransferItemServiceImplTest {

    TransferItemDAO transferItemDao;
    TarDAO tarDAO;
    FilenameAttributes filenameAttributes;
    FilesystemAttributes filesystemAttributes;
    FileContentAttributes fileContentAttributes;
    TransferItemService transferItemService;

    @BeforeEach
    void setUp() {
        transferItemDao = Mockito.mock(TransferItemDAO.class);
        tarDAO = Mockito.mock(TarDAO.class);

        filenameAttributes = new FilenameAttributes();
        filenameAttributes.setDveFilePath("some/file.zip");
        filenameAttributes.setDatasetPid("pid");
        filenameAttributes.setVersionMajor(5);
        filenameAttributes.setVersionMinor(3);

        filesystemAttributes = new FilesystemAttributes();
        filesystemAttributes.setCreationTime(LocalDateTime.now());
        filesystemAttributes.setBagSize(1234L);

        fileContentAttributes = new FileContentAttributes();
        fileContentAttributes.setBagId("bag id");
        fileContentAttributes.setNbn("nbn value");
        fileContentAttributes.setOaiOre(new byte[] { 1, 2 });
        fileContentAttributes.setPidMapping(new byte[] { 3, 4 });
        fileContentAttributes.setDatasetVersion("dv version");
        fileContentAttributes.setBagChecksum("check");
    }

    /**
     * test that all properties are set correctly
     */
    @Test
    void createTransferItem() {

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        try {
            var transferItem = transferItemService.createTransferItem("datastation name", filenameAttributes, filesystemAttributes, fileContentAttributes);

            Assertions.assertEquals(TransferItem.TransferStatus.CREATED, transferItem.getTransferStatus());
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

    /**
     * test that all properties are set correctly
     */
    @Test
    void createTransferItemPartial() {

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        try {
            var transferItem = transferItemService.createTransferItem("datastation name", filenameAttributes, filesystemAttributes);

            Assertions.assertEquals(TransferItem.TransferStatus.CREATED, transferItem.getTransferStatus());
            assertNotNull(transferItem.getQueueDate());
            assertEquals("datastation name", transferItem.getDatasetDvInstance());
            assertEquals(filesystemAttributes.getCreationTime(), transferItem.getCreationTime());
            assertEquals(null, transferItem.getBagChecksum());
            assertEquals(1234L, transferItem.getBagSize());

            assertEquals(null, transferItem.getDatasetVersion());
            assertEquals(null, transferItem.getBagId());
            assertEquals(null, transferItem.getNbn());
            assertEquals(null, transferItem.getOaiOre());
            assertEquals(null, transferItem.getPidMapping());

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

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        assertThrows(InvalidTransferItemException.class, () -> {
            transferItemService.createTransferItem("datastation name", filenameAttributes, filesystemAttributes, fileContentAttributes);
        });
    }

    @Test
    void createDuplicateTransferItemPartial() {
        Mockito.when(transferItemDao.findByDatasetPidAndVersion("pid", 5, 3))
            .thenReturn(Optional.of(new TransferItem()));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        assertThrows(InvalidTransferItemException.class, () -> {
            transferItemService.createTransferItem("datastation name", filenameAttributes, filesystemAttributes);
        });
    }

    @Test
    void moveTransferItem() {
        var newPath = Path.of("new/path.zip");
        var newStatus = TransferItem.TransferStatus.COLLECTED;
        var transferItem = new TransferItem();
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        transferItem = transferItemService.moveTransferItem(transferItem, newStatus, newPath);

        assertEquals("new/path.zip", transferItem.getDveFilePath());
        assertEquals(TransferItem.TransferStatus.COLLECTED, transferItem.getTransferStatus());
    }

    @Test
    void saveAll() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var items = List.of(
            new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.TARRING),
            new TransferItem("pid2", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.TARRING)
        );
        transferItemService.saveAllTransferItems(items);

        Mockito.verify(transferItemDao, Mockito.times(1)).merge(items.get(0));
        Mockito.verify(transferItemDao, Mockito.times(1)).merge(items.get(1));
    }

    @Test
    void updateTarToCreated() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var metadata = new ArchiveMetadata();
        metadata.setParts(List.of(new ArchiveMetadata.ArchiveMetadataPart("ident", "md5", "check")));
        var tar = new Tar();
        var transferItem = new TransferItem();

        tar.setTransferItems(List.of(transferItem));

        Mockito.when(tarDAO.findById(Mockito.any()))
            .thenReturn(Optional.of(tar));

        transferItemService.updateTarToCreated("some_id", metadata);

        assertEquals(Tar.TarStatus.OCFLTARCREATED, tar.getTarStatus());
        // TODO determine the correct status
        assertEquals(TransferItem.TransferStatus.OCFLTARCREATED, transferItem.getTransferStatus());
        assertEquals("ident", tar.getTarParts().get(0).getPartName());
        assertEquals("md5", tar.getTarParts().get(0).getChecksumAlgorithm());
        assertEquals("check", tar.getTarParts().get(0).getChecksumValue());
    }

    @Test
    void findAllTarsToBeConfirmed() {
        var items = List.of(
            new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.TARRING),
            new TransferItem("pid2", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.TARRING),
            new TransferItem("pid3", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.TARRING)
        );

        var tar1 = new Tar(UUID.randomUUID().toString());
        var tar2 = new Tar(UUID.randomUUID().toString());

        items.get(0).setAipsTar(tar1);
        items.get(1).setAipsTar(tar2);
        items.get(2).setAipsTar(tar2);

        Mockito.when(tarDAO.findAllTarsToBeConfirmed())
            .thenReturn(List.of(tar1, tar2));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var result = transferItemService.stageAllTarsToBeConfirmed();

        assertEquals(List.of(tar1, tar2), result);
    }

    @Test
    void updateCheckingProgressResults() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var tar = new Tar(UUID.randomUUID().toString());
        tar.setTransferItems(List.of());
        transferItemService.updateConfirmArchivedResult(tar, Tar.TarStatus.OCFLTARCREATED);

        Mockito.verify(tarDAO, Mockito.times(1)).save(tar);

        //        Mockito.verify(transferItemDao, Mockito.times(1))
        //            .updateCheckingProgressResults("some_id", TransferItem.TransferStatus.OCFLTARCREATED);
    }
}
