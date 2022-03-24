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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class TransferItemServiceImplTest {

    TransferItemDAO transferItemDao;
    TarDAO tarDAO;
    FilenameAttributes filenameAttributes;
    FilesystemAttributes filesystemAttributes;
    FileContentAttributes fileContentAttributes;

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

            Assertions.assertEquals(TransferItem.TransferStatus.COLLECTED, transferItem.getTransferStatus());
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

            Assertions.assertEquals(TransferItem.TransferStatus.COLLECTED, transferItem.getTransferStatus());
            assertNotNull(transferItem.getQueueDate());
            assertEquals("datastation name", transferItem.getDatasetDvInstance());
            assertEquals(filesystemAttributes.getCreationTime(), transferItem.getCreationTime());
            assertNull(transferItem.getBagChecksum());
            assertEquals(1234L, transferItem.getBagSize());

            assertNull(transferItem.getDatasetVersion());
            assertNull(transferItem.getBagId());
            assertNull(transferItem.getNbn());
            assertNull(transferItem.getOaiOre());
            assertNull(transferItem.getPidMapping());

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

        assertThrows(InvalidTransferItemException.class, () -> transferItemService.createTransferItem("name", filenameAttributes, filesystemAttributes, fileContentAttributes));
    }

    @Test
    void createDuplicateTransferItemPartial() {
        Mockito.when(transferItemDao.findByDatasetPidAndVersion("pid", 5, 3))
            .thenReturn(Optional.of(new TransferItem()));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        assertThrows(InvalidTransferItemException.class, () -> transferItemService.createTransferItem("name", filenameAttributes, filesystemAttributes));
    }

    @Test
    void moveTransferItem() {
        var newPath = Path.of("new/path.zip");
        var newStatus = TransferItem.TransferStatus.METADATA_EXTRACTED;
        var transferItem = new TransferItem();
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        transferItem = transferItemService.moveTransferItem(transferItem, newStatus, newPath);

        assertEquals("new/path.zip", transferItem.getDveFilePath());
        assertEquals(TransferItem.TransferStatus.METADATA_EXTRACTED, transferItem.getTransferStatus());
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
        assertEquals(TransferItem.TransferStatus.OCFLTARCREATED, transferItem.getTransferStatus());

        Mockito.verify(tarDAO).saveWithParts(Mockito.any(), Mockito.any());
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
    void getTransferItemByFilenameAttributes() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var attributes = new FilenameAttributes("path", "pid", 5, 3);
        transferItemService.getTransferItemByFilenameAttributes(attributes);

        Mockito.verify(transferItemDao).findByDatasetPidAndVersion("pid", 5, 3);
    }

    @Test
    void addMetadataAndMoveFile() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var attributes = new FileContentAttributes(
            "version",
            "id",
            "nbn",
            new byte[] { 1 },
            new byte[] { 2 },
            "otherid",
            "otheridversion",
            "sword",
            "sword2",
            "checksum"
        );
        var transferItem = new TransferItem();

        Mockito.when(transferItemDao.save(transferItem))
            .thenReturn(transferItem);

        var result = transferItemService.addMetadata(
            transferItem,
            attributes
        );

        assertEquals("version", result.getDatasetVersion());
        assertEquals("id", result.getBagId());
        assertEquals("nbn", result.getNbn());
        assertEquals("otherid", result.getOtherId());
        assertEquals("otheridversion", result.getOtherIdVersion());
        assertEquals("sword", result.getSwordToken());
        assertEquals("checksum", result.getBagChecksum());
        assertArrayEquals(new byte[] { 1 }, result.getOaiOre());
        assertArrayEquals(new byte[] { 2 }, result.getPidMapping());
    }

    @Test
    void createTarArchiveWithAllMetadataExtractedTransferItems() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var uuid = UUID.randomUUID().toString();
        var items = List.of(
            new TransferItem("pid", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED),
            new TransferItem("pid2", 1, 0, "path", LocalDateTime.now(), TransferItem.TransferStatus.METADATA_EXTRACTED)
        );

        Mockito.when(transferItemDao.findByStatus(TransferItem.TransferStatus.METADATA_EXTRACTED))
            .thenReturn(items);

        Mockito.when(tarDAO.save(Mockito.any()))
            .then(a -> a.getArguments()[0]);

        var tar = transferItemService.createTarArchiveWithAllMetadataExtractedTransferItems(uuid, "some-path");

        assertEquals(uuid, tar.getTarUuid());
        assertEquals(Tar.TarStatus.TARRING, tar.getTarStatus());
        assertNotNull(tar.getCreated());
        assertEquals("some-path", tar.getVaultPath());

        assertEquals(tar, items.get(0).getAipsTar());
        assertEquals(tar, items.get(1).getAipsTar());
    }

    @Test
    void resetTarToArchiving() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var tar = new Tar();
        tar.setConfirmCheckInProgress(true);
        transferItemService.resetTarToArchiving(tar);

        assertEquals(Tar.TarStatus.OCFLTARCREATED, tar.getTarStatus());
        assertFalse(tar.isConfirmCheckInProgress());
    }

    @Test
    void updateTarToArchived() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var tar = new Tar();
        tar.setConfirmCheckInProgress(true);
        transferItemService.updateTarToArchived(tar);

        assertEquals(Tar.TarStatus.CONFIRMEDARCHIVED, tar.getTarStatus());
        assertFalse(tar.isConfirmCheckInProgress());
        assertNotNull(tar.getDatetimeConfirmedArchived());
    }

    @Test
    void setArchiveAttemptFailed() {
        var tar = new Tar();
        tar.setTransferAttempt(0);

        Mockito.when(tarDAO.findById(Mockito.any())).thenReturn(Optional.of(tar));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        transferItemService.setArchiveAttemptFailed("id", true, 2);

        Mockito.verify(tarDAO).save(tar);

        assertEquals(1, tar.getTransferAttempt());
        assertEquals(Tar.TarStatus.TARRING, tar.getTarStatus());
    }

    @Test
    void setArchiveAttemptFailedWithMaxRetries() {
        var tar = new Tar();
        tar.setTransferAttempt(0);

        Mockito.when(tarDAO.findById(Mockito.any())).thenReturn(Optional.of(tar));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        // this is called after the initial attempt
        transferItemService.setArchiveAttemptFailed("id", true, 2);
        assertEquals(1, tar.getTransferAttempt());
        assertEquals(Tar.TarStatus.TARRING, tar.getTarStatus());

        // this is called after the first retry
        transferItemService.setArchiveAttemptFailed("id", true, 2);
        assertEquals(2, tar.getTransferAttempt());
        assertEquals(Tar.TarStatus.TARRING, tar.getTarStatus());

        // this is called after the second retry, and because maxRetries equals 2, there will not be a third retry
        transferItemService.setArchiveAttemptFailed("id", true, 2);
        assertEquals(3, tar.getTransferAttempt());
        assertEquals(Tar.TarStatus.OCFLTARFAILED, tar.getTarStatus());
    }

    @Test
    void setArchiveAttemptFailedWithoutCount() {
        var tar = new Tar();
        tar.setTransferAttempt(0);

        Mockito.when(tarDAO.findById(Mockito.any())).thenReturn(Optional.of(tar));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        transferItemService.setArchiveAttemptFailed("id", false, 2);
        assertEquals(0, tar.getTransferAttempt());
        assertEquals(Tar.TarStatus.TARRING, tar.getTarStatus());

    }
}
