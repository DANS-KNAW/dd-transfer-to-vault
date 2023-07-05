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
import nl.knaw.dans.ttv.core.domain.*;
import nl.knaw.dans.ttv.db.Tar;
import nl.knaw.dans.ttv.db.TarDAO;
import nl.knaw.dans.ttv.db.TransferItem;
import nl.knaw.dans.ttv.db.TransferItemDAO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

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

        filenameAttributes = FilenameAttributes
            .builder()
            .dveFilePath("some/file.zip")
            .datasetPid("pid")
            .version(Version.of(5, 3))
            .build();

        filesystemAttributes = new FilesystemAttributes(OffsetDateTime.now(), 1234L);

        fileContentAttributes = FileContentAttributes.builder()
            .bagId("bag id")
            .nbn("nbn value")
            .oaiOre("{}")
            .pidMapping("string\nline2\n")
            .datasetVersion("5.3")
            .bagChecksum("check")
            .build();
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

            assertEquals("5.3", transferItem.getDatasetVersion());
            assertEquals("bag id", transferItem.getBagId());
            assertEquals("nbn value", transferItem.getNbn());
            assertEquals("{}", transferItem.getOaiOre());
            assertEquals("string\nline2\n", transferItem.getPidMapping());

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
        Mockito.when(transferItemDao.findByDatasetPidAndVersion("pid", Version.of(5, 3)))
            .thenReturn(Optional.of(new TransferItem()));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);

        assertThrows(InvalidTransferItemException.class, () ->
            transferItemService.createTransferItem("name", filenameAttributes, filesystemAttributes, fileContentAttributes));
    }

    @Test
    void createDuplicateTransferItemPartial() {
        Mockito.when(transferItemDao.findByDatasetPidAndVersion("pid", Version.of(5, 3)))
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
            TransferItem.builder()
                .datasetPid("pid1")
                .dveFilePath("path")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build(),
            TransferItem.builder()
                .datasetPid("pid2")
                .dveFilePath("path")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build()
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
            TransferItem.builder()
                .datasetPid("pid1")
                .dveFilePath("path")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build(),
            TransferItem.builder()
                .datasetPid("pid2")
                .dveFilePath("path")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build(),
            TransferItem.builder()
                .datasetPid("pid3")
                .dveFilePath("path")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.TARRING)
                .build()
        );

        var tar1 = new Tar(UUID.randomUUID().toString());
        var tar2 = new Tar(UUID.randomUUID().toString());

        items.get(0).setTar(tar1);
        items.get(1).setTar(tar2);
        items.get(2).setTar(tar2);

        Mockito.when(tarDAO.findAllTarsToBeConfirmed())
            .thenReturn(List.of(tar1, tar2));

        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var result = transferItemService.stageAllTarsToBeConfirmed();

        assertEquals(List.of(tar1, tar2), result);
    }

    @Test
    void getTransferItemByFilenameAttributes() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var attributes = FilenameAttributes.builder()
            .dveFilePath("path")
            .datasetPid("pid")
            .version(Version.of(5, 3))
            .build();

        transferItemService.getTransferItemByFilenameAttributes(attributes);

        Mockito.verify(transferItemDao).findByDatasetPidAndVersion("pid", Version.of(5, 3));
    }

    @Test
    void addMetadataAndMoveFile() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var attributes = FileContentAttributes.builder()
            .datasetVersion("1.0")
            .bagId("id")
            .nbn("nbn")
            .oaiOre("{}")
            .pidMapping("a  b")
            .otherId("otherId")
            .otherIdVersion("otherIdVersion")
            .swordClient("swordClient")
            .swordToken("swordToken")
            .bagChecksum("bagChecksum")
            .build();

        var transferItem = new TransferItem();

        Mockito.when(transferItemDao.save(transferItem))
            .thenReturn(transferItem);

        var result = transferItemService.addMetadata(
            transferItem,
            attributes
        );

        assertEquals("1.0", result.getDatasetVersion());
        assertEquals("id", result.getBagId());
        assertEquals("nbn", result.getNbn());
        assertEquals("otherId", result.getOtherId());
        assertEquals("otherIdVersion", result.getOtherIdVersion());
        assertEquals("swordToken", result.getSwordToken());
        assertEquals("swordClient", result.getSwordClient());
        assertEquals("bagChecksum", result.getBagChecksum());
        assertEquals("{}", result.getOaiOre());
        assertEquals("a  b", result.getPidMapping());
    }

    @Test
    void createTarArchiveWithAllMetadataExtractedTransferItems() {
        var transferItemService = new TransferItemServiceImpl(transferItemDao, tarDAO);
        var uuid = UUID.randomUUID().toString();
        var items = List.of(
            TransferItem.builder()
                .datasetPid("pid1")
                .dveFilePath("path")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build(),
            TransferItem.builder()
                .datasetPid("pid2")
                .dveFilePath("path")
                .creationTime(OffsetDateTime.now())
                .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
                .build()
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

        assertEquals(tar, items.get(0).getTar());
        assertEquals(tar, items.get(1).getTar());
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
