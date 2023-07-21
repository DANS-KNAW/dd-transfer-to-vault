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

import io.ocfl.api.OcflOption;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.model.VersionInfo;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OcflRepositoryServiceImplTest {

    private FileService fileService;
    private OcflRepositoryFactory ocflRepositoryFactory;

    @BeforeEach
    void setUp() {
        fileService = Mockito.mock(FileService.class);
        ocflRepositoryFactory = Mockito.mock(OcflRepositoryFactory.class);
    }

    @Test
    void importTransferItem() {
        var repository = Mockito.mock(OcflRepository.class);
        var transferItem = TransferItem.builder()
            .ocflObjectVersion(1)
            .dataversePid("pid1")
            .dveFilePath("path/to/dir")
            .creationTime(OffsetDateTime.now())
            .transferStatus(TransferItem.TransferStatus.METADATA_EXTRACTED)
            .build();
        transferItem.setBagId("urn:uuid:a175c497-9a42-4832-9e71-626db678ed1b");
        var service = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);

        var objectId = service.importTransferItem(repository, transferItem);
        assertEquals("urn:uuid:a1/75/c4/97-9a42-4832-9e71-626db678ed1b", objectId);

        Mockito.verify(repository).putObject(
            Mockito.any(),
            Mockito.eq(Path.of("path/to/dir")),
            Mockito.eq(new VersionInfo().setMessage("initial commit")),
            Mockito.eq(OcflOption.MOVE_SOURCE)
        );
    }

    @Test
    void closeOcflRepository() throws IOException {
        var repository = Mockito.mock(OcflRepository.class);
        var service = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);
        service.closeOcflRepository(repository, Path.of("test/path"));

        Mockito.verify(repository).close();
    }

    @Test
    void getObjectIdForTransferItem() {
        var service = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);

        assertEquals("urn:uuid:b0/b8/0c/cd-504a-4167-8d80-90ee6c478b46",
            service.getObjectIdForBagId("urn:uuid:b0b80ccd-504a-4167-8d80-90ee6c478b46"));

        // spaces
        assertEquals("urn:uuid:b0/b8/0c/cd-504a-4167-8d80-90ee6c478b46",
            service.getObjectIdForBagId("  urn:uuid:b0b80ccd-504a-4167-8d80-90ee6c478b46  "));

        assertThrows(IllegalArgumentException.class, () -> {
            service.getObjectIdForBagId("Xrn:uuid:b0b80ccd-504a-4167-8d80-90ee6c478b46");
        });
    }
}
