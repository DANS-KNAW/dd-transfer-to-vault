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

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import nl.knaw.dans.ttv.db.TransferItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

class OcflRepositoryServiceImplTest {

    private FileService fileService;
    private OcflRepositoryFactory ocflRepositoryFactory;

    @BeforeEach
    void setUp() {
        fileService = Mockito.mock(FileService.class);
        ocflRepositoryFactory = Mockito.mock(OcflRepositoryFactory.class);
    }

    @Test
    void createRepository() {
        var service = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);

        try {
            Mockito.when(fileService.createDirectory(Mockito.any()))
                .thenReturn(Path.of("primary-dir"))
                .thenReturn(Path.of("working-dir"));

            service.createRepository(Path.of("some/path"), "uuid");
            Mockito.verify(ocflRepositoryFactory).createRepository(Path.of("primary-dir"), Path.of("working-dir"));
        }
        catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void importTransferItem() {
        var repository = Mockito.mock(OcflRepository.class);
        var transferItem = new TransferItem("pid", 1, 0, "path/to/dir", LocalDateTime.now(), TransferItem.TransferStatus.COLLECTED);
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
    void closeOcflRepository() {
        var repository = Mockito.mock(OcflRepository.class);
        var service = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);
        service.closeOcflRepository(repository);

        Mockito.verify(repository).close();
    }

    @Test
    void cleanupRepository() {
        var service = new OcflRepositoryServiceImpl(fileService, ocflRepositoryFactory);

        try {
            service.cleanupRepository(Path.of("some/path/123"), "123");

            Mockito.verify(fileService, Mockito.times(1))
                .deleteDirectory(Path.of("some/path/123/123"));

            Mockito.verify(fileService, Mockito.times(1))
                .deleteDirectory(Path.of("some/path/123/123-wd"));
        }
        catch (IOException e) {
            fail(e);
        };

    }
}
