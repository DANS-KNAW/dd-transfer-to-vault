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
import nl.knaw.dans.ttv.core.oaiore.OaiOreMetadataReader;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TransferItemMetadataReaderImplTest {

    private final OaiOreMetadataReader oaiOreMetadataReader = new OaiOreMetadataReader();

    @Test
    void getFilenameAttributes() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);
        try {
            var path = Path.of("doi-10-5072-dar-kxteqtv1.0.zip");
            var attributes = service.getFilenameAttributes(path);
            assertEquals(path.toString(), attributes.getDveFilePath());
            assertEquals("doi-10-5072-dar-kxteqtv1.0.zip", attributes.getDveFilename());
        }
        catch (InvalidTransferItemException e) {
            fail(e);
        }
    }

    @Test
    void getFilenameAttributes_with_version_should_not_include_version_in_result() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);
        try {
            var path = Path.of("doi-10-5072-dar-kxteqtv1.0-ttv3.zip");
            var attributes = service.getFilenameAttributes(path);
            assertEquals(path.toString(), attributes.getDveFilePath());
            assertEquals("doi-10-5072-dar-kxteqtv1.0.zip", attributes.getDveFilename());
        }
        catch (InvalidTransferItemException e) {
            fail(e);
        }
    }



    @Test
    void getFilesystemAttributes() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);

        try {
            var path = Path.of("doi-10-5072-dar-kxteqtv1.0.zip");
            var ts = Instant.now();

            Mockito.when(fileService.calculateChecksum(path))
                .thenReturn("checksum-test");

            Mockito.when(fileService.getFileSize(path))
                .thenReturn(1234L);

            Mockito.when(fileService.getFilesystemAttribute(path, "creationTime"))
                .thenReturn(FileTime.from(ts));

            var attributes = service.getFilesystemAttributes(path);

            assertEquals(1234L, attributes.getBagSize());
            assertEquals(OffsetDateTime.ofInstant(ts, ZoneId.systemDefault()), attributes.getCreationTime());
        }
        catch (InvalidTransferItemException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getFilesystemAttributesWithIOError() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);

        try {
            var path = Path.of("doi-10-5072-dar-kxteqtv1.0.zip");

            Mockito.when(fileService.getFilesystemAttribute(path, "creationTime"))
                .thenThrow(IOException.class);

            assertThrows(InvalidTransferItemException.class, () -> service.getFilesystemAttributes(path));
        }
        catch (IOException e) {
            fail(e);
        }
    }

    /**
     * test that IOExceptions are propagated
     *
     * @throws IOException
     */
    @Test
    void getFileContentAttributesWithIOException() throws IOException {
        var fileService = Mockito.mock(FileService.class);
        Mockito.when(fileService.openZipFile(Mockito.any()))
            .thenThrow(IOException.class);

        var service = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);

        assertThrows(InvalidTransferItemException.class, () -> service.getFileContentAttributes(Path.of("test.zip")));
    }

    @Test
    void getAssociatedXmlFile() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);
        var filename = "data/inbox/doi-10-5072-dar-kxteqtv1.0.zip";

        var result = service.getAssociatedXmlFile(Path.of(filename));

        assertTrue(result.isPresent());
        assertEquals("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.xml", result.get().toString());
    }

    @Test
    void getAssociatedXmlFileForInvalidFilename() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(fileService, oaiOreMetadataReader);
        // it has doa instead of doi
        var filename = "data/inbox/doa-10-5072-dar-kxteqtv1.0.zip";

        var result = service.getAssociatedXmlFile(Path.of(filename));

        assertTrue(result.isEmpty());
    }
}
