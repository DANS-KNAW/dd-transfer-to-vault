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

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.knaw.dans.ttv.core.InvalidTransferItemException;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TransferItemMetadataReaderImplTest {

    @Test
    void getFilenameAttributes() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);
        try {
            var path = Path.of("doi-10-5072-dar-kxteqtv1.0.zip");
            var attributes = service.getFilenameAttributes(path);
            assertEquals(0, attributes.getVersionMinor());
            assertEquals(1, attributes.getVersionMajor());
            assertEquals(path.toString(), attributes.getDveFilePath());
            assertEquals("10.5072/DAR/KXTEQT", attributes.getDatasetPid());
        }
        catch (InvalidTransferItemException e) {
            fail(e);
        }
    }

    @Test
    void getFilenameAttributesInvalidFilename() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);
        var path = Path.of("something.zip");
        assertThrows(InvalidTransferItemException.class, () -> service.getFilenameAttributes(path));
    }

    @Test
    void getFilenameAttributesInvalidExtension() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);
        var path = Path.of("doi-10-5072-dar-kxteqtv1.0.rar");
        assertThrows(InvalidTransferItemException.class, () -> service.getFilenameAttributes(path));
    }

    @Test
    void getFilesystemAttributes() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);

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
            assertEquals(LocalDateTime.ofInstant(ts, ZoneId.systemDefault()), attributes.getCreationTime());
        }
        catch (InvalidTransferItemException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getFilesystemAttributesWithIOError() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);

        try {
            var path = Path.of("doi-10-5072-dar-kxteqtv1.0.zip");
            var ts = Instant.now();

            Mockito.when(fileService.getFilesystemAttribute(path, "creationTime"))
                .thenThrow(IOException.class);

            assertThrows(InvalidTransferItemException.class, () -> service.getFilesystemAttributes(path));
        }
        catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void getFileContentAttributes() throws IOException, InvalidTransferItemException {
        var fileService = Mockito.mock(FileService.class);
        var fakeJsonld = "{\n"
            + "  \"ore:describes\": {\n"
            + "    \"dansDataVaultMetadata:Bag ID\": \"urn:uuid:a175c497-9a42-4832-9e71-626db678ed1b\",\n"
            + "    \"dansDataVaultMetadata:DV PID Version\": \"1.0\",\n"
            + "    \"dansDataVaultMetadata:DV PID\": \"doi:10.5072/DAR/XZNG4N\",\n"
            + "    \"dansDataVaultMetadata:NBN\": \"urn:nbn:nl:ui:13-39614943-e5b0-48c6-9383-731ef74cc0e9\"\n"
            + "  }\n"
            + "}";
        var fakePidmapping = "abc";

        Mockito.when(fileService.openFileFromZip(Mockito.any(), Mockito.eq(Path.of("metadata/oai-ore.jsonld"))))
            .thenReturn(new ByteArrayInputStream(fakeJsonld.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(fileService.openFileFromZip(Mockito.any(), Mockito.eq(Path.of("metadata/pid-mapping.txt"))))
            .thenReturn(new ByteArrayInputStream(fakePidmapping.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(fileService.calculateChecksum(Mockito.any()))
            .thenReturn("checksum-test");

        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);
        var result = service.getFileContentAttributes(Path.of("test.zip"));

        assertEquals("urn:uuid:a175c497-9a42-4832-9e71-626db678ed1b", result.getBagId());
        assertEquals("1.0", result.getDatasetVersion());
        assertEquals("urn:nbn:nl:ui:13-39614943-e5b0-48c6-9383-731ef74cc0e9", result.getNbn());
        assertNull(result.getOtherId());
        assertNull(result.getOtherIdVersion());
        assertNull(result.getSwordToken());
        assertArrayEquals(fakeJsonld.getBytes(StandardCharsets.UTF_8), result.getOaiOre());
        assertArrayEquals(fakePidmapping.getBytes(StandardCharsets.UTF_8), result.getPidMapping());
        assertEquals("checksum-test", result.getBagChecksum());
    }

    @Test
    void getFileContentAttributesWithOptionalFields() throws IOException, InvalidTransferItemException {
        var fileService = Mockito.mock(FileService.class);
        var fakeJsonld = "{\n"
            + "  \"ore:describes\": {\n"
            + "    \"dansDataVaultMetadata:Bag ID\": \"urn:uuid:a175c497-9a42-4832-9e71-626db678ed1b\",\n"
            + "    \"dansDataVaultMetadata:DV PID Version\": \"1.0\",\n"
            + "    \"dansDataVaultMetadata:DV PID\": \"doi:10.5072/DAR/XZNG4N\",\n"
            + "    \"dansDataVaultMetadata:NBN\": \"urn:nbn:nl:ui:13-39614943-e5b0-48c6-9383-731ef74cc0e9\",\n"
            + "    \"dansDataVaultMetadata:Other ID\": \"39614943-e5b0-48c6-9383-731ef74cc0e9\",\n"
            + "    \"dansDataVaultMetadata:Other ID Version\": \"1.2\",\n"
            + "    \"dansDataVaultMetadata:SWORD Token\": \"token\"\n"
            + "  }\n"
            + "}";
        var fakePidmapping = "abc";

        Mockito.when(fileService.openFileFromZip(Mockito.any(), Mockito.eq(Path.of("metadata/oai-ore.jsonld"))))
            .thenReturn(new ByteArrayInputStream(fakeJsonld.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(fileService.openFileFromZip(Mockito.any(), Mockito.eq(Path.of("metadata/pid-mapping.txt"))))
            .thenReturn(new ByteArrayInputStream(fakePidmapping.getBytes(StandardCharsets.UTF_8)));

        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);
        var result = service.getFileContentAttributes(Path.of("test.zip"));

        assertEquals("urn:uuid:a175c497-9a42-4832-9e71-626db678ed1b", result.getBagId());
        assertEquals("1.0", result.getDatasetVersion());
        assertEquals("urn:nbn:nl:ui:13-39614943-e5b0-48c6-9383-731ef74cc0e9", result.getNbn());
        assertEquals("39614943-e5b0-48c6-9383-731ef74cc0e9", result.getOtherId());
        assertEquals("1.2", result.getOtherIdVersion());
        assertEquals("token", result.getSwordToken());
    }

    /**
     * Test what happens if `DV PID Version` is missing from the JSON
     *
     * @throws IOException
     */
    @Test
    void getFileContentAttributesWithMissingFields() throws IOException {
        var fileService = Mockito.mock(FileService.class);
        var fakeJsonld = "{\n"
            + "  \"ore:describes\": {\n"
            + "    \"dansDataVaultMetadata:Bag ID\": \"urn:uuid:a175c497-9a42-4832-9e71-626db678ed1b\",\n"
            + "    \"dansDataVaultMetadata:DV PID\": \"doi:10.5072/DAR/XZNG4N\",\n"
            + "    \"dansDataVaultMetadata:NBN\": \"urn:nbn:nl:ui:13-39614943-e5b0-48c6-9383-731ef74cc0e9\"\n"
            + "  }\n"
            + "}";
        var fakePidmapping = "abc";

        Mockito.when(fileService.openFileFromZip(Mockito.any(), Mockito.eq(Path.of("metadata/oai-ore.jsonld"))))
            .thenReturn(new ByteArrayInputStream(fakeJsonld.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(fileService.openFileFromZip(Mockito.any(), Mockito.eq(Path.of("metadata/pid-mapping.txt"))))
            .thenReturn(new ByteArrayInputStream(fakePidmapping.getBytes(StandardCharsets.UTF_8)));

        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);

        assertThrows(InvalidTransferItemException.class, () -> service.getFileContentAttributes(Path.of("test.zip")));
    }

    /**
     * test that IOExceptions are propagated
     * @throws IOException
     */
    @Test
    void getFileContentAttributesWithIOException() throws IOException {
        var fileService = Mockito.mock(FileService.class);
        Mockito.when(fileService.openZipFile(Mockito.any()))
                .thenThrow(IOException.class);

        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);

        assertThrows(InvalidTransferItemException.class, () -> service.getFileContentAttributes(Path.of("test.zip")));
    }
    @Test
    void getAssociatedXmlFile() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);
        var filename = "data/inbox/doi-10-5072-dar-kxteqtv1.0.zip";

        var result = service.getAssociatedXmlFile(Path.of(filename));

        assertTrue(result.isPresent());
        assertEquals("data/inbox/doi-10-5072-dar-kxteqt-datacite.v1.0.xml", result.get().toString());
    }

    @Test
    void getAssociatedXmlFileForInvalidFilename() {
        var fileService = Mockito.mock(FileService.class);
        var service = new TransferItemMetadataReaderImpl(new ObjectMapper(), fileService);
        // it has doa instead of doi
        var filename = "data/inbox/doa-10-5072-dar-kxteqtv1.0.zip";

        var result = service.getAssociatedXmlFile(Path.of(filename));

        assertTrue(result.isEmpty());
    }
}
