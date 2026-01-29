/*
 * Copyright (C) 2025 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
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
package nl.knaw.dans.transfer.core;

import nl.knaw.dans.transfer.core.oaiore.OaiOreMetadataReader;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DveMetadataReaderTest {

    @Test
    void readDveMetadata_success() throws Exception {
        // Arrange
        var fileService = mock(FileService.class);
        var oaiReader = mock(OaiOreMetadataReader.class);
        var dataFileReader = new DataFileMetadataReader(fileService);
        var dataFileReaderSpy = org.mockito.Mockito.spy(dataFileReader);
        var reader = new DveMetadataReader(fileService, oaiReader, dataFileReaderSpy);

        var path = Path.of("dataset_1735689600000_v2-1.zip"); // 2025-01-01T00:00:00Z
        var zip = mock(ZipFile.class);
        when(fileService.openZipFile(path)).thenReturn(zip);

        var oaiOreJson = "{ }";
        when(fileService.getEntryUnderBaseFolder(zip, Path.of("metadata/oai-ore.jsonld")))
            .thenReturn(new ByteArrayInputStream(oaiOreJson.getBytes(StandardCharsets.UTF_8)));

        var bagInfo = String.join("\n",
            "Contact-Email: user@example.org",
            "Contact-Name: Jane Doe",
            "Some-Other: value"
        );
        when(fileService.getEntryUnderBaseFolder(zip, Path.of("bag-info.txt")))
            .thenReturn(new ByteArrayInputStream(bagInfo.getBytes(StandardCharsets.UTF_8)));

        var baseMetadata = DveMetadata.builder()
            .dataversePid("doi:10.5072/FK2/ABC")
            .dataversePidVersion("2")
            .title("My Title")
            .bagId("urn:uuid:1234")
            .nbn("urn:nbn:nl:ui:13-xyz")
            .metadata(oaiOreJson)
            .otherId("other")
            .otherIdVersion("1")
            .swordToken("token")
            .dataSupplier("supplier")
            .datastation("station")
            .exporter("exporter")
            .exporterVersion("1.0")
            .build();
        when(oaiReader.readMetadata(anyString())).thenReturn(baseMetadata);

        var dfList = List.of(new DataFileMetadata(
            Path.of("data/file1.txt"),
            URI.create("https://doi.org/10.5072/FK2/FILE1"),
            "abc123",
            123L
        ));
        org.mockito.Mockito.doReturn(dfList).when(dataFileReaderSpy).readDataFileAttributes(path);

        // Act
        var result = reader.readDveMetadata(path);

        // Assert
        assertThat(result.getContactEmail()).isEqualTo("user@example.org");
        assertThat(result.getContactName()).isEqualTo("Jane Doe");
        assertThat(result.getDataFileAttributes()).containsExactlyElementsOf(dfList);
        assertThat(result.getCreationTime()).isEqualTo(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1735689600000L), ZoneOffset.UTC));
        // fields originating from OAI-ORE should be preserved
        assertThat(result.getDataversePid()).isEqualTo("doi:10.5072/FK2/ABC");
        assertThat(result.getTitle()).isEqualTo("My Title");
        assertThat(result.getBagId()).isEqualTo("urn:uuid:1234");
    }

    @Test
    void readDveMetadata_wrapsIOException() throws Exception {
        // Arrange
        var fileService = mock(FileService.class);
        var oaiReader = mock(OaiOreMetadataReader.class);
        var dataFileReader = mock(DataFileMetadataReader.class);
        var reader = new DveMetadataReader(fileService, oaiReader, dataFileReader);

        var path = Path.of("dataset_1735689600000.zip");
        when(fileService.openZipFile(path)).thenThrow(new IOException("boom"));

        // Act / Assert
        assertThatThrownBy(() -> reader.readDveMetadata(path))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("unable to read metadata from file")
            .hasCauseInstanceOf(IOException.class);
    }

    @Test
    void readDveMetadata_missingContactEmail_throws() throws Exception {
        // Arrange
        var fileService = mock(FileService.class);
        var oaiReader = mock(OaiOreMetadataReader.class);
        var dataFileReader = mock(DataFileMetadataReader.class);
        var reader = new DveMetadataReader(fileService, oaiReader, dataFileReader);

        var path = Path.of("dataset_1735689600000.zip");
        var zip = mock(ZipFile.class);
        when(fileService.openZipFile(path)).thenReturn(zip);

        when(fileService.getEntryUnderBaseFolder(zip, Path.of("metadata/oai-ore.jsonld")))
            .thenReturn(new ByteArrayInputStream("{}".getBytes(StandardCharsets.UTF_8)));
        when(oaiReader.readMetadata(anyString())).thenReturn(DveMetadata.builder().build());

        // No Contact-Email in bag-info
        var bagInfo = String.join("\n",
            "Contact-Name: Jane Doe"
        );
        when(fileService.getEntryUnderBaseFolder(zip, Path.of("bag-info.txt")))
            .thenReturn(new ByteArrayInputStream(bagInfo.getBytes(StandardCharsets.UTF_8)));

        // Act / Assert
        assertThatThrownBy(() -> reader.readDveMetadata(path))
            .isInstanceOfAny(IllegalArgumentException.class, NullPointerException.class);
    }

    @Test
    void readDveMetadata_missingContactName_usesEmailAsFallback() throws Exception {
        // Arrange
        var fileService = mock(FileService.class);
        var oaiReader = mock(nl.knaw.dans.transfer.core.oaiore.OaiOreMetadataReader.class);
        var dataFileReader = mock(DataFileMetadataReader.class);
        var reader = new DveMetadataReader(fileService, oaiReader, dataFileReader);

        var path = java.nio.file.Path.of("dataset_1735689600000.zip");
        var zip = mock(java.util.zip.ZipFile.class);
        when(fileService.openZipFile(path)).thenReturn(zip);

        when(fileService.getEntryUnderBaseFolder(zip, java.nio.file.Path.of("metadata/oai-ore.jsonld")))
            .thenReturn(new java.io.ByteArrayInputStream("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        when(oaiReader.readMetadata(anyString())).thenReturn(DveMetadata.builder().build());

        // Only Contact-Email present
        var bagInfo = String.join("\n",
            "Contact-Email: user@example.org"
        );
        when(fileService.getEntryUnderBaseFolder(zip, java.nio.file.Path.of("bag-info.txt")))
            .thenReturn(new java.io.ByteArrayInputStream(bagInfo.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        when(dataFileReader.readDataFileAttributes(path)).thenReturn(java.util.List.of());

        // Act
        var result = reader.readDveMetadata(path);

        // Assert
        assertThat(result.getContactEmail()).isEqualTo("user@example.org");
        assertThat(result.getContactName()).isEqualTo("user@example.org");
    }
}
