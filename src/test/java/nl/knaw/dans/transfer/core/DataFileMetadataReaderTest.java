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

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFileMetadataReaderTest {

    @Test
    void readDataFileAttributes_should_include_fetch_sizes() throws IOException {
        // Arrange
        var fileService = mock(FileService.class);
        var reader = new DataFileMetadataReader(fileService);
        var dveZip = Path.of("dve.zip");
        var zipFile = mock(ZipFile.class);
        
        when(fileService.openZipFile(dveZip)).thenReturn(zipFile);
        
        String pidMapping = "doi:10.5072/FK2/FILE1 data/file1.txt\n" +
                           "doi:10.5072/FK2/FILE2 data/file2.txt\n" +
                           "doi:10.5072/FK2/FILE3 data/file3.txt\n";
        String sha1Manifest = "sha1-file1 data/file1.txt\n" +
                             "sha1-file2 data/file2.txt\n" +
                             "sha1-file3 data/file3.txt\n";
        String fetchTxt = "http://example.com/file1 100 data/file1.txt\n" +
                          "http://example.com/file3 - data/file3.txt\n";

        when(fileService.getEntryUnderBaseFolder(zipFile, Path.of("metadata/pid-mapping.txt")))
            .thenReturn(new ByteArrayInputStream(pidMapping.getBytes(StandardCharsets.UTF_8)));
        when(fileService.getEntryUnderBaseFolder(zipFile, Path.of("manifest-sha1.txt")))
            .thenReturn(new ByteArrayInputStream(sha1Manifest.getBytes(StandardCharsets.UTF_8)));
        
        // Mock fetch.txt exists
        when(fileService.getEntryUnderBaseFolder(zipFile, Path.of("fetch.txt")))
            .thenReturn(new ByteArrayInputStream(fetchTxt.getBytes(StandardCharsets.UTF_8)));

        // Mock zip entries for readPathToSizeMapping
        var entry1 = mock(ZipEntry.class);
        when(entry1.getName()).thenReturn("base/data/file1.txt");
        when(entry1.getSize()).thenReturn(-1L);
        when(entry1.isDirectory()).thenReturn(false);

        var entry2 = mock(ZipEntry.class);
        when(entry2.getName()).thenReturn("base/data/file2.txt");
        when(entry2.getSize()).thenReturn(200L);
        when(entry2.isDirectory()).thenReturn(false);

        var entry3 = mock(ZipEntry.class);
        when(entry3.getName()).thenReturn("base/data/file3.txt");
        when(entry3.getSize()).thenReturn(-1L);
        when(entry3.isDirectory()).thenReturn(false);
        
        when(zipFile.stream()).thenAnswer(i -> List.of(entry1, entry2, entry3).stream());

        // Act
        List<DataFileMetadata> result = reader.readDataFileAttributes(dveZip);

        // Assert
        assertThat(result).hasSize(3);
        
        var file1 = result.stream().filter(f -> f.getFilepath().toString().equals("data/file1.txt")).findFirst().get();
        assertThat(file1.getSize()).isEqualTo(100L); // From fetch.txt
        
        var file2 = result.stream().filter(f -> f.getFilepath().toString().equals("data/file2.txt")).findFirst().get();
        assertThat(file2.getSize()).isEqualTo(200L); // From ZipEntry

        var file3 = result.stream().filter(f -> f.getFilepath().toString().equals("data/file3.txt")).findFirst().get();
        assertThat(file3.getSize()).isEqualTo(-1L); // From fetch.txt with '-'
    }

    @Test
    void readDataFileAttributes_should_work_without_fetch_txt() throws IOException {
        // Arrange
        var fileService = mock(FileService.class);
        var reader = new DataFileMetadataReader(fileService);
        var dveZip = Path.of("dve.zip");
        var zipFile = mock(ZipFile.class);
        
        when(fileService.openZipFile(dveZip)).thenReturn(zipFile);
        
        String pidMapping = "doi:10.5072/FK2/FILE1 data/file1.txt\n";
        String sha1Manifest = "sha1-file1 data/file1.txt\n";

        when(fileService.getEntryUnderBaseFolder(zipFile, Path.of("metadata/pid-mapping.txt")))
            .thenReturn(new ByteArrayInputStream(pidMapping.getBytes(StandardCharsets.UTF_8)));
        when(fileService.getEntryUnderBaseFolder(zipFile, Path.of("manifest-sha1.txt")))
            .thenReturn(new ByteArrayInputStream(sha1Manifest.getBytes(StandardCharsets.UTF_8)));
        
        // Mock fetch.txt does NOT exist
        when(fileService.getEntryUnderBaseFolder(zipFile, Path.of("fetch.txt")))
            .thenThrow(new IllegalArgumentException("No entry found for path: fetch.txt"));

        var entry1 = new ZipEntry("base/data/file1.txt");
        entry1.setSize(100);
        when(zipFile.stream()).thenAnswer(i -> List.of(entry1).stream());

        // Act
        List<DataFileMetadata> result = reader.readDataFileAttributes(dveZip);

        // Assert
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getSize()).isEqualTo(100L);
    }
}
