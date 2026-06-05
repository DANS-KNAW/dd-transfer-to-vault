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

import nl.knaw.dans.transfer.TestDirFixture;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataFileMetadataReaderTest extends TestDirFixture  {

    @Test
    void readDataFileAttributes_should_include_fetch_sizes() throws IOException {
        // Arrange
        var fileService = new FileServiceImpl(); // Use real implementation if possible, or mock only what's needed
        var reader = new DataFileMetadataReader(fileService);
        var dveZip = testDir.resolve("dve.zip");

        try (var zos = new ZipOutputStream(Files.newOutputStream(dveZip))) {
            zos.putNextEntry(new ZipEntry("base/metadata/pid-mapping.txt"));
            zos.write("doi:10.5072/FK2/FILE1 data/file1.txt\ndoi:10.5072/FK2/FILE2 data/file2.txt\ndoi:10.5072/FK2/FILE3 data/file3.txt\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            zos.putNextEntry(new ZipEntry("base/bagit.txt"));
            zos.write("BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            zos.putNextEntry(new ZipEntry("base/manifest-sha1.txt"));
            zos.write("sha1-file1 data/file1.txt\nsha1-file2 data/file2.txt\nsha1-file3 data/file3.txt\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            zos.putNextEntry(new ZipEntry("base/fetch.txt"));
            zos.write("http://example.com/file1 100 data/file1.txt\nhttp://example.com/file3 - data/file3.txt\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            zos.putNextEntry(new ZipEntry("base/data/file2.txt"));
            zos.write("content of file 2".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        // Act
        List<DataFileMetadata> result = reader.readDataFileAttributes(dveZip);

        // Assert
        assertThat(result).hasSize(3);

        var file1 = result.stream().filter(f -> f.getFilepath().toString().equals("data/file1.txt")).findFirst().get();
        assertThat(file1.getSize()).isEqualTo(100L);

        var file2 = result.stream().filter(f -> f.getFilepath().toString().equals("data/file2.txt")).findFirst().get();
        assertThat(file2.getSize()).isEqualTo(17L); // "content of file 2".length()

        var file3 = result.stream().filter(f -> f.getFilepath().toString().equals("data/file3.txt")).findFirst().get();
        assertThat(file3.getSize()).isEqualTo(-1L);
    }

    @Test
    void readDataFileAttributes_should_work_without_fetch_txt() throws IOException {
        // Arrange
        var fileService = new FileServiceImpl();
        var reader = new DataFileMetadataReader(fileService);
        var dveZip = testDir.resolve("dve_no_fetch.zip");

        try (var zos = new ZipOutputStream(Files.newOutputStream(dveZip))) {
            zos.putNextEntry(new ZipEntry("base/metadata/pid-mapping.txt"));
            zos.write("doi:10.5072/FK2/FILE1 data/file1.txt\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            zos.putNextEntry(new ZipEntry("base/bagit.txt"));
            zos.write("BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            zos.putNextEntry(new ZipEntry("base/manifest-sha1.txt"));
            zos.write("sha1-file1 data/file1.txt\n".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();

            zos.putNextEntry(new ZipEntry("base/data/file1.txt"));
            zos.write("content of file 1".getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }

        // Act
        List<DataFileMetadata> result = reader.readDataFileAttributes(dveZip);

        // Assert
        assertThat(result).hasSize(1);
        assertThat(result.get(0).getSize()).isEqualTo(17L);
        assertThat(result.get(0).getFilepath().toString()).isEqualTo("data/file1.txt");
    }
}
