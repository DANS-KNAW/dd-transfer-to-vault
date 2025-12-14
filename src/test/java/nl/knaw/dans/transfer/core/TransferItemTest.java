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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TransferItemTest extends TestDirFixture {

    @Test
    public void getOcflObjectVersion_should_parse_from_name_or_return_zero() {
        // Given
        var withVersion = Path.of("dataset_1710000000000_v3.zip");
        var withoutVersion = Path.of("dataset_1710000000000.zip");

        // When
        var itemWithVersion = new TransferItem(withVersion);
        var itemWithoutVersion = new TransferItem(withoutVersion);

        // Then
        assertThat(itemWithVersion.getOcflObjectVersion()).isEqualTo(3);
        assertThat(itemWithoutVersion.getOcflObjectVersion()).isEqualTo(0);
    }

    @Test
    public void setOcflObjectVersion_should_rename_file() throws IOException {
        // Given
        var tempDir = testDir.resolve("transfer-item-test");
        Files.createDirectories(tempDir);
        var dve = tempDir.resolve("dataset.zip");
        Files.writeString(dve, "dummy");
        var item = new TransferItem(dve);

        var expectedPath = new DveFileName(dve).withOcflObjectVersion(2).getPath();

        // When
        item.setOcflObjectVersion(2);

        // Then
        assertThat(Files.exists(expectedPath)).isTrue();
        assertThat(Files.exists(dve)).isFalse();
    }

    @Test
    public void moveToDir_with_error_should_keep_name_and_write_error_log_and_avoid_collisions() throws Exception {
        // Given
        var tempDir = testDir.resolve("transfer-item-test");
        var targetDir = testDir.resolve("transfer-item-target");
        Files.createDirectories(tempDir);
        Files.createDirectories(targetDir);

        var dve = tempDir.resolve("dataset_1710000000000_v1.zip");
        Files.writeString(dve, "dummy");

        // Place a file with the same name in target to force index suffix
        var existing = targetDir.resolve(dve.getFileName());
        Files.writeString(existing, "existing");

        var item = new TransferItem(dve);

        // When
        var error = new IllegalStateException("boom");
        item.moveToDir(targetDir, error);

        // Then
        var expectedMoved = targetDir.resolve("dataset_1710000000000_v1-0.zip");
        assertThat(Files.exists(expectedMoved)).isTrue();
        assertThat(Files.exists(existing)).isTrue(); // original existing untouched

        var errorLog = expectedMoved.resolveSibling(expectedMoved.getFileName() + "-error.log");
        assertThat(Files.exists(errorLog)).isTrue();
        var logContent = Files.readString(errorLog);
        assertThat(logContent).contains("boom");
    }






    @Test
    public void getContact_and_getNbn_should_read_from_zip_dve() throws Exception {
        // Given: create a DVE zip with a top-level directory, bagit files, and metadata JSON
        var tempDir = testDir.resolve("transfer-item-zip");
        Files.createDirectories(tempDir);
        var dve = tempDir.resolve("dataset_v1.zip");
        createDveZip(dve,
            "Contact Name A;Contact Name B",
            "a@example.org;b@example.org",
            "urn:nbn:nl:ui:13-abcdef");

        var item = new TransferItem(dve);

        // When / Then
        assertThat(item.getContactName()).isEqualTo("Contact Name A;Contact Name B");
        assertThat(item.getContactEmail()).isEqualTo("a@example.org;b@example.org");
        assertThat(item.getNbn()).isEqualTo("urn:nbn:nl:ui:13-abcdef");
    }

    @Test
    public void getNbn_should_fail_when_metadata_missing() throws Exception {
        // Given
        var tempDir = testDir.resolve("transfer-item-zip");
        Files.createDirectories(tempDir);
        var dve = tempDir.resolve("dataset.zip");
        createDveZip(dve, "John Doe", "john@example.org", null); // no metadata JSON

        var item = new TransferItem(dve);

        // Then
        assertThatThrownBy(item::getNbn)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No metadata file");
    }

    private static void createDveZip(Path zipPath, String contactName, String contactEmail, String nbn) throws IOException {
        var env = new HashMap<String, String>();
        env.put("create", "true");
        try (var zipfs = FileSystems.newFileSystem(URI.create("jar:" + zipPath.toUri()), env)) {
            var root = zipfs.getPath("/");
            var top = root.resolve("bag");
            Files.createDirectories(top);

            // Minimal bagit files + bag-info.txt
            Files.writeString(top.resolve("bagit.txt"), "BagIt-Version: 1.0\nTag-File-Character-Encoding: UTF-8\n");

            var bagInfo = new StringBuilder();
            if (contactName != null) {
                bagInfo.append("Contact-Name: ").append(contactName).append("\n");
            }
            if (contactEmail != null) {
                bagInfo.append("Contact-Email: ").append(contactEmail).append("\n");
            }
            Files.writeString(top.resolve("bag-info.txt"), bagInfo.toString(), StandardCharsets.UTF_8);

            if (nbn != null) {
                var metadataDir = Files.createDirectories(top.resolve("metadata"));
                var json = """
                    {
                      "ore:describes": {
                        "dansDataVaultMetadata:dansNbn": "%s"
                      }
                    }
                    """.formatted(nbn);
                Files.writeString(metadataDir.resolve("oai-ore.jsonld"), json, StandardCharsets.UTF_8);
            }
        }
    }
}
