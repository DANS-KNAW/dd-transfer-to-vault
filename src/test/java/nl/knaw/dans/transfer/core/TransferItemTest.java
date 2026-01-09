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
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

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
    public void moveToDir_without_error_should_move_and_not_write_error_log() throws Exception {
        // Given
        var sourceDir = testDir.resolve("transfer-item-noerr");
        var targetDir = testDir.resolve("transfer-item-noerr-target");
        Files.createDirectories(sourceDir);
        Files.createDirectories(targetDir);

        var dve = sourceDir.resolve("dataset.zip");
        Files.writeString(dve, "dummy");

        var item = new TransferItem(dve);

        // Capture filesystem creation time before the move
        var millis = Files.readAttributes(dve, java.nio.file.attribute.BasicFileAttributes.class)
            .creationTime().toMillis();

        // When
        item.moveToDir(targetDir);

        // Then: creationTime should be added to the name when missing
        var expected = targetDir.resolve("dataset_" + millis + ".zip");
        assertThat(Files.exists(expected)).isTrue();
        assertThat(Files.exists(dve)).isFalse();

        var errorLog = expected.resolveSibling(expected.getFileName() + "-error.log");
        assertThat(Files.exists(errorLog)).isFalse();
    }

    @Test
    public void moveToDir_without_error_should_avoid_collisions_by_index() throws Exception {
        // Given
        var sourceDir = testDir.resolve("transfer-item-noerr-collision");
        var targetDir = testDir.resolve("transfer-item-noerr-collision-target");
        Files.createDirectories(sourceDir);
        Files.createDirectories(targetDir);

        var dve = sourceDir.resolve("dataset.zip");
        Files.writeString(dve, "dummy");

        // Determine the expected timestamped name that moveToDir will produce
        var millis = Files.readAttributes(dve, java.nio.file.attribute.BasicFileAttributes.class)
            .creationTime().toMillis();
        var timestampedName = Path.of("dataset_" + millis + ".zip");

        // existing file in target to force index suffix (must match the timestamped name)
        var existing = targetDir.resolve(timestampedName);
        Files.writeString(existing, "existing");

        var item = new TransferItem(dve);

        // When
        item.moveToDir(targetDir);

        // Then: should pick an index because the timestamped target name already exists
        var expected = targetDir.resolve("dataset_" + millis + "-1.zip");
        assertThat(Files.exists(expected)).isTrue();
        assertThat(Files.exists(existing)).isTrue();

        var errorLog = expected.resolveSibling(expected.getFileName() + "-error.log");
        assertThat(Files.exists(errorLog)).isFalse();
    }

    @Test
    public void moveToDir_without_error_should_preserve_creationTime_when_matches_filesystem() throws Exception {
        // Given
        var sourceDir = testDir.resolve("transfer-item-noerr-ctime");
        var targetDir = testDir.resolve("transfer-item-noerr-ctime-target");
        Files.createDirectories(sourceDir);
        Files.createDirectories(targetDir);

        // Start with a plain file
        var initial = sourceDir.resolve("dataset.zip");
        Files.writeString(initial, "dummy");

        // Read its creation time and rename to include that exact millis
        var millis = Files.readAttributes(initial, java.nio.file.attribute.BasicFileAttributes.class)
            .creationTime().toMillis();
        var withCtimePath = new DveFileName(initial)
            .withCreationTime(OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC))
            .getPath();
        Files.move(initial, withCtimePath);

        var item = new TransferItem(withCtimePath);

        // When
        item.moveToDir(targetDir);

        // Then: name should stay the same (same creationTime), just moved
        var expected = targetDir.resolve(withCtimePath.getFileName());
        assertThat(Files.exists(expected)).isTrue();
        assertThat(Files.exists(withCtimePath)).isFalse();

        var errorLog = expected.resolveSibling(expected.getFileName() + "-error.log");
        assertThat(Files.exists(errorLog)).isFalse();
    }

    @Test
    public void moveToDir_without_error_should_add_creationTime_when_matches_filesystem() throws Exception {
        // Given
        var sourceDir = testDir.resolve("transfer-item-noerr-addctime");
        var targetDir = testDir.resolve("transfer-item-noerr-addctime-target");
        Files.createDirectories(sourceDir);
        Files.createDirectories(targetDir);

        // Start with a plain file without creationTime in the name
        var initial = sourceDir.resolve("dataset.zip");
        Files.writeString(initial, "dummy");

        // Read its filesystem creation time (millis)
        var millis = Files.readAttributes(initial, java.nio.file.attribute.BasicFileAttributes.class)
            .creationTime().toMillis();

        var item = new TransferItem(initial);

        // When
        item.moveToDir(targetDir);

        // Then: name should now include the creationTime from filesystem
        var expected = targetDir.resolve("dataset_" + millis + ".zip");
        assertThat(Files.exists(expected)).isTrue();
        assertThat(Files.exists(initial)).isFalse();

        var errorLog = expected.resolveSibling(expected.getFileName() + "-error.log");
        assertThat(Files.exists(errorLog)).isFalse();
    }

    @Test
    public void setOcflObjectVersion_should_rename_file() throws IOException {
        // Given
        var sourceDir = testDir.resolve("transfer-item-test");
        Files.createDirectories(sourceDir);
        var dve = sourceDir.resolve("dataset.zip");
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
        var sourceDir = testDir.resolve("transfer-item-test");
        var targetDir = testDir.resolve("transfer-item-target");
        Files.createDirectories(sourceDir);
        Files.createDirectories(targetDir);

        var dve = sourceDir.resolve("dataset_1710000000000_v1.zip");
        Files.writeString(dve, "dummy");

        // Place a file with the same name in target to force index suffix
        var existing = targetDir.resolve(dve.getFileName());
        Files.writeString(existing, "existing");

        var item = new TransferItem(dve);

        // When
        var error = new IllegalStateException("boom");
        item.moveToDir(targetDir, error);

        // Then
        var expectedMoved = targetDir.resolve("dataset_1710000000000_v1-1.zip");
        assertThat(Files.exists(expectedMoved)).isTrue();
        assertThat(Files.exists(existing)).isTrue(); // original existing untouched

        var errorLog = expectedMoved.resolveSibling(expectedMoved.getFileName() + "-error.log");
        assertThat(Files.exists(errorLog)).isTrue();
        var logContent = Files.readString(errorLog);
        assertThat(logContent).contains("boom");
    }

    @Test
    public void getContact_and_getNbn_and_getDataversePidVersion_and_getHasOrganizationalIdentifierVersion_should_read_from_zip_dve() throws Exception {
        // Given: create a DVE zip with a top-level directory, bagit files, and metadata JSON
        var sourceDir = testDir.resolve("transfer-item-zip");
        Files.createDirectories(sourceDir);
        var dve = sourceDir.resolve("dataset_v1.zip");
        createDveZip(dve,
            "Contact Name A;Contact Name B",
            "a@example.org;b@example.org",
            "urn:nbn:nl:ui:13-abcdef",
            "doi:10.5072/FK2/ABCDEF:1.0",
            "true");

        var item = new TransferItem(dve);

        // When / Then
        assertThat(item.getContactName()).isEqualTo("Contact Name A;Contact Name B");
        assertThat(item.getContactEmail()).isEqualTo("a@example.org;b@example.org");
        assertThat(item.getNbn()).isEqualTo("urn:nbn:nl:ui:13-abcdef");
        assertThat(item.getDataversePidVersion()).contains("doi:10.5072/FK2/ABCDEF:1.0");
        assertThat(item.getHasOrganizationalIdentifierVersion()).contains("true");
    }

    @Test
    public void getNbn_should_fail_when_metadata_missing() throws Exception {
        // Given
        var sourceDir = testDir.resolve("transfer-item-zip");
        Files.createDirectories(sourceDir);
        var dve = sourceDir.resolve("dataset.zip");
        createDveZip(dve, "John Doe", "john@example.org", null, null, null); // no metadata JSON

        var item = new TransferItem(dve);

        // Then
        assertThatThrownBy(item::getNbn)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No metadata file");
    }

    @Test
    public void getDataversePidVersion_should_return_empty_when_metadata_missing() throws Exception {
        // Given
        var sourceDir = testDir.resolve("transfer-item-zip-pid");
        Files.createDirectories(sourceDir);
        var dve = sourceDir.resolve("dataset.zip");
        createDveZip(dve, "John Doe", "john@example.org", "urn:nbn:nl:ui:13-abcdef", null, null); // no PID version in metadata

        var item = new TransferItem(dve);

        // Then
        assertThat(item.getDataversePidVersion()).isEmpty();
    }

    /**
     * Helper method to create a DVE zip file for testing purposes. Creates a minimal BagIt structure with bag-info.txt containing contact information and optionally an oai-ore.jsonld metadata file
     * with NBN information.
     *
     * @param zipPath                                  the path where the zip file should be created
     * @param contactName                              the contact name to include in bag-info.txt, or null to omit
     * @param contactEmail                             the contact email to include in bag-info.txt, or null to omit
     * @param nbn                                      the NBN identifier to include in metadata/oai-ore.jsonld, or null to omit metadata file
     * @param pidVersion                               the Dataverse PID version to include in metadata/oai-ore.jsonld, or null to omit from metadata file
     * @param hasOrganizationalIdentifierVersion       the Has-Organizational-Identifier-Version to include in bag-info.txt, or null to omit
     * @throws IOException if an I/O error occurs during zip creation
     */
    private static void createDveZip(Path zipPath, String contactName, String contactEmail, String nbn, String pidVersion, String hasOrganizationalIdentifierVersion) throws IOException {
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
            if (hasOrganizationalIdentifierVersion != null) {
                bagInfo.append("Has-Organizational-Identifier-Version: ").append(hasOrganizationalIdentifierVersion).append("\n");
            }
            Files.writeString(top.resolve("bag-info.txt"), bagInfo.toString(), StandardCharsets.UTF_8);

            if (nbn != null || pidVersion != null) {
                var metadataDir = Files.createDirectories(top.resolve("metadata"));
                var nbnJson = nbn != null ? "\"dansDataVaultMetadata:dansNbn\": \"%s\"".formatted(nbn) : "";
                var pidVersionJson = pidVersion != null ? "\"dansDataVaultMetadata:dansDataversePidVersion\": \"%s\"".formatted(pidVersion) : "";
                var comma = (nbn != null && pidVersion != null) ? "," : "";

                var json = """
                    {
                      "ore:describes": {
                        %s%s%s
                      }
                    }
                    """.formatted(nbnJson, comma, pidVersionJson);
                Files.writeString(metadataDir.resolve("oai-ore.jsonld"), json, StandardCharsets.UTF_8);
            }
        }
    }
}
