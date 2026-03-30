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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.util.DataSize;
import nl.knaw.dans.lib.util.ZipUtil;
import nl.knaw.dans.lib.util.healthcheck.DependenciesReadyCheck;
import nl.knaw.dans.transfer.TestDirFixture;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.config.CustomPropertyConfig;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SendToVaultTaskTest extends TestDirFixture {
    @Test
    void addToObjectImportDirectory_should_not_delete_objectImportDirectory_if_other_versions_exist() throws Exception {
        // Given
        var dve = testDir.resolve("fail.zip");
        var currentBatchWorkDir = testDir.resolve("batch");
        var dataVaultBatchRoot = testDir.resolve("vault");
        var outboxProcessed = testDir.resolve("processed");
        var outboxFailed = testDir.resolve("failed");
        var dataVaultClient = Mockito.mock(DataVaultClient.class);
        var fileService = new FileServiceImpl();
        var readyCheck = Mockito.mock(DependenciesReadyCheck.class);
        var defaultMessage = "msg";
        var customProperties = new ArrayList<CustomPropertyConfig>();

        var task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(0), outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService, readyCheck, 100
        );

        // Prepare object import directory with two versions
        int version = 1;
        var objectImportDirectory = testDir.resolve("object-import");
        Files.createDirectories(objectImportDirectory);
        var versionDirectory = objectImportDirectory.resolve("v" + version);
        Files.createDirectories(versionDirectory);
        var otherVersionDirectory = objectImportDirectory.resolve("v2");
        Files.createDirectories(otherVersionDirectory);

        // Set up a TransferItem with required methods
        var transferItem = Mockito.mock(TransferItem.class);
        Mockito.when(transferItem.getOcflObjectVersion()).thenReturn(version);
        Mockito.when(transferItem.getNbn()).thenReturn("nbn123");
        Mockito.when(transferItem.getContactName()).thenReturn("user");
        Mockito.when(transferItem.getContactEmail()).thenReturn("email");
        // Set currentTransferItem via reflection (private field)
        var field = SendToVaultTask.class.getDeclaredField("currentTransferItem");
        field.setAccessible(true);
        field.set(task, transferItem);

        // When / Then
        try (MockedStatic<ZipUtil> zipUtilMock = Mockito.mockStatic(ZipUtil.class)) {
            zipUtilMock.when(() -> ZipUtil.extractZipFile(Mockito.any(), Mockito.any()))
                .thenThrow(new RuntimeException("Extraction failed"));
            var method = task.getClass().getDeclaredMethod("addToObjectImportDirectory", Path.class, int.class, Path.class);
            method.setAccessible(true);
            Throwable thrown = null;
            try {
                method.invoke(task, dve, version, objectImportDirectory);
            } catch (InvocationTargetException e) {
                thrown = e.getCause();
            }
            assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Extraction failed");
            // Only the failed version directory should be deleted, not the parent or other version
            assertThat(Files.exists(objectImportDirectory)).isTrue();
            assertThat(Files.exists(versionDirectory)).isFalse();
            assertThat(Files.exists(otherVersionDirectory)).isTrue();
        }
    }

    @Test
    public void createVersionInfoProperties_should_handle_special_characters_and_whitespace() throws Exception {
        // Given
        Path dve = testDir.resolve("test.zip");
        Path currentBatchWorkDir = testDir.resolve("batch");
        Path dataVaultBatchRoot = testDir.resolve("vault");
        Path outboxProcessed = testDir.resolve("processed");
        Path outboxFailed = testDir.resolve("failed");
        DataVaultClient dataVaultClient = Mockito.mock(DataVaultClient.class);
        FileService fileService = new FileServiceImpl();
        DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);
        String defaultMessage = "Default message with\nnewline";
        List<CustomPropertyConfig> customProperties = new ArrayList<>();

        var task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(0), outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService, readyCheck, 100
        );

        Path versionDirectory = testDir.resolve("v1");
        Files.createDirectories(versionDirectory);

        String user = "  John Doe \n  ";
        String email = "\tjohn@example.com\r\n";
        String message = "Message with = and : and \n newline";

        // When
        task.createVersionInfoJson(versionDirectory, user, email, message);

        // Then
        Path jsonFile = testDir.resolve("v1.json");
        assertThat(jsonFile).exists();

        var mapper = new ObjectMapper();
        Map<?,?> root;
        try (var is = Files.newInputStream(jsonFile)) {
            root = mapper.readValue(is, Map.class);
        }
        Map<?,?> versionInfo = (Map<?,?>) root.get("version-info");
        Map<?,?> userObj = (Map<?,?>) versionInfo.get("user");
        assertThat(userObj.get("name")).isEqualTo("John Doe");
        assertThat(userObj.get("email")).isEqualTo("john@example.com");
        assertThat(versionInfo.get("message")).isEqualTo(message);
    }

    @Test
    void addToObjectImportDirectory_should_cleanup_on_extraction_failure() throws Exception {
        // Given
        Path dve = testDir.resolve("fail.zip");
        Path currentBatchWorkDir = testDir.resolve("batch");
        Path dataVaultBatchRoot = testDir.resolve("vault");
        Path outboxProcessed = testDir.resolve("processed");
        Path outboxFailed = testDir.resolve("failed");
        DataVaultClient dataVaultClient = Mockito.mock(DataVaultClient.class);
        FileService fileService = new FileServiceImpl();
        DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);
        String defaultMessage = "msg";
        List<CustomPropertyConfig> customProperties = new ArrayList<>();

        var task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(0), outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService, readyCheck, 100
        );

        // Prepare version directory
        int version = 1;
        Path objectImportDirectory = testDir.resolve("object-import");
        Files.createDirectories(objectImportDirectory);
        Path versionDirectory = objectImportDirectory.resolve("v" + version);
        Files.createDirectories(versionDirectory);

        // Set up a TransferItem with required methods
        var transferItem = Mockito.mock(TransferItem.class);
        Mockito.when(transferItem.getOcflObjectVersion()).thenReturn(version);
        Mockito.when(transferItem.getNbn()).thenReturn("nbn123");
        Mockito.when(transferItem.getContactName()).thenReturn("user");
        Mockito.when(transferItem.getContactEmail()).thenReturn("email");
        // Set currentTransferItem via reflection (private field)
        var field = SendToVaultTask.class.getDeclaredField("currentTransferItem");
        field.setAccessible(true);
        field.set(task, transferItem);

        // When / Then
        try (MockedStatic<ZipUtil> zipUtilMock = Mockito.mockStatic(ZipUtil.class)) {
            zipUtilMock.when(() -> ZipUtil.extractZipFile(Mockito.any(), Mockito.any()))
                .thenThrow(new RuntimeException("Extraction failed"));
            var method = task.getClass().getDeclaredMethod("addToObjectImportDirectory", Path.class, int.class, Path.class);
            method.setAccessible(true);
            Throwable thrown = null;
            try {
                method.invoke(task, dve, version, objectImportDirectory);
            } catch (InvocationTargetException e) {
                thrown = e.getCause();
            }
            assertThat(thrown)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Extraction failed");
            // Directory should be deleted (parent or version dir)
            assertThat(Files.exists(objectImportDirectory)).isFalse();
        }
    }
}
