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
import nl.knaw.dans.lobstore.client.api.TransferRequestDto;
import nl.knaw.dans.transfer.TestDirFixture;
import nl.knaw.dans.transfer.client.DataVaultClient;
import nl.knaw.dans.transfer.client.LobStoreClient;
import nl.knaw.dans.transfer.config.CustomPropertyConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
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
        var dveMetadataReader = Mockito.mock(DveMetadataReader.class);
        var lobStoreClient = Mockito.mock(LobStoreClient.class);
        var readyCheck = Mockito.mock(DependenciesReadyCheck.class);
        var defaultMessage = "msg";
        var customProperties = new ArrayList<CustomPropertyConfig>();

        var task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(0), outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService, dveMetadataReader, lobStoreClient, "ds1", readyCheck, 100
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
        DveMetadataReader dveMetadataReader = Mockito.mock(DveMetadataReader.class);
        LobStoreClient lobStoreClient = Mockito.mock(LobStoreClient.class);
        DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);
        String defaultMessage = "Default message with\nnewline";
        List<CustomPropertyConfig> customProperties = new ArrayList<>();

        var task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(0), outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService, dveMetadataReader, lobStoreClient, "ds1", readyCheck, 100
        );

        // Set up a TransferItem
        var transferItem = Mockito.mock(TransferItem.class);
        Mockito.when(transferItem.getFetchSha1s()).thenReturn(List.of());
        var field = SendToVaultTask.class.getDeclaredField("currentTransferItem");
        field.setAccessible(true);
        field.set(task, transferItem);

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
        DveMetadataReader dveMetadataReader = Mockito.mock(DveMetadataReader.class);
        LobStoreClient lobStoreClient = Mockito.mock(LobStoreClient.class);
        DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);
        String defaultMessage = "msg";
        List<CustomPropertyConfig> customProperties = new ArrayList<>();

        var task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(0), outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService, dveMetadataReader, lobStoreClient, "ds1", readyCheck, 100
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
    @Test
    public void createVersionInfoJson_should_include_external_large_objects_when_fetch_sha1s_present() throws Exception {
        // Given
        Path dve = testDir.resolve("test.zip");
        Path currentBatchWorkDir = testDir.resolve("batch");
        Path dataVaultBatchRoot = testDir.resolve("vault");
        Path outboxProcessed = testDir.resolve("processed");
        Path outboxFailed = testDir.resolve("failed");
        DataVaultClient dataVaultClient = Mockito.mock(DataVaultClient.class);
        FileService fileService = new FileServiceImpl();
        DveMetadataReader dveMetadataReader = Mockito.mock(DveMetadataReader.class);
        LobStoreClient lobStoreClient = Mockito.mock(LobStoreClient.class);
        DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);
        String defaultMessage = "msg";
        List<CustomPropertyConfig> customProperties = new ArrayList<>();

        var task = new SendToVaultTask(
            dve, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(0), outboxProcessed, outboxFailed,
            dataVaultClient, defaultMessage, customProperties, fileService, dveMetadataReader, lobStoreClient, "ds1", readyCheck, 100
        );

        Path versionDirectory = testDir.resolve("v1");
        Files.createDirectories(versionDirectory);

        // Set up a TransferItem with fetch sha1s
        var transferItem = Mockito.mock(TransferItem.class);
        Mockito.when(transferItem.getFetchSha1s()).thenReturn(List.of("sha1-1", "sha1-2"));
        // Set currentTransferItem via reflection
        var field = SendToVaultTask.class.getDeclaredField("currentTransferItem");
        field.setAccessible(true);
        field.set(task, transferItem);

        // When
        task.createVersionInfoJson(versionDirectory, "user", "email", "message");

        // Then
        Path jsonFile = testDir.resolve("v1.json");
        assertThat(jsonFile).exists();

        var mapper = new ObjectMapper();
        Map<?,?> root;
        try (var is = Files.newInputStream(jsonFile)) {
            root = mapper.readValue(is, Map.class);
        }
        Map<?,?> objectVersionProperties = (Map<?,?>) root.get("object-version-properties");
        assertThat(objectVersionProperties).isNotNull();
        Map<?,?> externalLargeObjects = (Map<?,?>) objectVersionProperties.get("external-large-objects");
        assertThat(externalLargeObjects).isNotNull();
        assertThat(externalLargeObjects.get("checksum-algorithm")).isEqualTo("sha1");
        assertThat((List<String>) externalLargeObjects.get("lobs")).containsExactlyInAnyOrder("sha1-1", "sha1-2");
    }

    @Test
    public void processItem_should_request_transfers_from_lob_store_when_lobs_present() throws Exception {
        // Given
        Path item = testDir.resolve("test.zip");
        Files.createFile(item);
        Path currentBatchWorkDir = testDir.resolve("batch");
        Files.createDirectories(currentBatchWorkDir);
        Path dataVaultBatchRoot = testDir.resolve("vault");
        Path outboxProcessed = testDir.resolve("processed");
        Files.createDirectories(outboxProcessed);
        Path outboxFailed = testDir.resolve("failed");
        DataVaultClient dataVaultClient = Mockito.mock(DataVaultClient.class);
        FileService fileService = Mockito.mock(FileService.class);
        DveMetadataReader dveMetadataReader = Mockito.mock(DveMetadataReader.class);
        LobStoreClient lobStoreClient = Mockito.mock(LobStoreClient.class);
        DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);

        var task = Mockito.spy(new SendToVaultTask(
            testDir, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(1000), outboxProcessed, outboxFailed,
            dataVaultClient, "msg", List.of(), fileService, dveMetadataReader, lobStoreClient, "ds1", readyCheck, 100
        ));

        DveMetadata metadata = Mockito.mock(DveMetadata.class);
        DataFileMetadata attr = Mockito.mock(DataFileMetadata.class);
        Mockito.when(attr.getSha1Checksum()).thenReturn("sha1");
        Mockito.when(attr.getUri()).thenReturn(new URI("http://dv.com/file/?fileId=123"));
        Mockito.when(metadata.getDataFileAttributes()).thenReturn(List.of(attr));

        Mockito.when(dveMetadataReader.readDveMetadata(item)).thenReturn(metadata);

        // Mock TransferItem
        TransferItem transferItem = Mockito.mock(TransferItem.class);
        Mockito.when(transferItem.getNbn()).thenReturn("nbn1");
        Mockito.when(transferItem.getOcflObjectVersion()).thenReturn(1);
        Mockito.when(transferItem.getFetchSha1s()).thenReturn(List.of("sha1"));

        // Use the real getLobRequests but with mocked metadata
        Mockito.when(transferItem.getLobRequests(Mockito.any(), Mockito.anyString())).thenCallRealMethod();

        // Inject TransferItem into task
        Mockito.doReturn(transferItem).when(task).createTransferItem(item);
        Mockito.doNothing().when(task).addToObjectImportDirectory(Mockito.any(), Mockito.anyInt(), Mockito.any());
        Mockito.doNothing().when(task).importIfBatchThresholdReached();

        // When
        task.processItem(item);

        // Then
        ArgumentCaptor<List<TransferRequestDto>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(lobStoreClient).requestTransfers(captor.capture());
        List<TransferRequestDto> requests = captor.getValue();
        assertThat(requests).hasSize(1);
        assertThat(requests.get(0).getDataverseFileId()).isEqualTo(123L);
        assertThat(requests.get(0).getSha1Sum()).isEqualTo("sha1");
        assertThat(requests.get(0).getDatastation()).isEqualTo("ds1");
    }

    @Test
    public void processItem_should_fail_when_fileId_is_missing_in_uri() throws Exception {
        // Given
        Path item = testDir.resolve("test.zip");
        Files.createFile(item);
        Path currentBatchWorkDir = testDir.resolve("batch");
        Files.createDirectories(currentBatchWorkDir);
        Path dataVaultBatchRoot = testDir.resolve("vault");
        Path outboxProcessed = testDir.resolve("processed");
        Path outboxFailed = testDir.resolve("failed");
        DataVaultClient dataVaultClient = Mockito.mock(DataVaultClient.class);
        FileService fileService = Mockito.mock(FileService.class);
        DveMetadataReader dveMetadataReader = Mockito.mock(DveMetadataReader.class);
        LobStoreClient lobStoreClient = Mockito.mock(LobStoreClient.class);
        DependenciesReadyCheck readyCheck = Mockito.mock(DependenciesReadyCheck.class);

        var task = Mockito.spy(new SendToVaultTask(
            testDir, currentBatchWorkDir, dataVaultBatchRoot, DataSize.bytes(1000), outboxProcessed, outboxFailed,
            dataVaultClient, "msg", List.of(), fileService, dveMetadataReader, lobStoreClient, "ds1", readyCheck, 100
        ));

        DveMetadata metadata = Mockito.mock(DveMetadata.class);
        DataFileMetadata attr = Mockito.mock(DataFileMetadata.class);
        Mockito.when(attr.getSha1Checksum()).thenReturn("sha1");
        Mockito.when(attr.getUri()).thenReturn(new URI("http://dv.com/file/?otherId=123"));
        Mockito.when(metadata.getDataFileAttributes()).thenReturn(List.of(attr));

        Mockito.when(dveMetadataReader.readDveMetadata(item)).thenReturn(metadata);

        TransferItem transferItem = Mockito.mock(TransferItem.class);
        Mockito.when(transferItem.getNbn()).thenReturn("nbn1");
        Mockito.when(transferItem.getOcflObjectVersion()).thenReturn(1);
        Mockito.when(transferItem.getFetchSha1s()).thenReturn(List.of("sha1"));
        Mockito.when(transferItem.getLobRequests(Mockito.any(), Mockito.anyString())).thenCallRealMethod();

        Mockito.doReturn(transferItem).when(task).createTransferItem(item);
        Mockito.doNothing().when(task).addToObjectImportDirectory(Mockito.any(), Mockito.anyInt(), Mockito.any());

        // When
        try {
            task.processItem(item);
        } catch (IllegalArgumentException e) {
            task.rejectCurrentItem(e);
        }

        // Then
        Mockito.verify(transferItem).moveToErrorBox(Mockito.eq(outboxFailed), Mockito.any(IllegalArgumentException.class));
    }
}
