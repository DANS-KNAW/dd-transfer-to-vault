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
package nl.knaw.dans.transfer.health;

import com.codahale.metrics.health.HealthCheck;
import nl.knaw.dans.transfer.config.DataVaultBatchConfig;
import nl.knaw.dans.transfer.config.ExtractMetadataConfig;
import nl.knaw.dans.transfer.config.InboxConfig;
import nl.knaw.dans.transfer.config.NbnRegistrationConfig;
import nl.knaw.dans.transfer.config.OutboxConfig;
import nl.knaw.dans.transfer.config.OutboxWithRejectedConfig;
import nl.knaw.dans.transfer.config.SendToVaultConfig;
import nl.knaw.dans.transfer.config.TransferConfig;
import nl.knaw.dans.transfer.core.FileService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FileSystemPermissionsHealthCheckTest {

    private FileService fileService;
    private FileSystemPermissionsHealthCheck healthCheck;

    @BeforeEach
    void setUp() {
        TransferConfig transferConfig = mock(TransferConfig.class);
        fileService = mock(FileService.class);
        healthCheck = new FileSystemPermissionsHealthCheck(transferConfig, fileService);

        // Mock NbnRegistrationConfig
        NbnRegistrationConfig nbnRegistrationConfig = mock(NbnRegistrationConfig.class);
        when(nbnRegistrationConfig.getOutbox()).thenReturn(Path.of("/nbn/inbox"));
        when(transferConfig.getNbnRegistration()).thenReturn(nbnRegistrationConfig);


        // Mock ExtractMetadataConfig
        ExtractMetadataConfig extractMetadataConfig = mock(ExtractMetadataConfig.class);
        InboxConfig emInbox = mock(InboxConfig.class);
        when(emInbox.getPath()).thenReturn(Path.of("/em/inbox"));
        OutboxWithRejectedConfig emOutbox = mock(OutboxWithRejectedConfig.class);
        when(emOutbox.getProcessed()).thenReturn(Path.of("/em/outbox/processed"));
        when(emOutbox.getFailed()).thenReturn(Path.of("/em/outbox/failed"));
        when(emOutbox.getRejected()).thenReturn(Path.of("/em/outbox/rejected"));
        when(extractMetadataConfig.getInbox()).thenReturn(emInbox);
        when(extractMetadataConfig.getOutbox()).thenReturn(emOutbox);
        when(transferConfig.getExtractMetadata()).thenReturn(extractMetadataConfig);

        // Mock SendToVaultConfig
        SendToVaultConfig sendToVaultConfig = mock(SendToVaultConfig.class);
        DataVaultBatchConfig dataVaultConfig = mock(DataVaultBatchConfig.class);
        when(dataVaultConfig.getCurrentBatchWorkingDir()).thenReturn(Path.of("/sv/working"));
        when(dataVaultConfig.getBatchRoot()).thenReturn(Path.of("/sv/batch"));
        InboxConfig svInbox = mock(InboxConfig.class);
        when(svInbox.getPath()).thenReturn(Path.of("/sv/inbox"));
        OutboxConfig svOutbox = mock(OutboxConfig.class);
        when(svOutbox.getProcessed()).thenReturn(Path.of("/sv/outbox/processed"));
        when(svOutbox.getFailed()).thenReturn(Path.of("/sv/outbox/failed"));
        when(sendToVaultConfig.getDataVault()).thenReturn(dataVaultConfig);
        when(sendToVaultConfig.getInbox()).thenReturn(svInbox);
        when(sendToVaultConfig.getOutbox()).thenReturn(svOutbox);
        when(transferConfig.getSendToVault()).thenReturn(sendToVaultConfig);

        // Mock CollectDveConfig
        nl.knaw.dans.transfer.config.CollectDveConfig collectDveConfig = mock(nl.knaw.dans.transfer.config.CollectDveConfig.class);
        InboxConfig cdInbox = mock(InboxConfig.class);
        when(cdInbox.getPath()).thenReturn(Path.of("/cd/inbox"));
        when(collectDveConfig.getInbox()).thenReturn(cdInbox);
        when(collectDveConfig.getProcessed()).thenReturn(Path.of("/cd/processed"));
        when(transferConfig.getCollectDve()).thenReturn(collectDveConfig);

        // Default: all writable/readable and same filesystem
        when(fileService.exists(any(Path.class))).thenReturn(true);
        when(fileService.exists(any(Path.class), anyInt(), anyLong())).thenReturn(true);
        when(fileService.canWriteTo(any(Path.class), anyBoolean())).thenReturn(true);
        when(fileService.canReadFrom(any(Path.class))).thenReturn(true);
        when(fileService.isSameFileSystem(any(Collection.class))).thenReturn(true);
    }

    @Test
    void check_should_return_healthy_when_all_conditions_are_met() {
        HealthCheck.Result result = healthCheck.check();
        assertThat(result.isHealthy()).isTrue();
    }

    @Test
    void check_should_return_unhealthy_when_a_path_is_not_writable() {
        Path nonWritablePath = Path.of("/em/inbox");
        when(fileService.canWriteTo(nonWritablePath)).thenReturn(false);
        when(fileService.canWriteTo(nonWritablePath, false)).thenReturn(false);
        when(fileService.canWriteTo(nonWritablePath, true)).thenReturn(false);
        HealthCheck.Result result = healthCheck.check();
        assertThat(result.isHealthy()).isFalse();
        assertThat(result.getMessage()).contains(nonWritablePath.toString());
        assertThat(result.getDetails()).containsKey(nonWritablePath.toString());
        assertThat(result.getDetails().get(nonWritablePath.toString()).toString()).contains("Path is not writable");
    }

    @Test
    void check_should_return_unhealthy_when_a_path_is_not_readable() {
        Path nonReadablePath = Path.of("/em/inbox");
        when(fileService.canReadFrom(nonReadablePath)).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertThat(result.isHealthy()).isFalse();
        assertThat(result.getMessage()).contains(nonReadablePath.toString());
        assertThat(result.getDetails()).containsKey(nonReadablePath.toString());
        assertThat(result.getDetails().get(nonReadablePath.toString()).toString()).contains("Path is not readable");
    }

    @Test
    void check_should_return_unhealthy_when_a_path_does_not_exist() {
        Path nonExistentPath = Path.of("/em/inbox");
        when(fileService.exists(nonExistentPath)).thenReturn(false);
        when(fileService.exists(nonExistentPath, 5, 1000)).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertThat(result.isHealthy()).isFalse();
        assertThat(result.getMessage()).contains(nonExistentPath.toString());
        assertThat(result.getDetails()).containsKey(nonExistentPath.toString());
        assertThat(result.getDetails().get(nonExistentPath.toString()).toString()).contains("Path does not exist");
    }

    @Test
    void check_should_return_unhealthy_when_paths_are_not_on_same_filesystem() {
        when(fileService.isSameFileSystem(any())).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertThat(result.isHealthy()).isFalse();

        Set<Path> allPaths = Set.of(
            Path.of("/em/inbox"),
            Path.of("/em/outbox/processed"),
            Path.of("/em/outbox/failed"),
            Path.of("/em/outbox/rejected"),
            Path.of("/sv/working"),
            Path.of("/sv/batch"),
            Path.of("/sv/inbox"),
            Path.of("/sv/outbox/processed"),
            Path.of("/sv/outbox/failed")
        );

        String expectedDetailKey = allPaths.stream().map(Path::toString).sorted().collect(java.util.stream.Collectors.joining(", "));
        assertThat(result.getDetails()).containsKey(expectedDetailKey);
        assertThat(result.getDetails().get(expectedDetailKey).toString()).contains("Paths are not all on the same file system");
    }

    @Test
    void check_should_return_unhealthy_with_multiple_failures() {
        Path nonWritablePath = Path.of("/sv/inbox");
        Path nonReadablePath = Path.of("/em/inbox");
        when(fileService.canWriteTo(nonWritablePath)).thenReturn(false);
        when(fileService.canWriteTo(nonWritablePath, false)).thenReturn(false);
        when(fileService.canWriteTo(nonWritablePath, true)).thenReturn(false);
        when(fileService.canReadFrom(nonReadablePath)).thenReturn(false);
        when(fileService.isSameFileSystem(any())).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertThat(result.isHealthy()).isFalse();

        // Check non-writable path
        assertThat(result.getDetails()).containsKey(nonWritablePath.toString());
        assertThat(result.getDetails().get(nonWritablePath.toString()).toString()).contains("Path is not writable");

        // Check non-readable path
        assertThat(result.getDetails()).containsKey(nonReadablePath.toString());
        assertThat(result.getDetails().get(nonReadablePath.toString()).toString()).contains("Path is not readable");

        // Check same-filesystem failure
        Set<Path> allPaths = Set.of(
            Path.of("/em/inbox"),
            Path.of("/em/outbox/processed"),
            Path.of("/em/outbox/failed"),
            Path.of("/em/outbox/rejected"),
            Path.of("/sv/working"),
            Path.of("/sv/batch"),
            Path.of("/sv/inbox"),
            Path.of("/sv/outbox/processed"),
            Path.of("/sv/outbox/failed")
        );
        String expectedDetailKey = allPaths.stream().map(Path::toString).sorted().collect(java.util.stream.Collectors.joining(", "));
        assertThat(result.getDetails()).containsKey(expectedDetailKey);

        // Check message contains all
        assertThat(result.getMessage()).contains(nonWritablePath.toString());
        assertThat(result.getMessage()).contains(nonReadablePath.toString());
        assertThat(result.getMessage()).contains(expectedDetailKey);
    }
}
