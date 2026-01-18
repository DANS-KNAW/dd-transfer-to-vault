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
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FileSystemPermissionHealthCheckTest {

    private TransferConfig transferConfig;
    private NbnRegistrationConfig nbnRegistrationConfig;
    private FileService fileService;
    private FileSystemPermissionHealthCheck healthCheck;

    @BeforeEach
    void setUp() {
        transferConfig = mock(TransferConfig.class);
        nbnRegistrationConfig = mock(NbnRegistrationConfig.class);
        fileService = mock(FileService.class);
        healthCheck = new FileSystemPermissionHealthCheck(transferConfig, nbnRegistrationConfig, fileService);

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

        // Mock NbnRegistrationConfig
        InboxConfig nbnInbox = mock(InboxConfig.class);
        when(nbnInbox.getPath()).thenReturn(Path.of("/nbn/inbox"));
        OutboxConfig nbnOutbox = mock(OutboxConfig.class);
        when(nbnOutbox.getProcessed()).thenReturn(Path.of("/nbn/outbox/processed"));
        when(nbnOutbox.getFailed()).thenReturn(Path.of("/nbn/outbox/failed"));
        when(nbnRegistrationConfig.getInbox()).thenReturn(nbnInbox);
        when(nbnRegistrationConfig.getOutbox()).thenReturn(nbnOutbox);

        // Mock CollectDveConfig
        nl.knaw.dans.transfer.config.CollectDveConfig collectDveConfig = mock(nl.knaw.dans.transfer.config.CollectDveConfig.class);
        InboxConfig cdInbox = mock(InboxConfig.class);
        when(cdInbox.getPath()).thenReturn(Path.of("/cd/inbox"));
        when(collectDveConfig.getInbox()).thenReturn(cdInbox);
        when(collectDveConfig.getProcessed()).thenReturn(Path.of("/cd/processed"));
        when(transferConfig.getCollectDve()).thenReturn(collectDveConfig);

        // Default: all writable/readable and same filesystem
        when(fileService.canWriteTo(any(Path.class))).thenReturn(true);
        when(fileService.canReadFrom(any(Path.class))).thenReturn(true);
        when(fileService.isSameFileSystem(any(Collection.class))).thenReturn(true);
    }

    @Test
    void check_should_return_healthy_when_all_conditions_are_met() {
        HealthCheck.Result result = healthCheck.check();
        assertTrue(result.isHealthy());
    }

    @Test
    void check_should_return_unhealthy_when_a_path_is_not_writable() {
        Path nonWritablePath = Path.of("/em/inbox");
        when(fileService.canWriteTo(nonWritablePath)).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertFalse(result.isHealthy());
        assertTrue(result.getMessage().contains(nonWritablePath.toString()));
        assertTrue(result.getDetails().containsKey(nonWritablePath.toString()));
        assertTrue(result.getDetails().get(nonWritablePath.toString()).toString().contains("Path is not writable"));
    }

    @Test
    void check_should_return_unhealthy_when_a_path_is_not_readable() {
        Path nonReadablePath = Path.of("/em/inbox");
        when(fileService.canReadFrom(nonReadablePath)).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertFalse(result.isHealthy());
        assertTrue(result.getMessage().contains(nonReadablePath.toString()));
        assertTrue(result.getDetails().containsKey(nonReadablePath.toString()));
        assertTrue(result.getDetails().get(nonReadablePath.toString()).toString().contains("Path is not readable"));
    }

    @Test
    void check_should_return_unhealthy_when_paths_are_not_on_same_filesystem() {
        when(fileService.isSameFileSystem(any())).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertFalse(result.isHealthy());

        Set<Path> allPaths = Set.of(
            Path.of("/em/inbox"),
            Path.of("/em/outbox/processed"),
            Path.of("/em/outbox/failed"),
            Path.of("/em/outbox/rejected"),
            Path.of("/sv/working"),
            Path.of("/sv/batch"),
            Path.of("/sv/inbox"),
            Path.of("/sv/outbox/processed"),
            Path.of("/sv/outbox/failed"),
            Path.of("/nbn/inbox"),
            Path.of("/nbn/outbox/processed"),
            Path.of("/nbn/outbox/failed")
        );

        String expectedDetailKey = allPaths.stream().map(Path::toString).sorted().collect(java.util.stream.Collectors.joining(", "));
        assertTrue(result.getDetails().containsKey(expectedDetailKey));
        assertTrue(result.getDetails().get(expectedDetailKey).toString().contains("Paths are not all on the same file system"));
    }

    @Test
    void check_should_return_unhealthy_with_multiple_failures() {
        Path nonWritablePath = Path.of("/sv/inbox");
        Path nonReadablePath = Path.of("/em/inbox");
        when(fileService.canWriteTo(nonWritablePath)).thenReturn(false);
        when(fileService.canReadFrom(nonReadablePath)).thenReturn(false);
        when(fileService.isSameFileSystem(any())).thenReturn(false);

        HealthCheck.Result result = healthCheck.check();
        assertFalse(result.isHealthy());

        // Check non-writable path
        assertTrue(result.getDetails().containsKey(nonWritablePath.toString()));
        assertTrue(result.getDetails().get(nonWritablePath.toString()).toString().contains("Path is not writable"));

        // Check non-readable path
        assertTrue(result.getDetails().containsKey(nonReadablePath.toString()));
        assertTrue(result.getDetails().get(nonReadablePath.toString()).toString().contains("Path is not readable"));

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
            Path.of("/sv/outbox/failed"),
            Path.of("/nbn/inbox"),
            Path.of("/nbn/outbox/processed"),
            Path.of("/nbn/outbox/failed")
        );
        String expectedDetailKey = allPaths.stream().map(Path::toString).sorted().collect(java.util.stream.Collectors.joining(", "));
        assertTrue(result.getDetails().containsKey(expectedDetailKey));

        // Check message contains all
        assertTrue(result.getMessage().contains(nonWritablePath.toString()));
        assertTrue(result.getMessage().contains(nonReadablePath.toString()));
        assertTrue(result.getMessage().contains(expectedDetailKey));
    }
}
