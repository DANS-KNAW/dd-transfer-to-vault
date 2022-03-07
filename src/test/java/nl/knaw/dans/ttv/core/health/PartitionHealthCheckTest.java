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
package nl.knaw.dans.ttv.core.health;

import nl.knaw.dans.ttv.DdTransferToVaultConfiguration;
import nl.knaw.dans.ttv.core.config.CreateOcflTarConfiguration;
import nl.knaw.dans.ttv.core.config.ExtractMetadataConfiguration;
import nl.knaw.dans.ttv.core.service.FileService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.FileStore;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionHealthCheckTest {

    @Test
    void onSamePartition() throws Exception {
        var fileService = Mockito.mock(FileService.class);
        var config = new DdTransferToVaultConfiguration();
        config.setExtractMetadata(new ExtractMetadataConfiguration());
        config.getExtractMetadata().setInbox(Path.of("metadata"));
        config.setCreateOcflTar(new CreateOcflTarConfiguration());
        config.getCreateOcflTar().setInbox(Path.of("ocfl-inbox"));
        config.getCreateOcflTar().setWorkDir(Path.of("ocfl-workdir"));

        var fs1 = Mockito.mock(FileStore.class);
        var fs2 = Mockito.mock(FileStore.class);

        Mockito.when(fileService.getFileStore(Mockito.any()))
            .thenReturn(fs1);

        var result = new PartitionHealthCheck(config, fileService).check();

        assertTrue(result.isHealthy());
    }

    @Test
    void onDifferentPartition() throws Exception {
        var fileService = Mockito.mock(FileService.class);
        var config = new DdTransferToVaultConfiguration();
        config.setExtractMetadata(new ExtractMetadataConfiguration());
        config.getExtractMetadata().setInbox(Path.of("metadata"));
        config.setCreateOcflTar(new CreateOcflTarConfiguration());
        config.getCreateOcflTar().setInbox(Path.of("ocfl-inbox"));
        config.getCreateOcflTar().setWorkDir(Path.of("ocfl-workdir"));

        var fs1 = Mockito.mock(FileStore.class);
        var fs2 = Mockito.mock(FileStore.class);

        Mockito.when(fileService.getFileStore(Mockito.any()))
            .thenReturn(fs1)
            .thenReturn(fs2);

        var result = new PartitionHealthCheck(config, fileService).check();

        assertFalse(result.isHealthy());
    }
}
