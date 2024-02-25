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
package nl.knaw.dans.ttv.health;

import nl.knaw.dans.ttv.config.DdTransferToVaultConfig;
import nl.knaw.dans.ttv.config.ExtractMetadataConfig;
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
        var config = new DdTransferToVaultConfig();
        config.setExtractMetadata(new ExtractMetadataConfig());
        config.getExtractMetadata().setInbox(Path.of("metadata"));

        var fs1 = Mockito.mock(FileStore.class);
        var fs2 = Mockito.mock(FileStore.class);

        Mockito.when(fileService.getFileStore(Mockito.any()))
            .thenReturn(fs1);

        var result = new PartitionHealthCheck(config, fileService).check();

        assertTrue(result.isHealthy());
    }
}
