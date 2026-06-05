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

import nl.knaw.dans.lib.util.healthcheck.DependenciesReadyCheck;
import nl.knaw.dans.transfer.TestDirFixture;
import nl.knaw.dans.transfer.client.ValidateBagPackClient;
import nl.knaw.dans.transfer.client.VaultCatalogClient;
import nl.knaw.dans.transfer.config.ValidateBagPackConfig;
import nl.knaw.dans.validatebagpack.client.api.ValidationResultDto;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExtractMetadataTaskTest extends TestDirFixture {

    @Test
    public void processItem_should_throw_IllegalArgumentException_when_holey_bag_and_datastationName_is_null() throws Exception {
        // Given
        var srcDir = testDir.resolve("src");
        var outboxProcessed = testDir.resolve("processed");
        var outboxFailed = testDir.resolve("failed");
        var outboxRejected = testDir.resolve("rejected");
        var nbnRegistrationInbox = testDir.resolve("nbn-inbox");
        var vaultCatalogBaseUri = URI.create("http://localhost/catalog");
        var dveMetadataReader = Mockito.mock(DveMetadataReader.class);
        var fileService = Mockito.mock(FileService.class);
        var vaultCatalogClient = Mockito.mock(VaultCatalogClient.class);
        var validateBagPackClient = Mockito.mock(ValidateBagPackClient.class);
        var readyCheck = Mockito.mock(DependenciesReadyCheck.class);

        // Mock TransferItem to return non-empty fetch sha1s
        var transferItem = Mockito.mock(TransferItem.class);
        Mockito.when(transferItem.getFetchSha1s()).thenReturn(List.of("sha1"));

        var task = new ExtractMetadataTask(
            srcDir, null, outboxProcessed, outboxFailed, outboxRejected,
            nbnRegistrationInbox, vaultCatalogBaseUri, dveMetadataReader, fileService,
            vaultCatalogClient, validateBagPackClient, readyCheck, 100
        ) {
            @Override
            protected TransferItem createTransferItem(Path item) {
                return transferItem;
            }
        };

        var item = srcDir.resolve("dataset.zip");
        
        // Mock validateBagPackClient to return success
        var result = Mockito.mock(ValidationResultDto.class);
        Mockito.when(result.getIsCompliant()).thenReturn(true);
        Mockito.when(validateBagPackClient.validateBagPack(item)).thenReturn(result);

        // When / Then
        assertThatThrownBy(() -> task.processItem(item))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Holey bags (with fetch.txt) are not supported for VaaS customers yet.");
    }
}
